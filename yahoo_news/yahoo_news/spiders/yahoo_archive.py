import json
import csv
from pathlib import Path
from time import perf_counter
from urllib.parse import urlparse
import scrapy
from scrapy import signals
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from scrapy_playwright.page import PageMethod

TZ = ZoneInfo("Asia/Taipei")


class YahooArchivePWSpider(scrapy.Spider):
    name = "yahoo_archive"
    allowed_domains = ["tw.news.yahoo.com"]
    start_urls = ["https://tw.news.yahoo.com/archive"]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.close_reason = "unknown"
        spider.output_uri = None

        existing_feeds = crawler.settings.getdict("FEEDS") or {}
        existing_feed_uri = crawler.settings.get("FEED_URI")
        if not existing_feeds and not existing_feed_uri:
            auto_uri = spider._build_output_filename()
            crawler.settings.set(
                "FEEDS",
                {
                    auto_uri: {
                        "format": "csv",
                        "encoding": "utf-8-sig",
                        "overwrite": True,
                    }
                },
                priority="spider",
            )
            spider.output_uri = auto_uri

        crawler.signals.connect(spider._on_spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(spider._on_engine_stopped, signal=signals.engine_stopped)
        return spider

    def __init__(
        self,
        end=None,
        max_scroll=None,
        max_urls=None,
        old_streak_stop=20,
        scroll_wait_ms=350,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.max_scroll = int(max_scroll) if max_scroll is not None else None
        self.max_urls = int(max_urls) if max_urls is not None else None
        self.old_streak_stop = int(old_streak_stop)
        self.scroll_wait_ms = int(scroll_wait_ms)

        if end:
            self.end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M").replace(tzinfo=TZ)
        else:
            self.end_dt = datetime.now(TZ)
        self.start_dt = self.end_dt - timedelta(hours=1)

        self.seen_urls = set()
        self.old_count_streak = 0
        self.seen_recent_article = False
        self.stop_requested = False
        self.run_started_at = None
        self.run_started_perf = None
        self.item_count = 0

    def start_requests(self):
        self.run_started_at = datetime.now(TZ)
        self.run_started_perf = perf_counter()
        yield self._make_archive_request(retry_count=0)

    def _make_archive_request(self, retry_count=0):
        return scrapy.Request(
            self.start_urls[0],
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_goto_kwargs": {
                    "wait_until": "domcontentloaded",
                    "timeout": 45_000,
                },
                "playwright_page_methods": [
                    PageMethod("wait_for_timeout", 350),
                ],
                "archive_retry_count": retry_count,
            },
            callback=self.parse_archive,
            errback=self.on_archive_error,
            dont_filter=True,
        )

    def on_archive_error(self, failure):
        retry_count = failure.request.meta.get("archive_retry_count", 0)
        self.logger.warning("archive request failed (retry=%s): %s", retry_count, failure.value)
        if retry_count < 2:
            yield self._make_archive_request(retry_count=retry_count + 1)

    async def parse_archive(self, response):
        page = response.meta["playwright_page"]

        async def block_heavy(route):
            rt = route.request.resource_type
            if rt in ("image", "media", "font"):
                await route.abort()
            else:
                await route.continue_()

        try:
            await page.route("**/*", block_heavy)

            scroll_round = 0
            while True:
                if self.stop_requested:
                    self.logger.info(
                        "stop archive scrolling: old_count_streak=%s threshold=%s",
                        self.old_count_streak,
                        self.old_streak_stop,
                    )
                    break

                if self.max_scroll is not None and scroll_round >= self.max_scroll:
                    self.logger.info("stop archive scrolling: hit max_scroll=%s", self.max_scroll)
                    break

                scroll_round += 1
                html = await page.content()
                sel = scrapy.Selector(text=html)

                anchors = sel.xpath('//a[contains(@href, ".html")]')
                added = 0

                for anchor in anchors:
                    h = anchor.xpath("./@href").get()
                    if not h:
                        continue
                    url = response.urljoin(h)
                    if not url.startswith("https://tw.news.yahoo.com/"):
                        continue
                    if not url.endswith(".html"):
                        continue
                    if url in self.seen_urls:
                        continue

                    # Pre-filter by timestamp found on archive cards to avoid requesting old articles.
                    archive_dt_str = (
                        anchor.xpath('.//time[@datetime][1]/@datetime').get()
                        or anchor.xpath("ancestor::*[time[@datetime]][1]/time[@datetime][1]/@datetime").get()
                        or anchor.xpath("ancestor::*[.//time[@datetime]][1]//time[@datetime][1]/@datetime").get()
                    )
                    archive_pub_dt = self._parse_datetime(archive_dt_str)
                    if archive_pub_dt:
                        if archive_pub_dt > self.end_dt:
                            continue
                        if archive_pub_dt < self.start_dt:
                            if self.seen_recent_article:
                                self.old_count_streak += 1
                                if self.old_count_streak >= self.old_streak_stop:
                                    self.stop_requested = True
                                    break
                            continue
                        self.seen_recent_article = True
                        self.old_count_streak = 0

                    self.seen_urls.add(url)
                    added += 1
                    yield scrapy.Request(
                        url,
                        callback=self.parse_article,
                        cb_kwargs={"archive_dt_str": archive_dt_str},
                        meta={"playwright": False},
                    )

                    if self.max_urls is not None and len(self.seen_urls) >= self.max_urls:
                        break

                self.logger.info("scroll=%s added_urls=%s total_urls=%s", scroll_round, added, len(self.seen_urls))

                if self.max_urls is not None and len(self.seen_urls) >= self.max_urls:
                    self.logger.info("stop archive scrolling: hit max_urls=%s", self.max_urls)
                    break

                await page.evaluate("window.scrollBy(0, document.body.scrollHeight * 0.85)")
                await page.wait_for_timeout(self.scroll_wait_ms)
        finally:
            await page.close()

    def parse_article(self, response, archive_dt_str=None):
        title = response.css("h1::text").get()
        title = title.strip() if title else None

        jsonld_author, jsonld_provider = self._extract_from_jsonld(response)

        author = (
            jsonld_author
            or response.css('meta[name="author"]::attr(content)').get()
            or response.css('[rel="author"]::text').get()
        )
        author = author.strip() if author else None

        provider = (
            jsonld_provider
            or response.css('meta[property="og:site_name"]::attr(content)').get()
            or response.css('meta[name="application-name"]::attr(content)').get()
        )
        provider = provider.strip() if provider else None

        dt_str = (
            response.css("time::attr(datetime)").get()
            or response.css('meta[property="article:published_time"]::attr(content)').get()
            or response.css('meta[name="pubdate"]::attr(content)').get()
        )
        pub_dt = self._parse_datetime(dt_str) or self._parse_datetime(archive_dt_str)

        if pub_dt is None:
            return

        if pub_dt > self.end_dt:
            return

        # Stop when we keep seeing articles older than the 1-hour window.
        if pub_dt < self.start_dt:
            if self.seen_recent_article:
                self.old_count_streak += 1
                if self.old_count_streak >= self.old_streak_stop:
                    self.stop_requested = True
            return

        self.seen_recent_article = True
        self.old_count_streak = 0

        if not (self.start_dt <= pub_dt <= self.end_dt):
            return

        self.item_count += 1
        yield {
            "link": response.url,
            "title": title,
            "author": author,
            "provider": provider,
            "date": pub_dt.isoformat(),
        }

    def _on_spider_closed(self, spider, reason):
        if spider is self:
            self.close_reason = reason

    def _on_engine_stopped(self):
        self._sort_output_csv_by_date()

        ended_at = datetime.now(TZ)
        started_at = self.run_started_at or ended_at
        elapsed_seconds = 0.0
        if self.run_started_perf is not None:
            elapsed_seconds = perf_counter() - self.run_started_perf

        self.logger.info(
            "crawl finished | reason=%s | start=%s | end=%s | elapsed=%.2fs | items=%s",
            self.close_reason,
            started_at.strftime("%Y-%m-%d %H:%M"),
            ended_at.strftime("%Y-%m-%d %H:%M"),
            elapsed_seconds,
            self.item_count,
        )

    def _sort_output_csv_by_date(self):
        output_uri = self._pick_output_csv_uri()
        if not output_uri:
            return

        output_path = self._resolve_output_csv_path(output_uri)
        if output_path is None or not output_path.exists():
            return

        try:
            with output_path.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                fieldnames = reader.fieldnames

            if not rows or not fieldnames or "date" not in fieldnames:
                return

            rows_with_idx = list(enumerate(rows))
            rows_with_idx.sort(
                key=lambda pair: (
                    self._parse_datetime(pair[1].get("date") or "") or datetime.min.replace(tzinfo=TZ),
                    pair[0],
                )
            )
            rows = [row for _, row in rows_with_idx]

            with output_path.open("w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            self.logger.info("sorted output csv by date ascending: %s", output_path)
        except Exception as e:
            self.logger.warning("failed to sort output csv: %s", e)

    def _pick_output_csv_uri(self):
        if self.output_uri:
            return self.output_uri
        feeds = self.crawler.settings.getdict("FEEDS") or {}
        for uri in feeds.keys():
            if str(uri).lower().endswith(".csv"):
                return uri
        return self.crawler.settings.get("FEED_URI")

    def _build_output_filename(self):
        if self.start_dt.date() == self.end_dt.date():
            date_part = self.end_dt.strftime("%Y%m%d")
            time_part = f"{self.start_dt.strftime('%H%M')}~{self.end_dt.strftime('%H%M')}"
            return f"result_{date_part}_{time_part}.csv"
        return (
            f"result_{self.start_dt.strftime('%Y%m%d-%H%M')}"
            f"~{self.end_dt.strftime('%Y%m%d-%H%M')}.csv"
        )

    def _resolve_output_csv_path(self, uri):
        if not uri:
            return None

        if uri.startswith("file://"):
            parsed = urlparse(uri)
            path = parsed.path
            if len(path) >= 3 and path[0] == "/" and path[2] == ":":
                path = path[1:]
            resolved = Path(path)
        else:
            resolved = Path(uri)

        if not resolved.is_absolute():
            resolved = Path.cwd() / resolved
        return resolved

    def _extract_from_jsonld(self, response):
        scripts = response.css('script[type="application/ld+json"]::text').getall()
        if not scripts:
            return None, None

        author_name = None
        provider_name = None

        for raw in scripts:
            raw = raw.strip() if raw else ""
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue

            for node in self._walk_json_nodes(payload):
                if not isinstance(node, dict):
                    continue
                ntype = node.get("@type")
                if isinstance(ntype, list):
                    ntype = " ".join([str(x) for x in ntype])
                ntype = str(ntype) if ntype is not None else ""
                ntype_lower = ntype.lower()

                if "newsarticle" in ntype_lower or "article" in ntype_lower:
                    if author_name is None:
                        author_name = self._extract_person_name(node.get("author"))
                    if provider_name is None:
                        provider_name = self._extract_org_name(node.get("provider"))

                if author_name is None and "person" in ntype_lower:
                    candidate = node.get("name")
                    if isinstance(candidate, str) and candidate.strip():
                        author_name = candidate.strip()

                if provider_name is None and "organization" in ntype_lower:
                    candidate = node.get("name")
                    if isinstance(candidate, str) and candidate.strip():
                        provider_name = candidate.strip()

                if author_name and provider_name:
                    return author_name, provider_name

        return author_name, provider_name

    def _walk_json_nodes(self, data):
        if isinstance(data, dict):
            yield data
            for value in data.values():
                yield from self._walk_json_nodes(value)
        elif isinstance(data, list):
            for item in data:
                yield from self._walk_json_nodes(item)

    def _extract_person_name(self, value):
        if isinstance(value, str):
            return value.strip() or None
        if isinstance(value, dict):
            name = value.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
            return None
        if isinstance(value, list):
            names = []
            for item in value:
                n = self._extract_person_name(item)
                if n:
                    names.append(n)
            if names:
                return ", ".join(names)
        return None

    def _extract_org_name(self, value):
        if isinstance(value, str):
            return value.strip() or None
        if isinstance(value, dict):
            name = value.get("name")
            if isinstance(name, str) and name.strip():
                return name.strip()
            return None
        if isinstance(value, list):
            for item in value:
                n = self._extract_org_name(item)
                if n:
                    return n
        return None

    def _parse_datetime(self, s: str):
        if not s:
            return None
        s = s.strip()
        try:
            if s.endswith("Z"):
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            return dt.astimezone(TZ)
        except Exception:
            return None
