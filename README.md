# Proj_yahoo_news

Yahoo 台灣新聞爬蟲專案（`Scrapy + Playwright`），目標是抓取 `https://tw.news.yahoo.com/archive` 在指定時間窗（預設最近 1 小時）內的新聞資料，並輸出 CSV。

## 功能重點

- 來源頁：Yahoo 新聞 archive（動態捲動）
- 文章欄位：
  - `link`
  - `title`
  - `author`（優先 JSON-LD）
  - `provider`（優先 JSON-LD）
  - `date`（ISO 8601，台北時區）
- 停止條件：
  - 主要以「超出 1 小時時間窗的連續舊文」判斷停止
  - 安全保護：`CLOSESPIDER_TIMEOUT = 600`（10 分鐘自動停止）
- 輸出 CSV 會在結束時自動按 `date` 由小到大排序
- 若未手動指定 `-o/-O`，會自動命名輸出檔為：
  - 同日：`result_YYYYMMDD_HHMM~HHMM.csv`
  - 跨日：`result_YYYYMMDD-HHMM~YYYYMMDD-HHMM.csv`

## 專案結構

```text
Proj_yahoo_news/
  yahoo_news/
    scrapy.cfg
    yahoo_news/
      settings.py
      spiders/
        yahoo_archive.py
```

## 環境需求

- Python 3.11+（建議）
- Windows / macOS / Linux

## 安裝步驟

```powershell
cd d:\桌面\Proj_yahoo_news
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install scrapy scrapy-playwright
playwright install chromium
```

## 執行方式

### 1) 使用自動檔名輸出（推薦）

```powershell
cd d:\桌面\Proj_yahoo_news\yahoo_news
scrapy crawl yahoo_archive
```

### 2) 指定固定檔名輸出（覆蓋）

```powershell
scrapy crawl yahoo_archive -O output.csv
```

### 3) 指定時間窗終點（台北時區）

以下範例代表抓 `2026-02-26 15:30` 往前 1 小時：

```powershell
scrapy crawl yahoo_archive -a end="2026-02-26 15:30"
```

## 可調參數（Spider Arguments）

- `end`：時間窗終點，格式 `%Y-%m-%d %H:%M`
- `old_streak_stop`：連續舊文閾值（預設 `20`）
- `scroll_wait_ms`：每次滾動等待毫秒（預設 `350`）
- `max_scroll`：可選，最大捲動次數（預設不限制）
- `max_urls`：可選，最大 URL 數量（預設不限制）

範例：

```powershell
scrapy crawl yahoo_archive -a old_streak_stop=15 -a scroll_wait_ms=300
```

## 執行完成 Log

爬蟲結束時會輸出摘要：

- 開始時間
- 結束時間
- 總耗時（秒）
- 抓到筆數
- 停止原因（例如 timeout / 正常收斂）

## 流程使用

```powershell
git clone 
cd Proj_yahoo_news
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install scrapy scrapy-playwright
playwright install chromium
cd yahoo_news
scrapy crawl yahoo_archive
```

成功後可在 `yahoo_news/` 看到 `result_*.csv`。

