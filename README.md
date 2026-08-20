# SMT Manufacturing Data Intelligence System (MCP + DeepAgent)

本專案為針對 **SMT（表面貼焊技術）電子製造工廠** 所建構的智慧數據分析系統。結合 **Model Context Protocol (MCP)** 與 **DeepAgents** 智慧代理技術，讓大型語言模型（LLM）能以標準 SQL 自由查詢工廠內部 42 張跨領域資料表（涵蓋製造績效 DPM20、製程追蹤 SFCS 與設備感測 AIoT），並進行深度異常診斷與數據分析。

---

## 🏗️ 系統架構總覽

```
┌────────────────────────────────────────────────────────┐
│               使用者提問 (User Prompt)                  │
└──────────────────────────┬─────────────────────────────┘
                           ▼
┌────────────────────────────────────────────────────────┐
│     DeepAgent (run_deepagent.py / Gemini 3.6 Flash)    │
│  - 內建 SMT 製造專業 6 步 SOP 引導                     │
│  - 自動持久化 MCP Session (毫秒級工具調用)              │
│  - 完整執行生命週期記錄 (儲存至 agent_results/*.json)  │
└──────────────────────────┬─────────────────────────────┘
                           ▼ (STDIO JSON-RPC)
┌────────────────────────────────────────────────────────┐
│         SMT SQL MCP Server (server.py + assets.py)      │
│  ┌──────────────────────────────────────────────────┐  │
│  │ 1. list_available_tables (42 表分類與中文地圖)    │  │
│  │ 2. get_table_schema (欄位字典 + 自動生成 event_time)│  │
│  │ 3. execute_sql_query (SQLite in-memory + 自我修復)│  │
│  └──────────────────────────────────────────────────┘  │
└────────────┬─────────────────────────────┬─────────────┘
             ▼                             ▼
┌───────────────────────────┐ ┌───────────────────────────┐
│     schemas/ (42 表)       │ │  subscribed_datas/ (29 表) │
│  - Avro JSON Schema 欄位與 │ │  - 實體 Kafka / IoT 資料集  │
│    中文業務 Doc 說明       │ │  - ⚠️ 企業機密數據          │
└───────────────────────────┘ └───────────────────────────┘
```

---

## 📁 專案檔案結構清單

| 檔案 / 目錄 | 說明 |
| :--- | :--- |
| `server.py` | **MCP Server 主程式**：提供 3 大精簡 SQL Tools，內建前綴正規化、Kafka 信封遞迴解包、大小寫不敏感融合與 SQLite 執行引擎。 |
| `assets.py` | **資料表元數據常數**：定義 42 張資料表的分類（`cim_performance`, `cim_sfcs`, `aiot_equipment`）與結合 Avro Doc 的中文功能說明。 |
| `run_deepagent.py` | **DeepAgent 端到端推理腳本**：連接 MCP Server、掛載 6 步 SOP System Prompt、呼叫 LLM 進行推理並將完整軌跡存為 JSON。 |
| `test_client.py` | **本地輕量驗證腳本**：不經過 LLM，直接以 MCP Client 測試連線、列出工具、查 Schema 與執行 SQL。 |
| `requirements.txt` | 專案 Python 依賴套件清單。 |
| `schemas/` | 42 個 Avro JSON 格式的資料表結構定義檔。 |
| `subscribed_datas/` | 實體 JSON 資料檔（由 Kafka/IoT 訂閱導出，**內含企業機密數據，嚴禁隨意讀取/探查**）。 |
| `agent_results/` | 自動儲存每次 DeepAgent 執行的完整生命週期 JSON 紀錄檔。 |

---

## 🛠️ 核心 3 大精簡 MCP Tools 規範

| Tool 名稱 | 參數 | 功能與特性說明 |
| :--- | :--- | :--- |
| **`list_available_tables`** | `keyword` (可選), `category` (可選) | **[1/3 資料表地圖]** 列出 42 張資料表名稱、所屬類別與中文業務說明，引導 LLM 正確定位目標資料表。 |
| **`get_table_schema`** | `table_name` (必填) | **[2/3 資料表字典]** 查詢指定表的欄位名稱、型別與中文 Doc。支援短表名（如 `oeedetail`）與帶前綴全名；**所有表均已自動補上 `event_time` (TIMESTAMP) 欄位**。 |
| **`execute_sql_query`** | `sql_query` (必填) | **[3/3 自由 SQL 執行引擎]** 支援跨表動態 SQL 查詢（WHERE, GROUP BY, AVG, SUM 等）。內建唯讀限制（僅限 SELECT）、自動 LIMIT 500、前綴自動替換與 Self-Healing 錯誤回傳。 |

---

## 🔑 關鍵技術實作與踩坑經驗 (Key Learnings)

### 1. Kafka Stringified JSON 遞迴解包 (`_unwrap_payload`)
* **現象**：資料集為 Kafka 導出檔時，`"Message"` 或 `"evt_data"` 欄位常為被跳脫的 JSON 字串（例如 `"Message": "{\"front_pressure\": 3.2}"`）。
* **解法**：`server.py` 實作 `_unwrap_payload` 進行深度遞迴 JSON 解析，支援 `Message`、`payload`、`value`、`data`、`evt_data` 各種層級，徹底解決欄位寫入 SQLite 為 `null` 的問題。

### 2. 欄位大小寫雙向索引 (Case-Insensitive Mapping)
* **現象**：Avro Schema 定義為小寫（如 `front_pressure`），但真實設備資料可能為大寫（如 `FRONT_PRESSURE`）。
* **解法**：動態載入 SQLite 時，為每筆資料建立原始名、全小寫、全大寫索引，確保 `SELECT front_pressure` 能精準命中。

### 3. 標準化時間過濾欄位 (`event_time`)
* **機制**：底層自動將 `data_timestamp`、`insert_date_time`、`begintime` 等 Unix Timestamp（秒/毫秒）轉換為 `YYYY-MM-DD HH:MM:SS` 格式的 `event_time`，LLM 可直接以直覺的時間字串過濾。

### 4. MCP 持久化連線 (Persistent Session)
* **效能關鍵**：在 `run_deepagent.py` 中改用 `async with client.session("smt_server") as session:` 搭配 `load_mcp_tools(session)`，避免每次 Tool 調用重複啟動 Python 子程序，**將 Tool 執行速度由數秒縮短至毫秒級**。

### 5. 模型版本相容性
* Google Gemini API 舊版（如 `gemini-2.0-flash`, `gemini-2.5-flash`）已下線並回傳 404。預設模型已更新為 **`gemini-3.6-flash`**。

---

## 🚀 快速上手 (Quick Start)

### 1. 安裝環境依賴
```bash
pip install -r requirements.txt
```

### 2. 設定環境變數 (`.env`)
在專案根目錄建立 `.env` 檔案，填入以下任一支援的 API Key：
```env
# 推薦 (預設使用)
GOOGLE_API_KEY="your-gemini-api-key"
GEMINI_MODEL="gemini-3.6-flash"

# 或使用 Azure OpenAI / OpenAI / Anthropic
# AZURE_OPENAI_API_KEY="your-azure-key"
# AZURE_OPENAI_ENDPOINT="https://your-endpoint.openai.azure.com/"
# AZURE_OPENAI_DEPLOYMENT_NAME="gpt-4o"
# OPENAI_API_KEY="your-openai-key"
# ANTHROPIC_API_KEY="your-anthropic-key"
```

### 3. 本地輕量驗證 (不耗費 LLM Token)
```bash
python test_client.py
```

### 4. 啟動 DeepAgent 智慧推理
```bash
python run_deepagent.py
```

---

## 💡 Token 消耗輕量化實作與成效 (Token-Saving Architecture)

本專案已全面導入 **4 大 Token 輕量化技術**，單次查詢 Token 消耗由 **20,000+ 降至 3,000 ~ 5,000 內**（節省逾 75%）：

1. **Schema Compact 緊湊化 (節省 ~70% Schema Token)**：
   • `get_table_schema` 由繁複的 Avro JSON 轉為簡約 DDL 格式（如 `- front_pressure (FLOAT): 前刮刀 實際壓力`），大幅消除 JSON 引號與鍵名開銷。
2. **Top-K 關鍵字搜尋地圖 (節省 ~90% Map Token)**：
   • `list_available_tables` 內建多詞加權排序搜尋（支援 `keyword="印刷機 刮刀 壓力"`），預設僅回傳高匹配度前 3~10 張表，避免 42 張表全量塞入 Context。
3. **三大 Prompt 黃金準則 (In-DB 運算)**：
   • **嚴禁 `SELECT *`**：強制 Selective Projection，僅投影問題所需欄位。
   • **資料庫內聚合 (In-DB Aggregation)**：強制使用 `AVG()`, `COUNT()`, `SUM()`, `GROUP BY` 在 SQLite 內完成計算，回傳結果由 500 行降為 1 行。
   • **精準 Keyword 定位**：強制呼叫工具時帶入具體關鍵字。
4. **中間工具結果修剪 (Tool Result Pruning)**：
   • 提供 `prune_tool_messages` 機制，在多輪對話中修剪前期的查字典紀錄，避免 Context Window 隨輪次膨脹。
