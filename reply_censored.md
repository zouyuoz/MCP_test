
# OEE Detail Schema 完整解析
 
## Schema 與 單筆資料結構
 
Schema:
```
{"type":"record","name":"oeedetail","namespace":"xxx...","fields":[{"name":"evt_ns","type":"string","doc":"event namespace"},{"name":"evt_tp","type":"string","doc":"event topic"},{"name":"evt_dt","type":[{"type":"long","java-class":"java.util.Date"}],"doc":"event datetime"},{"name":"evt_pubBy","type":"string","doc":"event published by"},{"name":"evt_data","type":{"type":"record","name":"oeedetail_evt_data","fields":[{"name":"SYNCID","type":["null","string"],"doc":"synchronized id","default":null},{"name":"SYNCDATE","type":"string","doc":"synchronized time","default":"xxx..."},{"name":"SYNCACTION","type":"string","doc":"status","default":"xxx..."},{"name":"PLANT","type":"string","doc":"plant code"},{"name":"SHIFTDATE","type":["null","string"],"doc":"shift date","default":null},{"name":"SITE","type":["null","string"],"doc":"site","default":null},{"name":"SHIFT","type":["null","string"],"doc":"shift name","default":null},{"name":"SHIFTTYPE","type":"string","doc":"day shift/night shift"},{"name":"BU","type":["null","string"],"doc":"bu","default":null},{"name":"LINE","type":["null","string"],"doc":"production line","default":null},{"name":"TOTALPRESENTTIME","type":["null","string"],"doc":"total present time","default":null},{"name":"EARNEDHRS","type":["null","string"],"doc":"earned hours","default":null},{"name":"IEEARNEDHRS","type":["null","string"],"doc":"ie earned hours","default":null},{"name":"LOSSHRS","type":["null","string"],"doc":"loss hours","default":null},{"name":"CHARGEHRS","type":["null","string"],"doc":"charge hours","default":null},{"name":"EFFICIENCY","type":["null","string"],"doc":"efficiency","default":null},{"name":"PRODUCTIVITY","type":["null","string"],"doc":"productivity","default":null},{"name":"CHARGE","type":["null","string"],"doc":"charge","default":null},{"name":"IEPRODUCTIVITY","type":["null","string"],"doc":"ie productivity","default":null}]},"doc":"event data"}]}
```
 
單筆資料:
```
[{"evt_ns":"xxx...","evt_tp":"xxx...","evt_dt":"xxx...","evt_pubBy":"xxx...","evt_data":{"SYNCID":"xxx...","SYNCDATE":"xxx...","SYNCACTION":"xxx...","PLANT":"xxx...","SHIFTDATE":"xxx...","SITE":"xxx...","SHIFT":"xxx...","SHIFTTYPE":"xxx...","BU":"xxx...","LINE":"xxx...","TOTALPRESENTTIME":"xxx...","EARNEDHRS":"xxx...","IEEARNEDHRS":"xxx...","LOSSHRS":"xxx...","CHARGEHRS":"xxx...","EFFICIENCY":"xxx...","PRODUCTIVITY":"xxx...","CHARGE":"xxx...","IEPRODUCTIVITY":"xxx..."}}]
```
 
## 資料結構拆解
 
```
整筆資料（Kafka Message）
├── 📡 Kafka Metadata
│   ├── Timestamp     xxx...
│   ├── Topic         xxx...
│   ├── Partition     xxx...
│   └── Offset        xxx...
│
├── 📋 事件 Metadata（evt_*）
│   ├── evt_ns        "xxx..."
│   ├── evt_tp        "xxx..."
│   ├── evt_dt        "xxx..."（Unix timestamp ms）
│   └── evt_pubBy     "xxx..."（來源）
│
└── 📦 evt_data（業務資料）
    ├── 🔑 同步資訊
    │   ├── SYNCID      "xxx..."
    │   ├── SYNCDATE    "xxx..."
    │   └── SYNCACTION  "xxx..." ← 重要！Insert/Update/Delete
    │
    ├── 📅 時間維度
    │   ├── SHIFTDATE   "xxx..."（班別日期，注意格式 YYYYMMDD）
    │   ├── SHIFT       "xxx..."（班別代碼）
    │   └── SHIFTTYPE   "xxx..."
    │
    ├── 🏭 生產維度
    │   ├── PLANT       "xxx..."
    │   ├── SITE        "xxx..."
    │   ├── BU          "xxx..."
    │   └── LINE        "xxx..."
    │
    └── 📊 OEE 核心指標
        ├── TOTALPRESENTTIME  "xxx..."      出勤總時數（小時）
        ├── EARNEDHRS         "xxx..."    實際產出工時
        ├── IEEARNEDHRS       "xxx..."       IE 標準產出工時
        ├── LOSSHRS           "xxx..."    損失工時
        ├── CHARGEHRS         "xxx..."       計費工時
        ├── EFFICIENCY        "xxx..."   效率 %
        ├── PRODUCTIVITY      "xxx..."   生產力 %
        ├── CHARGE            "xxx..."       費用計算相關
        └── IEPRODUCTIVITY    "xxx..."       IE 生產力 %
```
 
---
 
## 關鍵數字驗證
 
從這筆樣本資料可以驗證邏輯：
 
$$
EFFICIENCY = \frac{EARNEDHRS}{TOTALPRESENTTIME} \times 100
$$
 
$$
= \frac{xxx...}{xxx...} \times 100 = xxx...\% \checkmark
$$
 
損失工時驗證：
 
$$
LOSSHRS = TOTALPRESENTTIME - EARNEDHRS = xxx... - xxx... = xxx... \checkmark
$$
 
---
 
## 幾個需要注意的細節
 
### ⚠️ 1. SYNCACTION 很重要
```
"U" = Update   ← 這筆是更新，不是新增
"I" = Insert   ← 新增
"D" = Delete   ← 刪除
 
→ API 在消費資料時，必須處理 Upsert 邏輯
  不能單純 append，要用 SYNCID 做去重
```
 
(註：上面三種行為代號若為範例也可視為機敏範本，實務中可替換為 "xxx...")
 
### ⚠️ 2. 日期格式不一致
```
SHIFTDATE：  "xxx..."        → YYYYMMDD（無分隔符）
SYNCDATE：   "xxx..."  → 標準 datetime
evt_dt：      "xxx..."    → Unix timestamp (ms)
 
→ API 層要做格式統一轉換
```
 
### ⚠️ 3. SHIFTDATE ≠ SYNCDATE
```
SHIFTDATE = xxx...  （班別是某日的班）
SYNCDATE  = xxx...  （但資料在之後才同步進來）
 
→ 查詢時要用 SHIFTDATE，不要用 SYNCDATE
  否則夜班資料會被歸到隔天
```
 
### ⚠️ 4. 所有數值都是 string 型別
```
"EFFICIENCY": "xxx..."  ← 這是字串，不是數字！
 
→ API 回傳前要做型別轉換
→ 運算前要做 null 值保護
```
 
---
 
## API 端點設計
 
```http
GET /api/v1/efficiency/oee
```
 
### Query Parameters
 
| 參數         | 類型   | 必填 | 說明                        | 範例     |
| ------------ | ------ | ---- | --------------------------- | -------- |
| `start_date` | string | ✅    | 班別起始日 YYYY-MM-DD       | `xxx...` |
| `end_date`   | string | ✅    | 班別結束日 YYYY-MM-DD       | `xxx...` |
| `line`       | string | ❌    | 產線代號                    | `xxx...` |
| `plant`      | string | ❌    | 廠別                        | `xxx...` |
| `bu`         | string | ❌    | 事業單位                    | `xxx...` |
| `shift_type` | string | ❌    | `Day shift` / `Night shift` | `xxx...` |
 
### 回傳格式
 
```json
{
  "status": "success",
  "query": {
    "start_date": "xxx...",
    "end_date": "xxx...",
    "line": "xxx...",
    "shift_type": "xxx..."
  },
  "summary": {
    "avg_efficiency": "xxx...",
    "avg_productivity": "xxx...",
    "total_present_hours": "xxx...",
    "total_earned_hours": "xxx...",
    "total_loss_hours": "xxx...",
    "loss_rate": "xxx..."
  },
  "records": [
    {
      "shift_date": "xxx...",
      "shift": "xxx...",
      "shift_type": "xxx...",
      "plant": "xxx...",
      "bu": "xxx...",
      "line": "xxx...",
      "total_present_time": "xxx...",
      "earned_hrs": "xxx...",
      "loss_hrs": "xxx...",
      "efficiency": "xxx...",
      "productivity": "xxx..."
    }
  ]
}
```
 
---
 
## Function Calling 定義
 
```json
{
  "name": "get_oee_detail",
  "description": "查詢產線的 OEE 效率與生產力數據，包含出勤工時、實際產出工時、損失工時與效率百分比。以班別日期為主要查詢維度。",
  "parameters": {
    "type": "object",
    "properties": {
      "start_date": {
        "type": "string",
        "description": "班別起始日期，格式 YYYY-MM-DD"
      },
      "end_date": {
        "type": "string",
        "description": "班別結束日期，格式 YYYY-MM-DD"
      },
      "line": {
        "type": "string",
        "description": "產線代號，例如 xxx..."
      },
      "plant": {
        "type": "string",
        "description": "廠別代號，例如 xxx..."
      },
      "shift_type": {
        "type": "string",
        "enum": ["Day shift", "Night shift"],
        "description": "日班或夜班"
      }
    },
    "required": ["start_date", "end_date"]
  }
}
```
 
---
 
## 目前掌握的資料全貌
 
```
✅ oeedetail  → 已有 Schema + 樣本，可以開始實作
⏸ fpyr       → 已有 Schema，但目前沒有訂閱資料
 
建議下一步：
□ 確認 SYNCACTION "xxx..." 的 Upsert 處理邏輯
□ 確認 LINE 的所有可能值（xxx...）
□ 確認 SHIFT 代碼的命名規則
□ 挑下一個 Schema 繼續解析
   推薦：oeeissue（可以解釋 OEE 損失的原因）
```
 
> **`oeeissue` 和 `oeedetail` 是天然的一對**——一個告訴你效率是多少，另一個告訴你為什麼。把這兩個串起來，聊天機器人就能回答「xxx...」這類問題！