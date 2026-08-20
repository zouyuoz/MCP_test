# server.py
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Literal

try:
    from mcp.server.fastmcp import FastMCP as MCPServer
except ImportError:
    from mcp.server import MCPServer

from assets import TABLE_METADATA

mcp = MCPServer("SMT_SQL_MCP_Server")

BASE_DIR = Path(__file__).parent
SCHEMA_DIR = BASE_DIR / "schemas"
DATA_DIR = BASE_DIR / "subscribed_datas"

# CIM / Kafka 信封欄位 (非實際業務資料，需過濾)
ENVELOPE_FIELDS = {"evt_ns", "evt_tp", "evt_dt", "evt_pubBy", "evt_data"}

# 1. 預載 Avro Schemas 快取
SCHEMAS: Dict[str, Dict[str, Any]] = {}
if SCHEMA_DIR.exists():
    for f in SCHEMA_DIR.glob("*.json"):
        try:
            with open(f, "r", encoding="utf-8") as file:
                SCHEMAS[f.stem] = json.load(file)
        except Exception:
            pass

def _normalize_table_name(name: str) -> str:
    """提取標準短表名 (例如 'wih.cim.dpm20.oeedetail' -> 'oeedetail')"""
    clean = name.strip('"`[] \t\n')
    if "." in clean:
        clean = clean.split(".")[-1]
    return clean

def _simplify_type(col_name: str, raw_type: Any) -> str:
    if col_name == "event_time":
        return "TIMESTAMP"
    s = str(raw_type).lower()
    if any(k in s for k in ["long", "int", "integer"]):
        return "INT"
    if any(k in s for k in ["float", "double", "decimal", "number"]):
        return "FLOAT"
    # ponytail: type-heuristic | Ceiling: infers FLOAT from common metric keywords. Upgrade path: explicit type registry per schema.
    metric_keywords = ["pressure", "speed", "temp", "humidity", "ratio", "rate", "offset", "distance", "height", "area", "volume", "hrs", "hour", "productivity", "efficiency"]
    if any(k in col_name.lower() for k in metric_keywords):
        return "FLOAT"
    return "TEXT"

# ponytail: sqlite-in-memory-json-flat | Ceiling: Nested JSON structs flattened only 1 level deep. Upgrade path: duckdb or custom json_tree recursive parser if complex arrays needed.
def _extract_columns(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """從 Avro Schema 提取實際業務欄位 (自動展開 evt_data 並過濾 Kafka/CIM 信封層)，並補上 event_time"""
    cols = []
    for field in schema.get("fields", []):
        name = field.get("name")
        if name == "evt_data" and isinstance(field.get("type"), dict):
            for sub in field["type"].get("fields", []):
                c_name = sub.get("name", "")
                cols.append({
                    "column_name": c_name,
                    "type": _simplify_type(c_name, sub.get("type")),
                    "doc": sub.get("doc", "")
                })
        elif name not in ENVELOPE_FIELDS:
            c_name = name or ""
            cols.append({
                "column_name": c_name,
                "type": _simplify_type(c_name, field.get("type")),
                "doc": field.get("doc", "")
            })

    cols.append({
        "column_name": "event_time",
        "type": "TIMESTAMP",
        "doc": "標準化時間過濾欄位 (格式: YYYY-MM-DD HH:MM:SS)，強烈建議在 WHERE 中使用"
    })
    return cols

def _resolve_data_file(table_name: str) -> Optional[Path]:
    """雙向配對資料庫檔案路徑 (相容 short name 與 full namespace)"""
    clean_name = _normalize_table_name(table_name)

    candidates = [
        DATA_DIR / f"wih.cim.dpm20.{clean_name}.json",
        DATA_DIR / f"wih.cim.sfcs.{clean_name}.json",
        DATA_DIR / f"wih.mfgdp.aiot.{clean_name}.json",
        DATA_DIR / f"{clean_name}.json",
        BASE_DIR / f"{clean_name}.json",
        BASE_DIR / "data" / f"{clean_name}.json"
    ]
    for c in candidates:
        if c.exists():
            return c

    # 通配符模糊查找 (例如非標準前綴或自定義檔名)
    if DATA_DIR.exists():
        for match in DATA_DIR.glob(f"*{clean_name}.json"):
            return match

    return None

def _unwrap_payload(obj: Any) -> Dict[str, Any]:
    """遞迴解開 Kafka/API 信封層 (支援 Message, payload, value 為字串或 dict，以及 evt_data 巢狀)"""
    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            if isinstance(parsed, dict):
                return _unwrap_payload(parsed)
        except Exception:
            return {}

    if not isinstance(obj, dict):
        return {}

    # 1. 檢查是否有外層信封 (Message, message, payload, value, data, body)
    for k in ["Message", "message", "payload", "value", "data", "body"]:
        if k in obj:
            inner = obj[k]
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except Exception:
                    pass
            if isinstance(inner, dict):
                return _unwrap_payload(inner)

    # 2. 檢查是否有內層 evt_data
    if "evt_data" in obj:
        inner_evt = obj["evt_data"]
        if isinstance(inner_evt, str):
            try:
                inner_evt = json.loads(inner_evt)
            except Exception:
                pass
        if isinstance(inner_evt, dict):
            return _unwrap_payload(inner_evt)

    return obj

def _load_and_flatten_records(table_name: str) -> List[Dict[str, Any]]:
    """載入 JSON 並深度解開各類信封 (字串/字典/Kafka/evt_data)，同時生成標準化 event_time (TIMESTAMP)"""
    file_path = _resolve_data_file(table_name)
    if not file_path:
        return []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_events = json.load(f)

        if not isinstance(raw_events, list):
            raw_events = [raw_events]

        records_by_id = {}
        for event in raw_events:
            evt_data = _unwrap_payload(event)
            if not isinstance(evt_data, dict) or not evt_data:
                continue

            # 建立大小寫雙向索引字典，防止大小寫不匹配
            normalized_dict = {}
            for k, v in evt_data.items():
                if isinstance(k, str):
                    normalized_dict[k] = v
                    normalized_dict[k.lower()] = v
                    normalized_dict[k.upper()] = v
                else:
                    normalized_dict[k] = v

            # 支援 CIM / SFCS / AioT 各自的 ID 與 Action 欄位 (大小寫不敏感)
            sync_id = (
                normalized_dict.get("syncid")
                or normalized_dict.get("sync_id")
                or normalized_dict.get("uuid")
                or id(evt_data)
            )
            sync_action = normalized_dict.get("syncaction") or normalized_dict.get("sync_op")
            if sync_action == "D":
                records_by_id.pop(sync_id, None)
                continue

            # 標準化 event_time (使用 Python 原生 datetime 取代 SQLite 連線)
            raw_ts = (
                normalized_dict.get("data_timestamp")
                or normalized_dict.get("insert_date_time")
                or normalized_dict.get("begintime")
                or normalized_dict.get("syncdate")
                or normalized_dict.get("trndate")
                or normalized_dict.get("sync_ts")
                or normalized_dict.get("timestamp")
                or normalized_dict.get("event_time")
            )
            event_time = None
            if raw_ts is not None:
                try:
                    ts_num = float(raw_ts)
                    if ts_num > 1e11:
                        ts_num /= 1000.0  # 毫秒轉秒
                    event_time = datetime.fromtimestamp(ts_num).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    event_time = str(raw_ts)

            # 保留所有原始欄位 + 小寫/大寫別名，並補上 event_time
            record = dict(evt_data)
            for k, v in evt_data.items():
                if isinstance(k, str):
                    record[k.lower()] = v
            record["event_time"] = event_time
            records_by_id[sync_id] = record

        return list(records_by_id.values())
    except Exception:
        return []

# ==============================================================================
# 3 大精簡 MCP Tools
# ==============================================================================

# ponytail: keyword-ranker | Ceiling: Multi-term substring ranking and category filter. Upgrade path: TF-IDF / BM25 / vector embeddings if 1000+ tables.
@mcp.tool()
def list_available_tables(
    keyword: Optional[str] = None,
    category: Optional[Literal["cim_performance", "cim_sfcs", "aiot_equipment"]] = None,
    limit: int = 10
) -> str:
    """
    [1/3 資料表地圖] 列出可供查詢的資料表名稱、類別與業務說明 (Compact Markdown 模式)。
    強烈建議帶入 keyword 進行精準匹配 (例如 keyword="printer" 或 keyword="前刮刀 壓力")。

    :param keyword: 搜尋關鍵字或問題描述 (支援多詞空格分割，如 "印刷機 刮刀 壓力")
    :param category: 資料表分類 ("cim_performance", "cim_sfcs", "aiot_equipment")
    :param limit: 最多回傳幾張表 (預設 10)
    """
    all_tables = []
    for k in sorted(SCHEMAS.keys()):
        meta = TABLE_METADATA.get(k, {})
        cat = meta.get("category", "aiot_equipment")
        desc = meta.get("description", f"SMT {k} 資料表")
        all_tables.append((k, cat, desc))

    if category:
        all_tables = [t for t in all_tables if t[1] == category]

    if keyword and keyword.strip():
        kw_clean = keyword.strip().lower()
        terms = [t for t in re.split(r"[\s,]+", kw_clean) if t]

        scored = []
        for name, cat, desc in all_tables:
            name_lower = name.lower()
            desc_lower = desc.lower()

            score = 0
            # 完全匹配高權重
            if kw_clean in name_lower:
                score += 10
            if kw_clean in desc_lower:
                score += 8

            # 分詞匹配加分
            for term in terms:
                if term in name_lower:
                    score += 5
                if term in desc_lower:
                    score += 3
                if term in cat.lower():
                    score += 2

            if score > 0:
                scored.append((score, name, cat, desc))

        scored.sort(key=lambda x: x[0], reverse=True)
        top_matches = scored[:limit]

        if not top_matches:
            return f"No tables found matching keyword '{keyword}'. Try another keyword (e.g. 'printer', 'fpyr', 'reflow', 'aoi')."

        lines = [f"Found {len(top_matches)} matching table(s) for keyword '{keyword}':"]
        for _, name, cat, desc in top_matches:
            lines.append(f"- {name} [{cat}]: {desc}")
        return "\n".join(lines)
    else:
        lines = [f"Available SMT Tables ({len(all_tables)} total - recommendation: use keyword parameter to search):"]
        for name, cat, desc in all_tables[:limit]:
            short_desc = desc.split("（")[0] if "（" in desc else desc[:30]
            lines.append(f"- {name} [{cat}]: {short_desc}")
        if len(all_tables) > limit:
            lines.append(f"... and {len(all_tables) - limit} more tables. Please specify keyword to filter.")
        return "\n".join(lines)

@mcp.tool()
def get_table_schema(table_name: str) -> str:
    """
    [2/3 資料表字典] 查詢特定 table_name 的詳細欄位名稱、型別與中文說明 (Compact DDL 模式)。
    相容短表名 (如 oeedetail) 或帶前綴全名 (如 wih.cim.dpm20.oeedetail)。
    所有表均已自動補上標準時間過濾欄位 `event_time` (格式: YYYY-MM-DD HH:MM:SS)。
    """
    clean_name = _normalize_table_name(table_name)
    schema = SCHEMAS.get(clean_name)

    if schema:
        fields_info = _extract_columns(schema)
    else:
        # Fallback: 如果 schemas/ 內沒有，從實體 subscribed_datas/ 取樣推導欄位
        records = _load_and_flatten_records(clean_name)
        if not records:
            return f"Error: Table '{table_name}' (canonical: '{clean_name}') not found in schemas or subscribed data."
        fields_info = [{"column_name": k, "type": _simplify_type(k, "TEXT"), "doc": ""} for k in records[0].keys()]
        if not any(f["column_name"] == "event_time" for f in fields_info):
            fields_info.append({
                "column_name": "event_time",
                "type": "TIMESTAMP",
                "doc": "標準化時間過濾欄位 (格式: YYYY-MM-DD HH:MM:SS)，強烈建議在 WHERE 中使用"
            })

    lines = [f"Table: {clean_name}", "Columns:"]
    for f in fields_info:
        c_name = f.get("column_name", "")
        c_type = f.get("type", "TEXT")
        c_doc = f.get("doc", "").strip()
        if c_doc:
            lines.append(f"- {c_name} ({c_type}): {c_doc}")
        else:
            lines.append(f"- {c_name} ({c_type})")

    return "\n".join(lines)

@mcp.tool()
def execute_sql_query(sql_query: str) -> Dict[str, Any]:
    """
    [3/3 自由 SQL 執行引擎] 輸入 ANSI SQL 進行跨表動態查詢與統計 (支援 SELECT, WHERE, GROUP BY, AVG, SUM, COUNT 等)。
    內建唯讀保護、前綴自動正規化、大小寫不敏感融合與自我修復 Error 回傳。

    :param sql_query: 標準 SQL 語法 (例: SELECT AVG(front_pressure) FROM aiot_smt_printer_real_processing_data_wihn2 WHERE event_time >= '2026-08-17')
    """
    # 1. 唯讀與安全檢查
    clean_sql = sql_query.strip()
    if not re.match(r"^\s*SELECT", clean_sql, re.IGNORECASE):
        return {"status": "error", "error_type": "SecurityError", "message": "唯讀限制：僅允許 SELECT 查詢！"}

    # 2. 自動限制 LIMIT 防爆
    if not re.search(r"\bLIMIT\b", clean_sql, re.IGNORECASE):
        clean_sql += " LIMIT 500"

    # 3. SQL 表名前處理：自動將各類前綴 (如 wih.cim.dpm20.oeedetail 或 "wih.cim.dpm20.oeedetail") 正規化為標準短表名
    all_canonical_tables = list(SCHEMAS.keys())
    used_canonical_tables = set()

    for canonical_name in all_canonical_tables:
        pattern = rf'(?:wih\.[a-zA-Z0-9_.]+\.)?({re.escape(canonical_name)})\b'
        if re.search(pattern, clean_sql, re.IGNORECASE):
            used_canonical_tables.add(canonical_name)
            # 將 SQL 中的前綴全名替換為短表名 (加雙引號以防關鍵字衝突)
            clean_sql = re.sub(
                rf'["\']?(?:wih\.[a-zA-Z0-9_.]+\.)?({re.escape(canonical_name)})["\']?',
                rf'"\1"',
                clean_sql,
                flags=re.IGNORECASE
            )

    # 4. 動態把相關資料載入 SQLite in-memory
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    for t in used_canonical_tables:
        records = _load_and_flatten_records(t)
        schema = SCHEMAS.get(t, {})
        schema_cols = [c["column_name"] for c in _extract_columns(schema) if c.get("column_name")]

        # 雙向融合 Schema 欄位與 Data 實際出現的欄位
        if records:
            cols = list(dict.fromkeys(schema_cols + list(records[0].keys())))
        else:
            cols = schema_cols

        # 動態建立 SQLite 資料表
        col_defs = ", ".join([f'"{c}" TEXT' for c in cols if c])
        conn.execute(f'CREATE TABLE "{t}" ({col_defs})')

        if records:
            placeholders = ", ".join(["?"] * len(cols))
            insert_sql = f'INSERT INTO "{t}" VALUES ({placeholders})'
            # 支援大小寫不敏感取值 (先查原名 -> 小寫 -> 大寫)
            rows = []
            for r in records:
                row = []
                for c in cols:
                    val = r.get(c)
                    if val is None and isinstance(c, str):
                        val = r.get(c.lower())
                    if val is None and isinstance(c, str):
                        val = r.get(c.upper())
                    row.append(str(val) if val is not None else None)
                rows.append(row)
            conn.executemany(insert_sql, rows)

    # 5. 執行 SQL (Self-Healing 捕捉 Error 回傳給 LLM 修正)
    try:
        cursor = conn.execute(clean_sql)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return {
            "status": "success",
            "executed_sql": clean_sql,
            "row_count": len(rows),
            "data": rows
        }
    except Exception as e:
        conn.close()
        return {
            "status": "error",
            "error_type": type(e).__name__,
            "message": str(e),
            "hint": "請檢查 SQL 語法或欄位拼字。可透過 get_table_schema 工具確認正確欄位名稱。"
        }

if __name__ == "__main__":
    mcp.run(transport="stdio")