# server.py
import json
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Any, Optional, Literal

try:
    from mcp.server.fastmcp import FastMCP as MCPServer
except ImportError:
    from mcp.server import MCPServer

mcp = MCPServer("SMT_SQL_MCP_Server")

BASE_DIR = Path(__file__).parent
SCHEMA_DIR = BASE_DIR / "schemas"
DATA_DIR = BASE_DIR / "subscribed_datas"

# 1. 預載 Avro Schemas 快取
SCHEMAS: Dict[str, Dict[str, Any]] = {}
if SCHEMA_DIR.exists():
    for f in SCHEMA_DIR.glob("*.json"):
        try:
            with open(f, "r", encoding="utf-8") as file:
                SCHEMAS[f.stem] = json.load(file)
        except Exception:
            pass

# ponytail: sqlite-in-memory-json-flat | Ceiling: Nested JSON structs flattened only 1 level deep. Upgrade path: duckdb or custom json_tree recursive parser if complex arrays needed.
def _resolve_data_file(table_name: str) -> Optional[Path]:
    """配對資料庫檔案路徑"""
    candidates = [
        DATA_DIR / f"wih.cim.dpm20.{table_name}.json",
        DATA_DIR / f"wih.cim.sfcs.{table_name}.json",
        DATA_DIR / f"wih.mfgdp.aiot.{table_name}.json",
        DATA_DIR / f"{table_name}.json",
        BASE_DIR / f"{table_name}.json",
        BASE_DIR / "data" / f"{table_name}.json"
    ]
    return next((c for c in candidates if c.exists()), None)

def _load_and_flatten_records(table_name: str) -> List[Dict[str, Any]]:
    """載入 JSON 並扁平化 evt_data，同時生成標準化 event_time (TIMESTAMP)"""
    file_path = _resolve_data_file(table_name)
    if not file_path:
        return []
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_events = json.load(f)
        
        records_by_id = {}
        for event in raw_events:
            evt_data = event.get("Message", {}).get("evt_data") if isinstance(event, dict) and "Message" in event else (event.get("evt_data") if isinstance(event, dict) else {})
            if not isinstance(evt_data, dict) or not evt_data:
                continue

            sync_id = evt_data.get("SYNCID") or evt_data.get("uuid") or id(evt_data)
            if evt_data.get("SYNCACTION") == "D":
                records_by_id.pop(sync_id, None)
                continue

            # 標準化 event_time (處理毫秒/秒 Timestamp 或日期字串)
            raw_ts = evt_data.get("data_timestamp") or evt_data.get("insert_date_time") or evt_data.get("BEGINTIME") or evt_data.get("SYNCDATE")
            event_time = None
            if raw_ts:
                try:
                    ts_num = float(raw_ts)
                    if ts_num > 1e11: ts_num /= 1000.0  # 毫秒轉秒
                    event_time = sqlite3.connect(":memory:").execute("SELECT datetime(?, 'unixepoch', 'localtime')", (ts_num,)).fetchone()[0]
                except Exception:
                    event_time = str(raw_ts)

            evt_data["event_time"] = event_time
            records_by_id[sync_id] = evt_data
            
        return list(records_by_id.values())
    except Exception:
        return []

# ==============================================================================
# 3 大精簡 MCP Tools
# ==============================================================================

@mcp.tool()
def list_available_tables(
    keyword: Optional[str] = None,
    category: Optional[Literal["cim_performance", "cim_sfcs", "aiot_equipment"]] = None
) -> Dict[str, Any]:
    """
    [1/3 資料表地圖] 列出可供查詢的 42 張資料表名稱與類別說明。
    當不確定資料表名稱時優先呼叫。
    """
    tables = []
    for k, v in SCHEMAS.items():
        doc = v.get("doc", "")
        # 從 namespace 判斷類別
        ns = v.get("namespace", "")
        cat = "cim_performance" if "dpm" in ns else ("cim_sfcs" if "sfcs" in ns else "aiot_equipment")
        
        if category and cat != category:
            continue
        if keyword and keyword.lower() not in k.lower() and keyword.lower() not in doc.lower():
            continue
            
        tables.append({"table_name": k, "category": cat, "description": doc or f"SMT {k} 資料表"})
        
    return {"status": "success", "total_tables": len(tables), "tables": tables}

@mcp.tool()
def get_table_schema(table_name: str) -> Dict[str, Any]:
    """
    [2/3 資料表字典] 查詢特定 table_name 的詳細欄位名稱、型別與中文 Doc。
    撰寫 SQL 查詢前務必先呼叫此 Tool 以確認欄位拼字。
    註：所有表均已自動補上標準時間過濾欄位 `event_time` (格式: YYYY-MM-DD HH:MM:SS)。
    """
    schema = SCHEMAS.get(table_name)
    if not schema:
        return {"status": "error", "message": f"Table '{table_name}' not found."}

    fields_info = []
    # 解析 Avro 中的所有欄位 (相容直接欄位與 evt_data 嵌套欄位)
    for field in schema.get("fields", []):
        if field.get("name") == "evt_data" and isinstance(field.get("type"), dict):
            for sub in field["type"].get("fields", []):
                fields_info.append({
                    "column_name": sub.get("name"),
                    "type": str(sub.get("type")),
                    "doc": sub.get("doc", "")
                })
        else:
            fields_info.append({
                "column_name": field.get("name"),
                "type": str(field.get("type")),
                "doc": field.get("doc", "")
            })

    # 加入標準化衍生欄位
    fields_info.append({
        "column_name": "event_time",
        "type": "TIMESTAMP",
        "doc": "標準化時間過濾欄位 (格式: YYYY-MM-DD HH:MM:SS)，強烈建議在 WHERE 中使用"
    })

    return {
        "status": "success",
        "table_name": table_name,
        "total_columns": len(fields_info),
        "columns": fields_info
    }

@mcp.tool()
def execute_sql_query(sql_query: str) -> Dict[str, Any]:
    """
    [3/3 自由 SQL 執行引擎] 輸入 ANSI SQL 進行跨表動態查詢與統計 (支援 SELECT, WHERE, GROUP BY, AVG, SUM, COUNT 等)。
    内建唯讀保護與自我修復 Error 回傳。
    
    :param sql_query: 標準 SQL 語法 (例: SELECT AVG(front_pressure) FROM aiot_smt_printer_real_processing_data_wihn2 WHERE event_time >= '2026-08-17')
    """
    # 1. 唯讀與安全檢查
    clean_sql = sql_query.strip()
    if not re.match(r"^\s*SELECT", clean_sql, re.IGNORECASE):
        return {"status": "error", "error_type": "SecurityError", "message": "唯讀限制：僅允許 SELECT 查詢！"}

    # 2. 自動限制 LIMIT 防爆
    if not re.search(r"\bLIMIT\b", clean_sql, re.IGNORECASE):
        clean_sql += " LIMIT 500"

    # 3. 解析 SQL 中出現的 Table Name，動態把資料載入 SQLite in-memory
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # 找出所有已註冊的表名
    used_tables = [t for t in SCHEMAS.keys() if re.search(r"\b" + re.escape(t) + r"\b", clean_sql)]
    
    for t in used_tables:
        records = _load_and_flatten_records(t)
        
        # 收集 Schema 所有欄位定義
        schema = SCHEMAS.get(t, {})
        schema_cols = []
        for field in schema.get("fields", []):
            if field.get("name") == "evt_data" and isinstance(field.get("type"), dict):
                schema_cols.extend([sub.get("name") for sub in field["type"].get("fields", [])])
            else:
                schema_cols.append(field.get("name"))
        schema_cols.append("event_time")

        if records:
            cols = list(dict.fromkeys(list(records[0].keys()) + schema_cols))
        else:
            cols = schema_cols

        # 動態建立 SQLite 資料表
        col_defs = ", ".join([f'"{c}" TEXT' for c in cols if c])
        conn.execute(f'CREATE TABLE "{t}" ({col_defs})')

        if records:
            placeholders = ", ".join(["?"] * len(cols))
            insert_sql = f'INSERT INTO "{t}" VALUES ({placeholders})'
            rows = [[str(r.get(c)) if r.get(c) is not None else None for c in cols] for r in records]
            conn.executemany(insert_sql, rows)

    # 4. 執行 SQL (Self-Healing 捕捉 Error 回傳給 LLM 修正)
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