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

# ponytail: sqlite-in-memory-json-flat | Ceiling: Nested JSON structs flattened only 1 level deep. Upgrade path: duckdb or custom json_tree recursive parser if complex arrays needed.
def _extract_columns(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """從 Avro Schema 提取實際業務欄位 (自動展開 evt_data 並過濾 Kafka/CIM 信封層)，並補上 event_time"""
    cols = []
    for field in schema.get("fields", []):
        name = field.get("name")
        if name == "evt_data" and isinstance(field.get("type"), dict):
            for sub in field["type"].get("fields", []):
                cols.append({
                    "column_name": sub.get("name"),
                    "type": str(sub.get("type")),
                    "doc": sub.get("doc", "")
                })
        elif name not in ENVELOPE_FIELDS:
            cols.append({
                "column_name": name,
                "type": str(field.get("type")),
                "doc": field.get("doc", "")
            })

    cols.append({
        "column_name": "event_time",
        "type": "TIMESTAMP",
        "doc": "標準化時間過濾欄位 (格式: YYYY-MM-DD HH:MM:SS)，強烈建議在 WHERE 中使用"
    })
    return cols

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
    """載入 JSON 並支援 Kafka 信封 / evt_data / Flat record，同時生成標準化 event_time (TIMESTAMP)"""
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
            if not isinstance(event, dict):
                continue

            # 兼容 3 種結構：Message.evt_data、直接 evt_data、或 Flat record
            if "Message" in event and isinstance(event["Message"], dict) and "evt_data" in event["Message"]:
                evt_data = event["Message"]["evt_data"]
            elif "evt_data" in event and isinstance(event["evt_data"], dict):
                evt_data = event["evt_data"]
            else:
                evt_data = event

            if not isinstance(evt_data, dict) or not evt_data:
                continue

            # 支援 CIM / SFCS / AioT 各自的 ID 與 Action 欄位
            sync_id = (
                evt_data.get("SYNCID")
                or evt_data.get("SYNC_ID")
                or evt_data.get("uuid")
                or id(evt_data)
            )
            sync_action = evt_data.get("SYNCACTION") or evt_data.get("SYNC_OP")
            if sync_action == "D":
                records_by_id.pop(sync_id, None)
                continue

            # 標準化 event_time (使用 Python 原生 datetime 取代 SQLite 連線，零開銷高效率)
            raw_ts = (
                evt_data.get("data_timestamp")
                or evt_data.get("insert_date_time")
                or evt_data.get("BEGINTIME")
                or evt_data.get("SYNCDATE")
                or evt_data.get("TRNDATE")
                or evt_data.get("SYNC_TS")
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

            # 複製資料避免污染原始 dict
            record = dict(evt_data)
            record["event_time"] = event_time
            records_by_id[sync_id] = record
            
        return list(records_by_id.values())
    except Exception:
        return []

# ==============================================================================
# SMT 42 張資料表語義字典 (結合 data service description.txt 與 Avro Schema 欄位 doc)
# ==============================================================================
TABLE_METADATA: Dict[str, Dict[str, str]] = {
    # === Group 1: CIM / DPM20 製造績效指標 (14 筆) ===
    "alert": {
        "category": "cim_performance",
        "description": "即時警報與異常通知記錄（包含 ALERTID、主旨 SUBJECT、警報內容 CONTENT、發送來源與接收對象 SENDFROM/SENDTO）"
    },
    "cssr": {
        "category": "cim_performance",
        "description": "客戶服務達成率與產線人力綁定統計（包含廠別 PLANT、產線 LINE、客戶 CUSTOMER、時段 PERIOD、綁定人數 BOUNDEMPNUM 與總人數 BOUNDEMPTOTAL）"
    },
    "fpyr": {
        "category": "cim_performance",
        "description": "產線直通良率（First Pass Yield Rate, FPYR）總體統計（包含產線 LINE、時段 PERIOD、起訖時間 BEGINTIME/ENDTIME、投入數、產出數與良率）"
    },
    "fpyrbymodel": {
        "category": "cim_performance",
        "description": "依產品機種（MODEL）分類之直通良率 FPYR 統計（包含客戶 CUSTOMER、機種 MODEL、各製程站別投入/產出數與良率 FPYR）"
    },
    "oeedetail": {
        "category": "cim_performance",
        "description": "產線 OEE 設備總體效率詳細數據（包含班別 SHIFT/SHIFTTYPE、產線 LINE、總在籍工時 TOTALPRESENTTIME、賺得工時 EARNEDHRS、稼動率與效率指標）"
    },
    "oeedetail_chargehours": {
        "category": "cim_performance",
        "description": "OEE 計費工時與停機損失統計（包含產線 LINE、機種 MODEL、料號 UPN、停機時數 STOPHOUR、閒置工時 IDLEHANDS 與稼動分析）"
    },
    "oeeissue": {
        "category": "cim_performance",
        "description": "OEE 停機異常事件與原因記錄（包含產線 LINE、機種 MODEL、料號 UPN、異常原因代碼與說明 CODE/CODEDESC、停機時數 STOPHOURS）"
    },
    "oeeissuegroup": {
        "category": "cim_performance",
        "description": "OEE 異常分群與彙總統計（包含時段 PERIOD、產線 LINE、客戶 CUSTOMER、停機時數 STOPHOUR 與總停機時數 TOTALSTOPHOUR）"
    },
    "pdtissues": {
        "category": "cim_performance",
        "description": "生產問題與工時損失清單（包含問題編號 ISSUEID、機種 MODEL、班別 SHIFTID、工時損失 LOSSHOUR、原因代碼 CODE 與說明 CODEDESCRIPTION）"
    },
    "productitvity": {
        "category": "cim_performance",
        "description": "生產力（Productivity）綜合指標統計（包含時段 PERIOD、產線 LINE、客戶 CUSTOMER、生產力數值 PRODUCTIVITY 與賺得工時 EARNHOUR）"
    },
    "uph": {
        "category": "cim_performance",
        "description": "每小時產出數（Units Per Hour, UPH）統計（包含時段 PERIOD、產線 LINE、製程站別 PROCESS、班別 SHIFT、目標與實際 UPH）"
    },
    "upph": {
        "category": "cim_performance",
        "description": "每人每小時產出數（Units Per Person Hour, UPPH）統計（包含時段 PERIOD、產線 LINE、產出量 OUTPUT、在籍工時 PRESENTHOUR 與 UPPH）"
    },
    "upphn": {
        "category": "cim_performance",
        "description": "UPPH 變形指標統計（含夜班/特殊班別調整之每人每小時產出，包含時段 PERIOD、產線 LINE、產出量 OUTPUT 與工時 PRESENTHOUR）"
    },
    "yrissue": {
        "category": "cim_performance",
        "description": "良率異常問題（Yield Rate Issue）追蹤記錄（包含異常編號 issueid、產線 LINE、機種 MODEL、時段 PERIOD 與不良原因說明）"
    },

    # === Group 2: CIM / SFCS 製程追蹤 (2 筆) ===
    "sfctransaction": {
        "category": "cim_sfcs",
        "description": "SFCS 產品過站交易主記錄（包含工單 MO、PCBA 序號 USN、產品料號 UPN、過站產線 LINE、製程站別 STAGE、測試結果 RESULTFLAG 0:Fail/1:Pass、過站次數 PASSCOUNT）"
    },
    "sfctransactioninfo": {
        "category": "cim_sfcs",
        "description": "SFCS 產品過站附加明細資訊（包含工單 MO、PCBA 序號 USN、過站站別 STAGE、過站時間 TRNDATE、附加資訊名稱/數值 INFONAME/INFOVALUE）"
    },

    # === Group 3: AioT / SMT Printer 錫膏印刷機 (7 筆) ===
    "aiot_smt_printer_equipment_traceability_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 設備追溯資訊（包含工單 MO、PCBA 序號 USN、鋼網編號 stencil_id、刮刀編號 squeegee_id、錫膏序號 solder_paste_sn）"
    },
    "aiot_smt_printer_machine_cycle_time_wih": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 設備週期時間（包含設備名稱 machine_name、機台週期時間 cycle_time、寫入時間 insert_date_time）"
    },
    "aiot_smt_printer_machine_error_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 設備錯誤與警報（包含異常代碼 error_code、異常持續時間 duration、錯誤發生時間 error_date_time）"
    },
    "aiot_smt_printer_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 設備三色燈狀態（包含狀態代碼 status_light: 綠燈/黃燈/紅燈、狀態變化時間 status_change_date_time）"
    },
    "aiot_smt_printer_processing_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 製程資訊（由 AIoT 程式抓取之印刷機製程參數，包含工單 MO、序號 USN、印刷參數）"
    },
    "aiot_smt_printer_real_processing_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 實際加工數據（由機台 Log 抓取，包含前後刮刀實際壓力 front/rear_pressure、印刷速度 front/rear_print_speed、溫度/濕度、脫模距離/速度 snap_off、印刷前後 X/Y/Theta 偏移量）"
    },
    "aiot_smt_printer_real_setting_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 實際配方設定值（包含前後刮刀設定壓力 front/rear_pressure_setup、設定速度 speed_setup、分離速度 snap_off_speed_setup）"
    },

    # === Group 4: AioT / SMT Mounter 貼片機 (7 筆) ===
    "aiot_smt_mounter_equipment_library_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 元件庫資料（包含元件名稱 component_name、料號 part_number、供料器與封裝庫參數）"
    },
    "aiot_smt_mounter_equipment_skip_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 關閉與跳料 (SKIP) 資訊（包含工單 MO、PCBA 序號 USN、跳過元件位置 skip_location、原因說明）"
    },
    "aiot_smt_mounter_machine_cycle_time_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 設備週期時間（包含設備名稱 machine_name、單元週期時間 cycle_time、寫入時間 insert_date_time）"
    },
    "aiot_smt_mounter_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 設備三色燈狀態（包含狀態代碼 status_light、狀態變化時間 status_change_date_time）"
    },
    "aiot_smt_mounter_processing_unitside_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 供料器實際製程資訊（由 Proviewer 每 10 分鐘拋送，包含供料器料站 slot、吸嘴 nozzle、吸料/拋料次數 pickup/throw_count）"
    },
    "aiot_smt_mounter_recipe_nozzle_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 吸嘴設定配方（包含吸嘴型號 nozzle_type、頭部編號 head_no、站別設定參數）"
    },
    "aiot_smt_mounter_recipe_stock_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 供料器料站配方設定（包含料站位置 slot_no、料號 part_no、供料器型號 feeder_type）"
    },

    # === Group 5: AioT / SMT Reflow 回焊爐 (5 筆) ===
    "aiot_smt_reflow_machine_cycle_time_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 設備週期時間（包含設備名稱 machine_name、回焊週期時間 cycle_time、寫入時間）"
    },
    "aiot_smt_reflow_machine_error_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 錯誤與警報（包含異常代碼 error_code、異常持續時間 duration、發生時間 error_date_time）"
    },
    "aiot_smt_reflow_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 設備三色燈狀態（包含狀態代碼 status_light、狀態變化時間 status_change_date_time）"
    },
    "aiot_smt_reflow_processing_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 實際製程溫控數據（包含工單 MO、序號 USN、各溫區實測溫度 zone1~zoneN_temp、冷卻區溫度、鏈速 chain_speed）"
    },
    "aiot_smt_reflow_recipe_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 配方設定資訊（包含配方程式名稱 programe_id、軌道鏈速設定值 chaine_speed_setup、各溫區目標溫度設定值）"
    },

    # === Group 6: AioT / SMT 檢測設備 (7 筆) ===
    "aiot_smt_aoi_inspection_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 自動光學檢測 (AOI) 檢驗紀錄（包含工單 MO、PCBA 序號 USN、測試結果 result Pass/Fail、缺陷代碼/名稱 defect_code/name、元件位置 component_location）"
    },
    "aiot_smt_aoi_inspection_real_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 自動光學檢測 (AOI) 檢驗實際量測值（包含工單 MO、PCBA 序號 USN、各元件實測偏移量 X/Y/Theta、高度 height、焊點品質量測數值）"
    },
    "aiot_smt_aoi_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 自動光學檢測 (AOI) 設備三色燈狀態（包含狀態代碼 status_light、狀態變化時間 status_change_date_time）"
    },
    "aiot_smt_spi_inspection_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏檢測 (SPI) 檢驗結果數據（包含工單 MO、PCBA 序號 USN、錫膏高度 height、面積 area、體積 volume、偏移量 offset、測試判定結果）"
    },
    "aiot_smt_spi_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏檢測 (SPI) 設備三色燈狀態（包含狀態代碼 status_light、狀態變化時間 status_change_date_time）"
    },
    "aiot_smt_axi_inspection_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT X光檢測 (AXI) 檢驗紀錄（包含工單 MO、PCBA 序號 USN、BGA 焊點氣孔率/空洞率 void_ratio、連錫/少錫等 X 光透視檢驗結果）"
    },
    "aiot_smt_mda_inspection_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 製造缺陷分析 (MDA) 檢驗紀錄（包含工單 MO、PCBA 序號 USN、開短路測試、電阻/電容/電感量測值及測試判定結果）"
    }
}

# ==============================================================================
# 3 大精簡 MCP Tools
# ==============================================================================

@mcp.tool()
def list_available_tables(
    keyword: Optional[str] = None,
    category: Optional[Literal["cim_performance", "cim_sfcs", "aiot_equipment"]] = None
) -> Dict[str, Any]:
    """
    [1/3 資料表地圖] 列出可供查詢的 42 張資料表名稱、類別與詳細業務功能說明。
    當不確定資料表名稱時優先呼叫。
    """
    tables = []
    for k in sorted(SCHEMAS.keys()):
        meta = TABLE_METADATA.get(k, {})
        cat = meta.get("category", "aiot_equipment")
        desc = meta.get("description", f"SMT {k} 資料表")
        
        if category and cat != category:
            continue
        if keyword and keyword.lower() not in k.lower() and keyword.lower() not in desc.lower():
            continue
            
        tables.append({"table_name": k, "category": cat, "description": desc})
        
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

    fields_info = _extract_columns(schema)
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

    # 3. 解析 SQL 中出現的 Table Name (快篩 + 邊界匹配)，動態把資料載入 SQLite in-memory
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # 找出所有 SQL 中提及的已註冊表名
    used_tables = [
        t for t in SCHEMAS.keys()
        if t in clean_sql and re.search(rf"\b{re.escape(t)}\b", clean_sql, re.IGNORECASE)
    ]
    
    for t in used_tables:
        records = _load_and_flatten_records(t)
        schema = SCHEMAS.get(t, {})
        schema_cols = [c["column_name"] for c in _extract_columns(schema) if c.get("column_name")]

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