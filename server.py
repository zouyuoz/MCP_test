# server.py
import json
import os
from typing import Literal, Optional
try:
    from mcp.server.fastmcp import FastMCP as MCPServer
except ImportError:
    from mcp.server import MCPServer

mcp = MCPServer("ManufacturingOEEServer")

# 資料檔案路徑
DATA_FILE = os.path.join(os.path.dirname(__file__), "oeedetail.json")

def _safe_float(val: Optional[str], default: float = 0.0) -> float:
    """防呆：將 string 或 null 安全轉為 float"""
    if val is None or val == "xxx...":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def _load_and_deduplicate_records() -> list[dict]:
    """讀取 oeedetail.json 並處理 Upsert / 去重邏輯 (支援 Kafka Envelope 與純 Message 結構)"""
    if not os.path.exists(DATA_FILE):
        return []
    
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        raw_events = json.load(f)
    
    # 按照 SYNCID 處理 Upsert (I/U) 與 Delete (D)
    records_by_id = {}
    for event in raw_events:
        # 相容 Kafka Envelope (包含 Timestamp, Topic, Message) 與扁平結構
        if "Message" in event and isinstance(event["Message"], dict):
            evt_data = event["Message"].get("evt_data", {})
        else:
            evt_data = event.get("evt_data", {})

        if not evt_data:
            continue

        sync_id = evt_data.get("SYNCID")
        action = evt_data.get("SYNCACTION", "I")
        
        if action == "D":
            records_by_id.pop(sync_id, None)
        else:
            records_by_id[sync_id] = evt_data
            
    return list(records_by_id.values())

@mcp.tool()
def get_oee_detail(
    start_date: str,
    end_date: str,
    line: Optional[str] = None,
    plant: Optional[str] = None,
    shift_type: Optional[Literal["Day shift", "Night shift"]] = None
) -> dict:
    """
    查詢產線的 OEE 效率與生產力數據，包含出勤工時、實際產出工時、損失工時與效率百分比。
    以班別日期 (SHIFTDATE) 為主要查詢維度。

    :param start_date: 班別起始日期，格式 YYYY-MM-DD (例如 "2026-08-15")
    :param end_date: 班別結束日期，格式 YYYY-MM-DD (例如 "2026-08-17")
    :param line: 產線代號 (可選，例如 "LINE_SMT_01")
    :param plant: 廠別代號 (可選，例如 "PLANT_A")
    :param shift_type: 班別類型，可選 "Day shift" 或 "Night shift"
    :return: 包含查詢摘要 (summary) 與明細 (records) 的 JSON 結構
    """
    start_shiftdate = start_date.replace("-", "")
    end_shiftdate = end_date.replace("-", "")
    
    all_records = _load_and_deduplicate_records()
    matched_records = []
    
    for r in all_records:
        shift_date = r.get("SHIFTDATE", "")
        # 日期範圍過濾 (YYYYMMDD)
        if not (start_shiftdate <= shift_date <= end_shiftdate):
            continue
        # 產線過濾
        if line and r.get("LINE") != line:
            continue
        # 廠別過濾
        if plant and r.get("PLANT") != plant:
            continue
        # 班別過濾
        if shift_type and r.get("SHIFTTYPE") != shift_type:
            continue
            
        matched_records.append(r)
        
    total_present = sum(_safe_float(r.get("TOTALPRESENTTIME")) for r in matched_records)
    total_earned = sum(_safe_float(r.get("EARNEDHRS")) for r in matched_records)
    total_loss = sum(_safe_float(r.get("LOSSHRS")) for r in matched_records)
    
    avg_efficiency = (total_earned / total_present * 100) if total_present > 0 else 0.0
    loss_rate = (total_loss / total_present * 100) if total_present > 0 else 0.0
    
    return {
        "status": "success",
        "query": {
            "start_date": start_date,
            "end_date": end_date,
            "line": line,
            "plant": plant,
            "shift_type": shift_type
        },
        "summary": {
            "total_records": len(matched_records),
            "avg_efficiency_pct": round(avg_efficiency, 2),
            "total_present_hours": round(total_present, 2),
            "total_earned_hours": round(total_earned, 2),
            "total_loss_hours": round(total_loss, 2),
            "loss_rate_pct": round(loss_rate, 2)
        },
        "records": matched_records
    }

@mcp.tool()
def get_low_efficiency_lines(
    target_date: str,
    threshold_pct: float = 80.0,
    plant: Optional[str] = None
) -> dict:
    """
    找出特定日期中，效率低於預設門檻的產線清單，便於 Agent 進行異常診斷與根因分析。

    :param target_date: 查詢日期，格式 YYYY-MM-DD (例如 "2026-08-17")
    :param threshold_pct: 效率警示門檻百分比，預設為 80.0
    :param plant: 廠別代號 (可選，例如 "PLANT_B")
    :return: 效率低於門檻的異常產線清單
    """
    target_shiftdate = target_date.replace("-", "")
    all_records = _load_and_deduplicate_records()
    
    anomalies = []
    for r in all_records:
        if r.get("SHIFTDATE") != target_shiftdate:
            continue
        if plant and r.get("PLANT") != plant:
            continue
            
        eff = _safe_float(r.get("EFFICIENCY"))
        if eff < threshold_pct:
            anomalies.append({
                "shift_date": r.get("SHIFTDATE"),
                "plant": r.get("PLANT"),
                "line": r.get("LINE"),
                "shift_type": r.get("SHIFTTYPE"),
                "efficiency_pct": eff,
                "loss_hours": _safe_float(r.get("LOSSHRS")),
                "total_present_time": _safe_float(r.get("TOTALPRESENTTIME")),
                "alert": f"效率 {eff}% 低於警戒門檻 {threshold_pct}%"
            })
            
    return {
        "status": "success",
        "target_date": target_date,
        "threshold_pct": threshold_pct,
        "anomalies_count": len(anomalies),
        "anomalies": anomalies
    }

if __name__ == "__main__":
    mcp.run(transport="stdio")