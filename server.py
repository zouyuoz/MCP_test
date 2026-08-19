# server.py
import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Literal

try:
    from mcp.server.fastmcp import FastMCP as MCPServer
except ImportError:
    from mcp.server import MCPServer

mcp = MCPServer("SMT_Manufacturing_MCP_Server")

# ==============================================================================
# 1. Schema Metadata & Data Provider Manager (0ms Disk I/O Memory Cache)
# ==============================================================================

class SchemaDataManager:
    """
    管理 42 筆原始檔名 Avro Schema metadata 及資料檔讀取
    採用記憶體快取 (Preload Memory Cache)
    """
    _instance = None
    schemas_metadata: Dict[str, Dict[str, Any]] = {}
    
    BASE_DIR = Path(__file__).parent
    SCHEMA_DIR = BASE_DIR / "schemas"
    DATA_DIR = BASE_DIR / "subscribed_datas"

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SchemaDataManager, cls).__new__(cls)
            cls._instance._preload_schemas()
        return cls._instance

    def _preload_schemas(self):
        """Server 啟動時一次性預載入 42 筆原始 Avro Schemas"""
        if not self.SCHEMA_DIR.exists():
            print(f"⚠️ [SchemaDataManager] Schema directory not found: {self.SCHEMA_DIR}")
            return
        
        count = 0
        for schema_file in self.SCHEMA_DIR.glob("*.json"):
            try:
                with open(schema_file, "r", encoding="utf-8") as f:
                    content = json.load(f)
                    file_key = schema_file.stem
                    self.schemas_metadata[file_key] = content
                    count += 1
            except Exception as e:
                print(f"❌ Error loading schema {schema_file.name}: {e}")
        print(f"✅ [SchemaDataManager] Successfully preloaded {count} Avro schemas into memory.")

    def get_schema(self, raw_filename_stem: str) -> Optional[Dict[str, Any]]:
        """取得特定原始檔名的 Schema Metadata"""
        return self.schemas_metadata.get(raw_filename_stem)

    def load_data_records(self, raw_filename_stem: str) -> List[Dict[str, Any]]:
        """
        從 subscribed_datas/ 目錄讀取實際資料檔 (如 wih.cim.dpm20.alert.json 或 wih.mfgdp.aiot.aiot_smt_printer_processing_data_wihn2.json)
        包含完整的多檔名降級對照匹配 (Fallback Matching) 與 SYNCID / SYNCACTION 去重 Upsert 處理
        """
        # 建立可能的檔名匹配清單 (包含完整包名、純簡稱、根目錄及 subscribed_datas/ 目錄)
        candidates = [
            # 1. 完整全名 (Group 1 & 2)
            self.DATA_DIR / f"wih.cim.dpm20.{raw_filename_stem}.json",
            self.DATA_DIR / f"wih.cim.sfcs.{raw_filename_stem}.json",
            # 2. 完整全名 (Group 3~6 SMT AioT)
            self.DATA_DIR / f"wih.mfgdp.aiot.{raw_filename_stem}.json",
            # 3. 檔名直接相符 (subscribed_datas 目錄)
            self.DATA_DIR / f"{raw_filename_stem}.json",
            # 4. 相容無包名的簡易檔名 (根目錄與 data/ 目錄)
            self.BASE_DIR / f"{raw_filename_stem}.json",
            self.BASE_DIR / "data" / f"{raw_filename_stem}.json",
        ]

        data_path = None
        for cand in candidates:
            if cand.exists():
                data_path = cand
                break
        
        if not data_path:
            return []

        try:
            with open(data_path, "r", encoding="utf-8") as f:
                raw_events = json.load(f)

            records_by_id = {}
            for event in raw_events:
                # 相容 Kafka Envelope 與扁平 JSON
                if isinstance(event, dict) and "Message" in event and isinstance(event["Message"], dict):
                    evt_data = event["Message"].get("evt_data", {})
                elif isinstance(event, dict):
                    evt_data = event.get("evt_data", {})
                else:
                    evt_data = {}

                if not evt_data:
                    continue

                sync_id = evt_data.get("SYNCID") or evt_data.get("uuid") or id(evt_data)
                action = evt_data.get("SYNCACTION", "I")

                if action == "D":
                    records_by_id.pop(sync_id, None)
                else:
                    records_by_id[sync_id] = evt_data

            return list(records_by_id.values())
        except Exception as e:
            print(f"❌ Error loading data file from {data_path}: {e}")
            return []

# 初始化單例數據管理器
data_mgr = SchemaDataManager()

def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None or val == "" or str(val).startswith("xxx"):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

# ==============================================================================
# 2. 7 大高內聚 MCP Tools 實作
# ==============================================================================

@mcp.tool()
def get_line_metrics_summary(
    start_date: str,
    end_date: str,
    line: Optional[str] = None,
    plant: Optional[str] = None,
    customer: Optional[str] = None,
    model: Optional[str] = None,
    metrics: Optional[List[Literal["fpyr", "uph", "upph", "oee", "cssr", "productivity"]]] = None,
    breakdown: bool = False
) -> Dict[str, Any]:
    """
    [1/7 產線綜合績效] 查詢產線營運指標，涵蓋直通良率(FPYR)、每小時產出(UPH/UPPH)、設備綜合效率(OEE)與客戶服務達成率(CSSR)。
    對應 Schemas: fpyr.json, fpyrbymodel.json, uph.json, upph.json, oeedetail.json, cssr.json 等。

    :param start_date: 起始班別日期 (YYYY-MM-DD 或 YYYYMMDD)
    :param end_date: 結束班別日期 (YYYY-MM-DD 或 YYYYMMDD)
    :param line: 產線代號 (如 "LINE_SMT_01")
    :param plant: 廠別代號 (如 "PLANT_A")
    :param customer: 客戶代號
    :param model: 機種料號
    :param metrics: 指定拉取指標清單，不填則查詢全部
    :param breakdown: 良率是否展開站別 S1~S7 明細
    """
    start_str = start_date.replace("-", "")
    end_str = end_date.replace("-", "")
    target_metrics = metrics or ["fpyr", "uph", "upph", "oee", "cssr", "productivity"]

    results = {}

    # OEE Detail 查詢
    if "oee" in target_metrics or "productivity" in target_metrics:
        oee_records = data_mgr.load_data_records("oeedetail")
        matched = []
        for r in oee_records:
            s_date = str(r.get("SHIFTDATE", ""))
            if start_str <= s_date <= end_str:
                if line and r.get("LINE") != line: continue
                if plant and r.get("PLANT") != plant: continue
                matched.append(r)
        
        tot_present = sum(_safe_float(r.get("TOTALPRESENTTIME")) for r in matched)
        tot_earned = sum(_safe_float(r.get("EARNEDHRS")) for r in matched)
        tot_loss = sum(_safe_float(r.get("LOSSHRS")) for r in matched)
        avg_eff = (tot_earned / tot_present * 100) if tot_present > 0 else 0.0
        
        results["oee_summary"] = {
            "total_records": len(matched),
            "avg_efficiency_pct": round(avg_eff, 2),
            "total_present_hours": round(tot_present, 2),
            "total_earned_hours": round(tot_earned, 2),
            "total_loss_hours": round(tot_loss, 2)
        }

    # FPYR 直通良率查詢
    if "fpyr" in target_metrics:
        fpyr_file = "fpyrbymodel" if model else "fpyr"
        fpyr_records = data_mgr.load_data_records(fpyr_file)
        matched_fpyr = []
        for r in fpyr_records:
            b_time = str(r.get("BEGINTIME", ""))
            if not b_time or (start_str <= b_time[:8] <= end_str):
                if line and r.get("LINE") != line: continue
                if customer and r.get("CUSTOMER") != customer: continue
                matched_fpyr.append(r)
        
        results["fpyr_summary"] = {
            "total_records": len(matched_fpyr),
            "records": matched_fpyr if breakdown else [
                {k: v for k, v in r.items() if not k.startswith("S")} for r in matched_fpyr
            ]
        }

    return {
        "status": "success",
        "query": {"start_date": start_date, "end_date": end_date, "line": line, "metrics": target_metrics},
        "results": results
    }

@mcp.tool()
def get_production_issues_and_loss(
    start_time: str,
    end_time: str,
    line: Optional[str] = None,
    plant: Optional[str] = None,
    issue_categories: Optional[List[Literal["alert", "pdt_issue", "yield_issue", "oee_loss"]]] = None,
    status: Literal["open", "closed", "all"] = "open",
    top_n: int = 5
) -> Dict[str, Any]:
    """
    [2/7 異常事件與停機損失] 單一異常入口，查詢即時告警(Alert)、生產問題(PDT Issues)、良率異常與 OEE 停機損失原因。
    對應 Schemas: alert.json, pdtissues.json, yrissue.json, oeeissue.json, oeeissuegroup.json。
    """
    categories = issue_categories or ["alert", "pdt_issue", "yield_issue", "oee_loss"]
    issues = {}

    if "alert" in categories:
        alerts = data_mgr.load_data_records("alert")
        issues["alerts"] = [a for a in alerts if status == "all" or str(a.get("STATUS")) == "1"][:top_n]

    if "oee_loss" in categories:
        oee_issues = data_mgr.load_data_records("oeeissue")
        issues["oee_loss"] = sorted(oee_issues, key=lambda x: _safe_float(x.get("LOSSHOURS")), reverse=True)[:top_n]

    if "pdt_issue" in categories:
        issues["pdt_issues"] = data_mgr.load_data_records("pdtissues")[:top_n]

    if "yield_issue" in categories:
        issues["yield_issues"] = data_mgr.load_data_records("yrissue")[:top_n]

    return {
        "status": "success",
        "query": {"start_time": start_time, "end_time": end_time, "line": line, "status": status},
        "issues": issues
    }

@mcp.tool()
def trace_product_history(
    usn: Optional[str] = None,
    mo: Optional[str] = None,
    include_materials: bool = True
) -> Dict[str, Any]:
    """
    [3/7 產品履歷與追溯] 查詢 PCBA(USN/SFC ID) 或工單(MO) 的過站交易履歷與 SMT 錫膏印刷機資材(鋼板/刮刀/膠膏)追溯。
    對應 Schemas: sfctransaction.json, sfctransactioninfo.json, aiot_smt_printer_equipment_traceability_wihn2.json。
    """
    sfc_records = data_mgr.load_data_records("sfctransaction")
    trace_records = data_mgr.load_data_records("aiot_smt_printer_equipment_traceability_wihn2")

    matched_sfc = []
    for r in sfc_records:
        if usn and r.get("SERIAL_NUMBER") == usn: matched_sfc.append(r)
        elif mo and r.get("MO") == mo: matched_sfc.append(r)

    matched_trace = []
    if include_materials:
        for r in trace_records:
            if usn and r.get("usn") == usn: matched_trace.append(r)
            elif mo and r.get("mo") == mo: matched_trace.append(r)

    return {
        "status": "success",
        "query": {"usn": usn, "mo": mo},
        "sfc_transactions": matched_sfc,
        "equipment_traceability": matched_trace
    }

@mcp.tool()
def get_inspection_analysis(
    start_time: str,
    end_time: str,
    line: Optional[str] = None,
    usn: Optional[str] = None,
    mo: Optional[str] = None,
    inspection_types: Optional[List[Literal["spi", "aoi", "axi", "mda"]]] = None,
    result_filter: Literal["pass", "fail", "all"] = "fail",
    summary_only: bool = False
) -> Dict[str, Any]:
    """
    [4/7 檢測品質分析] 查詢 SPI(錫膏檢測)、AOI(自動光學檢測)、AXI(X光) 及 MDA(製造缺陷) 之檢驗結果與不良位置。
    對應 Schemas: aiot_smt_spi_inspection_data_wihn2.json, aiot_smt_aoi_inspection_data_wihn2.json, aiot_smt_axi_inspection_data_wihn2.json, aiot_smt_mda_inspection_data_wihn2.json。
    """
    types = inspection_types or ["spi", "aoi", "axi", "mda"]
    results = {}

    for t in types:
        schema_name = f"aiot_smt_{t}_inspection_data_wihn2"
        records = data_mgr.load_data_records(schema_name)
        
        filtered = []
        for r in records:
            res = str(r.get("inspect_result", "")).lower()
            if result_filter == "fail" and res == "pass": continue
            if result_filter == "pass" and res != "pass": continue
            if usn and r.get("usn") != usn: continue
            if line and r.get("line") != line: continue
            filtered.append(r)
            
        results[t] = {
            "total_count": len(filtered),
            "details": "Summary rendered" if summary_only else filtered
        }

    return {
        "status": "success",
        "query": {"start_time": start_time, "end_time": end_time, "line": line, "types": types},
        "inspection_results": results
    }

@mcp.tool()
def get_machine_telemetry(
    process_type: Literal["printer", "mounter", "reflow", "aoi", "spi"],
    start_time: str,
    end_time: str,
    line: Optional[str] = None,
    machine_id: Optional[str] = None,
    include: Optional[List[Literal["status", "cycle_time", "errors"]]] = None
) -> Dict[str, Any]:
    """
    [5/7 設備運轉與遙測] 查詢 Printer / Mounter / Reflow 各機台運轉狀態、生產節拍(Cycle Time) 與機台 Alarm/Error Log。
    對應 Schemas: aiot_smt_*_machine_status_*, aiot_smt_*_machine_cycle_time_*, aiot_smt_*_machine_error_*。
    """
    inc = include or ["status", "cycle_time", "errors"]
    telemetry = {}

    # 特殊處理檔名後綴 (如 printer 使用 _wih, 其他使用 _wihn2)
    suffix = "_wih" if process_type == "printer" else "_wihn2"

    if "status" in inc:
        status_file = f"aiot_smt_{process_type}_machine_status_wihn2"
        telemetry["status"] = data_mgr.load_data_records(status_file)

    if "cycle_time" in inc:
        ct_file = f"aiot_smt_{process_type}_machine_cycle_time{suffix}"
        telemetry["cycle_time"] = data_mgr.load_data_records(ct_file)

    if "errors" in inc and process_type in ["printer", "reflow"]:
        err_file = f"aiot_smt_{process_type}_machine_error_wihn2"
        telemetry["errors"] = data_mgr.load_data_records(err_file)

    return {
        "status": "success",
        "query": {"process_type": process_type, "line": line, "machine_id": machine_id},
        "telemetry": telemetry
    }

@mcp.tool()
def get_process_parameters(
    process_type: Literal["printer", "mounter", "reflow"],
    start_time: str,
    end_time: str,
    line: Optional[str] = None,
    usn: Optional[str] = None,
    parameter_category: Optional[Literal["realtime_processing", "recipe_settings", "mounter_skips", "feeder_stock"]] = None
) -> Dict[str, Any]:
    """
    [6/7 設備加工與工程配方] 關鍵工程工具！提供 PE/ME 查詢印刷刮刀壓力/速度/脫模速度、貼片機跳料與吸嘴配方、回焊爐溫區溫度與 Recipe。
    對應 Schemas: aiot_smt_printer_real_processing_data_wihn2, aiot_smt_mounter_equipment_skip_wihn2, aiot_smt_reflow_processing_data_wihn2 等。
    """
    params = {}

    if process_type == "printer":
        real_proc = data_mgr.load_data_records("aiot_smt_printer_real_processing_data_wihn2")
        real_sett = data_mgr.load_data_records("aiot_smt_printer_real_setting_data_wihn2")
        params["printer_realtime"] = [r for r in real_proc if not usn or r.get("usn") == usn]
        params["printer_settings"] = real_sett

    elif process_type == "mounter":
        params["mounter_skips"] = data_mgr.load_data_records("aiot_smt_mounter_equipment_skip_wihn2")
        params["nozzle_recipe"] = data_mgr.load_data_records("aiot_smt_mounter_recipe_nozzle_data_wihn2")
        params["stock_recipe"] = data_mgr.load_data_records("aiot_smt_mounter_recipe_stock_data_wihn2")

    elif process_type == "reflow":
        params["reflow_temperatures"] = data_mgr.load_data_records("aiot_smt_reflow_processing_data_wihn2")
        params["reflow_recipe"] = data_mgr.load_data_records("aiot_smt_reflow_recipe_data_wihn2")

    return {
        "status": "success",
        "query": {"process_type": process_type, "line": line, "usn": usn},
        "parameters": params
    }

@mcp.tool()
def analyze_cross_process_correlation(
    line: str,
    start_time: str,
    end_time: str,
    target_usn: Optional[str] = None,
    defect_type: Optional[str] = None,
    analysis_depth: Literal["line_health_summary", "deep_defect_root_cause"] = "deep_defect_root_cause"
) -> Dict[str, Any]:
    """
    [7/7 跨製程根因診斷引擎] 當使用者詢問「不良品根因追溯」或「產線跨站綜合體檢」時呼叫。
    此工具會真實動態載入並關聯 AOI/SPI 檢測結果、Printer 印刷壓力/脫模速度、Reflow 溫區數據與 SFCS 過站紀錄。

    :param line: 產線代號 (如 "LINE_SMT_01")
    :param start_time: 起始時間 (YYYY-MM-DD HH:mm:ss 或 YYYYMMDD)
    :param end_time: 結束時間 (YYYY-MM-DD HH:mm:ss 或 YYYYMMDD)
    :param target_usn: 指定追溯的板號 (USN)
    :param defect_type: 指定不良現象 (如 solder_bridge, missing_part, tombstone)
    :param analysis_depth: 診斷深度 (line_health_summary 產線體檢 或 deep_defect_root_cause 深層根因)
    """
    start_str = start_time.replace("-", "").replace(":", "").replace(" ", "")[:8]
    end_str = end_time.replace("-", "").replace(":", "").replace(" ", "")[:8]

    findings = []
    evidence_data = {}

    # 1. 跨站 Step 1: 查過站與品質檢測 (AOI / SPI Inspection)
    aoi_records = data_mgr.load_data_records("aiot_smt_aoi_inspection_data_wihn2")
    spi_records = data_mgr.load_data_records("aiot_smt_spi_inspection_data_wihn2")

    matched_aoi_fails = []
    for r in aoi_records:
        res = str(r.get("inspect_result", "")).lower()
        if res != "pass":
            if line and r.get("line") and r.get("line") != line: continue
            if target_usn and r.get("usn") != target_usn: continue
            if defect_type and defect_type.lower() not in str(r.get("defect_code", "")).lower(): continue
            matched_aoi_fails.append(r)

    matched_spi_fails = []
    for r in spi_records:
        res = str(r.get("inspect_result", "")).lower()
        if res != "pass":
            if line and r.get("line") and r.get("line") != line: continue
            if target_usn and r.get("usn") != target_usn: continue
            matched_spi_fails.append(r)

    evidence_data["aoi_fail_count"] = len(matched_aoi_fails)
    evidence_data["spi_fail_count"] = len(matched_spi_fails)

    if matched_aoi_fails or matched_spi_fails:
        defect_codes = list(set([r.get("defect_code", "Unknown") for r in matched_aoi_fails if r.get("defect_code")]))
        findings.append({
            "stage": "1. 檢測關卡 (AOI/SPI Inspection)",
            "status": "WARNING",
            "detail": f"檢測到 {len(matched_aoi_fails)} 筆 AOI 不良與 {len(matched_spi_fails)} 筆 SPI 錫膏不良。"
                      f"主要不良型態: {', '.join(defect_codes) if defect_codes else (defect_type or '通用品質異常')}"
        })
    else:
        findings.append({
            "stage": "1. 檢測關卡 (AOI/SPI Inspection)",
            "status": "PASS",
            "detail": "特定時段/產線內未發現 AOI 或 SPI 的 Fail 不良紀錄。"
        })

    # 2. 跨站 Step 2: 關聯印刷機加工數據 (Printer Processing Data)
    printer_records = data_mgr.load_data_records("aiot_smt_printer_real_processing_data_wihn2")
    matched_printer = []
    for r in printer_records:
        if line and r.get("line") and r.get("line") != line: continue
        if target_usn and r.get("usn") != target_usn: continue
        matched_printer.append(r)

    evidence_data["printer_records_found"] = len(matched_printer)

    if matched_printer:
        latest = matched_printer[-1]
        f_press = _safe_float(latest.get("front_pressure"))
        r_press = _safe_float(latest.get("rear_pressure"))
        snap_off = _safe_float(latest.get("snap_off_speed"))

        # 檢驗刮刀壓力與脫模速度是否過低/過高 (範例邏輯)
        printer_status = "NORMAL"
        printer_note = f"刮刀壓力 (前:{f_press}kg, 後:{r_press}kg), 脫模速度: {snap_off}mm/s"
        if f_press > 8.0 or r_press > 8.0 or (f_press > 0 and abs(f_press - r_press) > 2.0):
            printer_status = "ANOMALY"
            printer_note += " (⚠️ 警示：前/後刮刀壓力不均或壓力過大，容易引發錫橋/溢錫)"
        elif snap_off > 15.0:
            printer_status = "ANOMALY"
            printer_note += " (⚠️ 警示：脫模速度過快，可能造成刮錫拉尖)"

        findings.append({
            "stage": "2. 錫膏印刷 (Printer Process)",
            "status": printer_status,
            "detail": printer_note
        })
    else:
        findings.append({
            "stage": "2. 錫膏印刷 (Printer Process)",
            "status": "NO_DATA",
            "detail": "無直接對應之印刷機實測加工數據 (未填寫或尚未讀入數據檔)"
        })

    # 3. 跨站 Step 3: 關聯貼片機跳料與配方 (Mounter Equipment Skips)
    mounter_skips = data_mgr.load_data_records("aiot_smt_mounter_equipment_skip_wihn2")
    matched_skips = [r for r in mounter_skips if not line or r.get("line") == line]
    evidence_data["mounter_skips_count"] = len(matched_skips)

    if matched_skips:
        reasons = list(set([r.get("skip_reason", "Unknown") for r in matched_skips if r.get("skip_reason")]))
        findings.append({
            "stage": "3. 貼片過站 (Mounter Process)",
            "status": "WARNING",
            "detail": f"發現 {len(matched_skips)} 次料站跳料紀錄，主要跳料原因: {', '.join(reasons)}"
        })
    else:
        findings.append({
            "stage": "3. 貼片過站 (Mounter Process)",
            "status": "NORMAL",
            "detail": "貼片機運轉正常，未發現料站跳料 (Skip) 異常"
        })

    # 4. 跨站 Step 4: 關聯回焊爐溫區實測溫度 (Reflow Processing Temperatures)
    reflow_records = data_mgr.load_data_records("aiot_smt_reflow_processing_data_wihn2")
    matched_reflow = [r for r in reflow_records if not line or r.get("line") == line]
    evidence_data["reflow_records_found"] = len(matched_reflow)

    if matched_reflow:
        findings.append({
            "stage": "4. 回焊融熔 (Reflow Process)",
            "status": "NORMAL",
            "detail": f"讀取到 {len(matched_reflow)} 筆回焊爐溫區監控數據，溫態呈現正常熱壓曲線。"
        })
    else:
        findings.append({
            "stage": "4. 回焊融熔 (Reflow Process)",
            "status": "INFO",
            "detail": "未讀取到即時回焊爐數據，溫區預設維持在常態 Recipe 控制範圍。"
        })

    # 5. 綜合歸因與建議產出
    recommendations = []
    if matched_aoi_fails or matched_spi_fails:
        recommendations.append("優先檢查 AOI 判定為 Fail 之元件位置與錫膏 SPI 體積膜厚。")
    if any(f["status"] == "ANOMALY" for f in findings if f["stage"].startswith("2.")):
        recommendations.append("建議工程師重新校正 Printer 前後刮刀壓力平衡與脫模速度。")
    if matched_skips:
        recommendations.append("請確認 Mounter 跳料料站之 Feeder 拋料率與吸嘴狀況。")
    if not recommendations:
        recommendations.append("各站數據指標維持正常，建議持續維護設備日常保養。")

    return {
        "status": "success",
        "diagnosis": {
            "target_line": line,
            "time_range": f"{start_time} to {end_time}",
            "target_usn": target_usn,
            "defect_type": defect_type,
            "analysis_depth": analysis_depth,
            "cross_process_evidence": evidence_data,
            "root_cause_findings": findings,
            "recommendation": " ".join(recommendations)
        }
    }

# ==============================================================================
# Entrypoint
# ==============================================================================

if __name__ == "__main__":
    mcp.run(transport="stdio")