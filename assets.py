from typing import Dict

TABLE_METADATA: Dict[str, Dict[str, str]] = {
    "aiot_smt_aoi_inspection_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 自動光學檢測 (AOI) 檢驗紀錄（包含工單 MO、PCBA 序號 USN、測試結果 result "
        "Pass/Fail、缺陷代碼/名稱 defect_code/name、元件位置 component_location）",
    },
    "aiot_smt_aoi_inspection_real_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 自動光學檢測 (AOI) 檢驗實際量測值（包含工單 MO、PCBA 序號 USN、各元件實測偏移量 "
        "X/Y/Theta、高度 height、焊點品質量測數值）",
    },
    "aiot_smt_aoi_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 自動光學檢測 (AOI) 設備三色燈狀態（包含狀態代碼 status_light、狀態變化時間 "
        "status_change_date_time）",
    },
    "aiot_smt_axi_inspection_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT X光檢測 (AXI) 檢驗紀錄（包含工單 MO、PCBA 序號 USN、BGA 焊點氣孔率/空洞率 "
        "void_ratio、連錫/少錫等 X 光透視檢驗結果）",
    },
    "aiot_smt_mda_inspection_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 製造缺陷分析 (MDA) 檢驗紀錄（包含工單 MO、PCBA 序號 "
        "USN、開短路測試、電阻/電容/電感量測值及測試判定結果）",
    },
    "aiot_smt_mounter_equipment_library_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 元件庫資料（包含元件名稱 component_name、料號 "
        "part_number、供料器與封裝庫參數）",
    },
    "aiot_smt_mounter_equipment_skip_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 關閉與跳料 (SKIP) 資訊（包含工單 MO、PCBA 序號 USN、跳過元件位置 "
        "skip_location、原因說明）",
    },
    "aiot_smt_mounter_machine_cycle_time_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 設備週期時間（包含設備名稱 machine_name、單元週期時間 "
        "cycle_time、寫入時間 insert_date_time）",
    },
    "aiot_smt_mounter_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 設備三色燈狀態（包含狀態代碼 status_light、狀態變化時間 "
        "status_change_date_time）",
    },
    "aiot_smt_mounter_processing_unitside_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 供料器實際製程資訊（由 Proviewer 每 10 "
        "分鐘拋送，包含供料器料站 slot、吸嘴 nozzle、吸料/拋料次數 "
        "pickup/throw_count）",
    },
    "aiot_smt_mounter_recipe_nozzle_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 吸嘴設定配方（包含吸嘴型號 nozzle_type、頭部編號 "
        "head_no、站別設定參數）",
    },
    "aiot_smt_mounter_recipe_stock_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 貼片機 (Mounter) 供料器料站配方設定（包含料站位置 slot_no、料號 "
        "part_no、供料器型號 feeder_type）",
    },
    "aiot_smt_printer_equipment_traceability_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 設備追溯資訊（包含工單 MO、PCBA 序號 USN、鋼網編號 "
        "stencil_id、刮刀編號 squeegee_id、錫膏序號 solder_paste_sn）",
    },
    "aiot_smt_printer_machine_cycle_time_wih": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 設備週期時間（包含設備名稱 machine_name、機台週期時間 "
        "cycle_time、寫入時間 insert_date_time）",
    },
    "aiot_smt_printer_machine_error_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 設備錯誤與警報（包含異常代碼 error_code、異常持續時間 "
        "duration、錯誤發生時間 error_date_time）",
    },
    "aiot_smt_printer_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 設備三色燈狀態（包含狀態代碼 status_light: "
        "綠燈/黃燈/紅燈、狀態變化時間 status_change_date_time）",
    },
    "aiot_smt_printer_processing_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 製程資訊（由 AIoT 程式抓取之印刷機製程參數，包含工單 MO、序號 "
        "USN、印刷參數）",
    },
    "aiot_smt_printer_real_processing_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 實際加工數據（由機台 Log 抓取，包含前後刮刀實際壓力 "
        "front/rear_pressure、印刷速度 "
        "front/rear_print_speed、溫度/濕度、脫模距離/速度 snap_off、印刷前後 "
        "X/Y/Theta 偏移量）",
    },
    "aiot_smt_printer_real_setting_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏印刷機 (Printer) 實際配方設定值（包含前後刮刀設定壓力 "
        "front/rear_pressure_setup、設定速度 speed_setup、分離速度 "
        "snap_off_speed_setup）",
    },
    "aiot_smt_reflow_machine_cycle_time_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 設備週期時間（包含設備名稱 machine_name、回焊週期時間 "
        "cycle_time、寫入時間）",
    },
    "aiot_smt_reflow_machine_error_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 錯誤與警報（包含異常代碼 error_code、異常持續時間 duration、發生時間 "
        "error_date_time）",
    },
    "aiot_smt_reflow_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 設備三色燈狀態（包含狀態代碼 status_light、狀態變化時間 "
        "status_change_date_time）",
    },
    "aiot_smt_reflow_processing_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 實際製程溫控數據（包含工單 MO、序號 USN、各溫區實測溫度 "
        "zone1~zoneN_temp、冷卻區溫度、鏈速 chain_speed）",
    },
    "aiot_smt_reflow_recipe_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 回焊爐 (Reflow) 配方設定資訊（包含配方程式名稱 programe_id、軌道鏈速設定值 "
        "chaine_speed_setup、各溫區目標溫度設定值）",
    },
    "aiot_smt_spi_inspection_data_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏檢測 (SPI) 檢驗結果數據（包含工單 MO、PCBA 序號 USN、錫膏高度 height、面積 "
        "area、體積 volume、偏移量 offset、測試判定結果）",
    },
    "aiot_smt_spi_machine_status_wihn2": {
        "category": "aiot_equipment",
        "description": "OT 錫膏檢測 (SPI) 設備三色燈狀態（包含狀態代碼 status_light、狀態變化時間 "
        "status_change_date_time）",
    },
    "alert": {
        "category": "cim_performance",
        "description": "即時警報與異常通知記錄（包含 ALERTID、主旨 SUBJECT、警報內容 CONTENT、發送來源與接收對象 SENDFROM/SENDTO）",
    },
    "cssr": {
        "category": "cim_performance",
        "description": "客戶服務達成率與產線人力綁定統計（包含廠別 PLANT、產線 LINE、客戶 CUSTOMER、時段 PERIOD、綁定人數 BOUNDEMPNUM 與總人數 "
        "BOUNDEMPTOTAL）",
    },
    "fpyr": {
        "category": "cim_performance",
        "description": "產線直通良率（First Pass Yield Rate, FPYR）總體統計（包含產線 LINE、時段 PERIOD、起訖時間 "
        "BEGINTIME/ENDTIME、投入數、產出數與良率）",
    },
    "fpyrbymodel": {
        "category": "cim_performance",
        "description": "依產品機種（MODEL）分類之直通良率 FPYR 統計（包含客戶 CUSTOMER、機種 MODEL、各製程站別投入/產出數與良率 FPYR）",
    },
    "oeedetail": {
        "category": "cim_performance",
        "description": "產線 OEE 設備總體效率詳細數據（包含班別 SHIFT/SHIFTTYPE、產線 LINE、總在籍工時 TOTALPRESENTTIME、賺得工時 "
        "EARNEDHRS、稼動率與效率指標）",
    },
    "oeedetail_chargehours": {
        "category": "cim_performance",
        "description": "OEE 計費工時與停機損失統計（包含產線 LINE、機種 MODEL、料號 UPN、停機時數 STOPHOUR、閒置工時 IDLEHANDS "
        "與稼動分析）",
    },
    "oeeissue": {
        "category": "cim_performance",
        "description": "OEE 停機異常事件與原因記錄（包含產線 LINE、機種 MODEL、料號 UPN、異常原因代碼與說明 CODE/CODEDESC、停機時數 STOPHOURS）",
    },
    "oeeissuegroup": {
        "category": "cim_performance",
        "description": "OEE 異常分群與彙總統計（包含時段 PERIOD、產線 LINE、客戶 CUSTOMER、停機時數 STOPHOUR 與總停機時數 TOTALSTOPHOUR）",
    },
    "pdtissues": {
        "category": "cim_performance",
        "description": "生產問題與工時損失清單（包含問題編號 ISSUEID、機種 MODEL、班別 SHIFTID、工時損失 LOSSHOUR、原因代碼 CODE 與說明 "
        "CODEDESCRIPTION）",
    },
    "productitvity": {
        "category": "cim_performance",
        "description": "生產力（Productivity）綜合指標統計（包含時段 PERIOD、產線 LINE、客戶 CUSTOMER、生產力數值 PRODUCTIVITY 與賺得工時 "
        "EARNHOUR）",
    },
    "sfctransaction": {
        "category": "cim_sfcs",
        "description": "SFCS 產品過站交易主記錄（包含工單 MO、PCBA 序號 USN、產品料號 UPN、過站產線 LINE、製程站別 STAGE、測試結果 RESULTFLAG "
        "0:Fail/1:Pass、過站次數 PASSCOUNT）",
    },
    "sfctransactioninfo": {
        "category": "cim_sfcs",
        "description": "SFCS 產品過站附加明細資訊（包含工單 MO、PCBA 序號 USN、過站站別 STAGE、過站時間 TRNDATE、附加資訊名稱/數值 "
        "INFONAME/INFOVALUE）",
    },
    "uph": {
        "category": "cim_performance",
        "description": "每小時產出數（Units Per Hour, UPH）統計（包含時段 PERIOD、產線 LINE、製程站別 PROCESS、班別 SHIFT、目標與實際 UPH）",
    },
    "upph": {
        "category": "cim_performance",
        "description": "每人每小時產出數（Units Per Person Hour, UPPH）統計（包含時段 PERIOD、產線 LINE、產出量 OUTPUT、在籍工時 PRESENTHOUR 與 "
        "UPPH）",
    },
    "upphn": {
        "category": "cim_performance",
        "description": "UPPH 變形指標統計（含夜班/特殊班別調整之每人每小時產出，包含時段 PERIOD、產線 LINE、產出量 OUTPUT 與工時 PRESENTHOUR）",
    },
    "yrissue": {
        "category": "cim_performance",
        "description": "良率異常問題（Yield Rate Issue）追蹤記錄（包含異常編號 issueid、產線 LINE、機種 MODEL、時段 PERIOD 與不良原因說明）",
    },
}
