# run_deepagent.py
import asyncio
import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from deepagents import create_deep_agent
from langchain_core.messages import messages_to_dict
from langchain_mcp_adapters.client import MultiServerMCPClient

load_dotenv()

def get_model():
    """依據環境變數動態解析並回傳對應的 LLM 實例 (支援 Azure, Gemini, Anthropic, OpenAI)"""
    if os.getenv("AZURE_OPENAI_API_KEY") and os.getenv("AZURE_OPENAI_ENDPOINT"):
        from langchain_openai import AzureChatOpenAI
        return AzureChatOpenAI(
            azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-05-01-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        )
    if api_key := (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")):
        from langchain_google_genai import ChatGoogleGenerativeAI
        model_name = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")
        return ChatGoogleGenerativeAI(model=model_name, google_api_key=api_key)
    if os.getenv("ANTHROPIC_API_KEY"):
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"))
    if os.getenv("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
    raise ValueError("未檢測到支援的 API Key，請在 .env 設定 AZURE_OPENAI_API_KEY、GEMINI_API_KEY、ANTHROPIC_API_KEY 或 OPENAI_API_KEY。")

def extract_text(content) -> str:
    """跨模型提取文字內容 (相容 str、list of content blocks 及自訂物件)"""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and "text" in block:
                parts.append(block["text"])
            elif isinstance(block, str):
                parts.append(block)
            elif hasattr(block, "text"):
                parts.append(getattr(block, "text"))
        return "\n".join(parts)
    return str(content) if content is not None else ""

def extract_tool_calls(msg) -> list[dict]:
    """跨模型提取 Tool Calls (相容標準 tool_calls 與 additional_kwargs)"""
    calls = []
    if hasattr(msg, "tool_calls") and msg.tool_calls:
        for tc in msg.tool_calls:
            calls.append({"name": tc["name"], "args": tc["args"]})
    elif hasattr(msg, "additional_kwargs"):
        for rc in msg.additional_kwargs.get("tool_calls", []):
            if "function" in rc:
                fn = rc["function"]
                args = json.loads(fn["arguments"]) if isinstance(fn.get("arguments"), str) else fn.get("arguments", {})
                calls.append({"name": fn["name"], "args": args})
    return calls

def _to_serializable(obj):
    """遞迴將所有複雜型別 (Pydantic models, Custom objects, sets, tuples) 轉為 JSON 安全資料型態"""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_serializable(item) for item in obj]
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        return _to_serializable(obj.model_dump())
    if hasattr(obj, "dict") and callable(obj.dict):
        return _to_serializable(obj.dict())
    if hasattr(obj, "to_json") and callable(obj.to_json):
        return _to_serializable(obj.to_json())
    return str(obj)

def save_agent_result(result: dict, prompt: str, filepath: str = None) -> str:
    """
    完整儲存 Agent 執行的全部生命週期（問題 + 思考/工具調用軌跡 + MCP 回傳 + 最終回應 + 完整 Raw State）
    預設儲存為含時間戳記的檔案名：agent_results/YYYYMMDD_HHMMSS.json
    """
    if not filepath:
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"agent_results/{timestamp_str}.json"

    # 自動建立目錄
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    messages = result.get("messages", [])

    # 1. 整理依時間順序的完整執行軌跡 (Trajectory)
    execution_trajectory = []
    for idx, msg in enumerate(messages, start=1):
        step_item = {
            "step_index": idx,
            "message_type": getattr(msg, "type", type(msg).__name__),
            "content": extract_text(getattr(msg, "content", "")),
            "tool_calls": getattr(msg, "tool_calls", []),
            "tool_call_id": getattr(msg, "tool_call_id", None),
            "tool_name": getattr(msg, "name", None),
            "usage_metadata": getattr(msg, "usage_metadata", None),
            "response_metadata": getattr(msg, "response_metadata", None),
            "additional_kwargs": getattr(msg, "additional_kwargs", {}),
        }

        # 若是 Tool 回傳訊息且內容為 JSON，額外解析出結構化物件方便檢視
        if getattr(msg, "type", None) == "tool":
            raw_content = getattr(msg, "content", "")
            try:
                step_item["parsed_tool_data"] = json.loads(raw_content) if isinstance(raw_content, str) else raw_content
            except Exception:
                step_item["parsed_tool_data"] = None

        execution_trajectory.append(step_item)

    # 2. 轉換 LangChain 官方標準完整物件
    try:
        raw_messages_dump = messages_to_dict(messages)
    except Exception:
        raw_messages_dump = _to_serializable(messages)

    # 3. 組合超完整 Payload
    full_payload = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "user_prompt": prompt,
            "total_steps": len(messages),
            "final_response": extract_text(messages[-1].content) if messages else "",
        },
        "execution_trajectory": execution_trajectory,
        "raw_messages": raw_messages_dump,
        "full_state": _to_serializable(result),
    }

    # 4. 寫入 JSON 檔案
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(full_payload, f, ensure_ascii=False, indent=2)

    return filepath

# ponytail: tool-result-pruning | Ceiling: In-place message trimmer for multi-turn history. Upgrade path: LangGraph State Trimmer if stateful checkpoints used.
def prune_tool_messages(messages: list) -> list:
    """修剪中間查字典過程 (list_available_tables / get_table_schema) 的冗長回傳，避免多輪對話 Context 爆炸"""
    pruned = []
    for msg in messages:
        if getattr(msg, "type", None) == "tool" and getattr(msg, "name", "") in ["list_available_tables", "get_table_schema"]:
            short_content = f"[Pruned: Schema/Table inspection for {getattr(msg, 'name', '')} completed]"
            msg_copy = type(msg)(content=short_content, tool_call_id=getattr(msg, "tool_call_id", None), name=getattr(msg, "name", None))
            pruned.append(msg_copy)
        else:
            pruned.append(msg)
    return pruned

from langchain_mcp_adapters.tools import load_mcp_tools

async def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)

    print("[1/4] 啟動 SMT MCP Server 連線 (server.py)...", flush=True)
    server_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "server.py")).replace("\\", "/")
    client = MultiServerMCPClient({"smt_server": {"command": sys.executable, "args": [server_path], "transport": "stdio"}})

    async with client.session("smt_server") as session:
        await session.initialize()
        print("[2/4] 取得 MCP Tools 清單...", flush=True)
        tools = await load_mcp_tools(session)
        print(f"      成功載入 {len(tools)} 個工具: {[t.name for t in tools]}", flush=True)

        print("[3/4] 初始化 LLM 模型 (Gemini 3.6 Flash)...", flush=True)
        model = get_model()

        system_prompt = """你是一個專業的 SMT 電子製造與工廠大數據分析專家助手。
你擁有訪問工廠數據庫的 MCP 工具（涵蓋製造績效 DPM20、製程追蹤 SFCS 與 SMT 設備感測 AIoT 等 42 張資料表）。

【三大 Token-Saving 黃金準則（最高優先級）】
1. 嚴禁 SELECT * (Selective Projection)：
   • 僅能 SELECT 問題所需的核心欄位（例如 `SELECT line, front_pressure, event_time`），嚴禁使用 SELECT * 把無關欄位全撈進 Context。
2. 資料庫內聚合 (In-DB Aggregation)：
   • 嚴禁將數百筆原始資料撈回給 LLM 自己算平均或統計！統計數值時必須在 SQL 內寫 `AVG()`, `COUNT()`, `SUM()`, `MAX()`, `MIN()`, `GROUP BY` 等，讓 SQLite 只回傳 1~5 行計算結果。
3. 精準關鍵字探索：
   • 呼叫 `list_available_tables` 時【必須帶入具體 keyword】（例如 `keyword="印刷機 刮刀 壓力"` 或 `keyword="printer"`），不要無條件拉取全表清單。

【數據查詢標準 SOP 與引導原則】
1. 探索定位 (Discovery)：若不確定確切表名，優先呼叫 `list_available_tables(keyword=...)` 精準定位 1~3 張目標表。
2. 字典驗證 (Inspection)：在撰寫 SQL 查詢前，先呼叫 `get_table_schema(table_name=...)` 查明欄位名稱與型別，【嚴禁憑空猜測欄位名稱】。
3. 標準時間過濾：所有資料表均已自動補上標準化時間欄位 `event_time` (格式: `YYYY-MM-DD HH:MM:SS`)，請優先在 SQL 的 `WHERE` 條件中使用 `event_time` 進行時間區間過濾。
4. 數值計算轉型：SQLite 中的數值欄位若需進行聚合計算或大小比較，建議使用 `CAST(column_name AS FLOAT)` (例如 `AVG(CAST(front_pressure AS FLOAT))`) 以確保計算精確。
5. 唯讀與自我修復 (Self-Healing)：僅支援 SELECT 查詢。若 `execute_sql_query` 執行回傳錯誤，請閱讀提示修正後重試。
6. 專業總結：取得真實數據後，結合 SMT 製造專業領域知識，向使用者提供結構化、客觀且具洞察力的分析與改善建議。"""

        agent = create_deep_agent(model=model, tools=tools, system_prompt=system_prompt)

        # 測試提問範例
        prompt = "檢查 2026-08-10 整天，S05 產線的 SMT 印刷機，前刮刀的壓力是多少，是否有異常？"
        print(f"\n[4/4] 💬 使用者提問: {prompt}", flush=True)
        print("⏳ DeepAgent 思考中並調用 MCP Tools ...\n", flush=True)

        result = await agent.ainvoke({"messages": [{"role": "user", "content": prompt}]})

        # 印出工具呼叫過程
        print("==================================================", flush=True)
        print("🔍 [DeepAgent 工具調用軌跡 (Tool Calls)]", flush=True)
        print("==================================================", flush=True)
        for msg in result["messages"]:
            for tc in extract_tool_calls(msg):
                print(f"🔧 [LLM 調用工具] : {tc['name']}\n   傳入參數 : {tc['args']}\n", flush=True)
            if getattr(msg, "type", None) == "tool":
                preview = str(msg.content)[:200] + "..." if len(str(msg.content)) > 200 else str(msg.content)
                print(f"📦 [MCP Server 回傳] ({getattr(msg, 'name', 'tool')}):\n   {preview}\n", flush=True)

        # 印出最終分析結果
        print("==================================================", flush=True)
        print("🎯 [DeepAgent 最終分析回覆]", flush=True)
        print("==================================================", flush=True)
        print(extract_text(result["messages"][-1].content), flush=True)

        # 儲存超完整格式 JSON
        saved_path = save_agent_result(result, prompt)
        print(f"\n💾 [已儲存完整生命週期 JSON 結果] -> {saved_path}", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
