# run_deepagent.py
import asyncio
import json
import os
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
        model_name = os.getenv("GEMINI_MODEL", "gemini-3.5-flash")
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
    預設儲存為含時間戳記的檔案名：agent_result_YYYYMMDD_HHMMSS.json
    """
    if not filepath:
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"agent_results/{timestamp_str}.json"

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

async def main():
    server_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "server.py")).replace("\\", "/")
    client = MultiServerMCPClient({"oee_server": {"command": "python", "args": [server_path], "transport": "stdio"}})
    
    tools = await client.get_tools()
    model = get_model()

    system_prompt = (
        "你是一個製造業 OEE 數據與產線效率分析專家助手。"
        "你有權限透過提供的 MCP Tools 查詢工廠 OEE 數據與異常產線。"
        "當使用者詢問工廠運作或效率時，請務必先呼叫對應的工具取得真實數據，再進行專業分析與總結。"
    )

    agent = create_deep_agent(model=model, tools=tools, system_prompt=system_prompt)

    prompt = "請幫我分析 2026-08-15 到 2026-08-17 期間所有廠別的 OEE 狀況，並特別指出效率低於 80% 的產線是哪一條、損失多少工時？"
    print(f"💬 Prompt: {prompt}\n⏳ DeepAgent 思考與呼叫 MCP 工具中...\n")

    result = await agent.ainvoke({"messages": [{"role": "user", "content": prompt}]})

    # 印出工具呼叫過程
    print("==================================================")
    print("🔍 [DeepAgent 工具調用軌跡 (Tool Calls)]")
    print("==================================================")
    for msg in result["messages"]:
        for tc in extract_tool_calls(msg):
            print(f"🔧 [LLM 調用工具] : {tc['name']}\n   傳入參數 : {tc['args']}\n")
        if getattr(msg, "type", None) == "tool":
            preview = str(msg.content)[:200] + "..." if len(str(msg.content)) > 200 else str(msg.content)
            print(f"📦 [MCP Server 回傳] ({getattr(msg, 'name', 'tool')}):\n   {preview}\n")

    # 印出最終分析結果
    print("==================================================")
    print("🎯 [DeepAgent 最終分析回覆]")
    print("==================================================")
    print(extract_text(result["messages"][-1].content))

    # 儲存超完整格式 JSON
    saved_path = save_agent_result(result, prompt)
    print(f"\n💾 [已儲存完整生命週期 JSON 結果] -> {saved_path}")

if __name__ == "__main__":
    asyncio.run(main())
