# run_deepagent.py
import asyncio
import os
from dotenv import load_dotenv
from deepagents import create_deep_agent
from langchain_mcp_adapters.client import MultiServerMCPClient

load_dotenv()

def get_model():
    if api_key := (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")):
        from langchain_google_genai import ChatGoogleGenerativeAI
        model_name = os.getenv("GEMINI_MODEL", "gemini-3.5-flash")
        return ChatGoogleGenerativeAI(model=model_name, google_api_key=api_key)
    if os.getenv("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI
        return ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"))
    if os.getenv("ANTHROPIC_API_KEY"):
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"))
    raise ValueError("未檢測到 API Key，請在 .env 中設定 GEMINI_API_KEY、OPENAI_API_KEY 或 ANTHROPIC_API_KEY。")

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
    print(result["messages"][-1].content)

if __name__ == "__main__":
    asyncio.run(main())
