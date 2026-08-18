# test_client.py
import asyncio
import json
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    # 1. 指定 MCP Server 的啟動命令與腳本路徑
    server_params = StdioServerParameters(
        command="python",
        args=["C:/Users/zyqio/source/repos/MCP_test/server.py"]
    )

    print("[1/4] 連線至 MCP Server (server.py)...")
    
    # 2. 透過 stdio 與 Server 建立通訊
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # 初始化連線交握
            await session.initialize()
            print("[2/4] 初始化成功！\n")

            # 3. 列出可用的 Tools
            tools_response = await session.list_tools()
            print("=== [3/4] 可用工具清單 (Tools) ===")
            for tool in tools_response.tools:
                print(f"• 工具名稱: {tool.name}")
                print(f"  說明: {tool.description.strip() if tool.description else '無'}")
                print(f"  參數綱要: {json.dumps(getattr(tool, 'input_schema', getattr(tool, 'inputSchema', {})), ensure_ascii=False, indent=2)}\n")

            # 4. 測試呼叫 1: get_oee_detail 工具
            print("=== [4/5] 測試呼叫 1: get_oee_detail ===")
            test_args_1 = {
                "start_date": "2026-08-15",
                "end_date": "2026-08-17",
                "line": "SMT01",
                "plant": "FC2A",
                "shift_type": "Day shift"
            }
            print(f"傳入參數: {json.dumps(test_args_1, ensure_ascii=False)}")
            result_1 = await session.call_tool(name="get_oee_detail", arguments=test_args_1)
            for content in result_1.content:
                print(content.text if hasattr(content, "text") else content)

            # 5. 測試呼叫 2: get_low_efficiency_lines 工具
            print("\n=== [5/5] 測試呼叫 2: get_low_efficiency_lines ===")
            test_args_2 = {"target_date": "2026-08-17", "threshold_pct": 80.0}
            print(f"傳入參數: {json.dumps(test_args_2, ensure_ascii=False)}")
            result_2 = await session.call_tool(name="get_low_efficiency_lines", arguments=test_args_2)
            for content in result_2.content:
                print(content.text if hasattr(content, "text") else content)

if __name__ == "__main__":
    asyncio.run(main())
