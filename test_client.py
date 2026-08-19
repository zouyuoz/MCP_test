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
            print(f"=== [3/4] 可用工具清單 ({len(tools_response.tools)} 個 Tools) ===")
            for tool in tools_response.tools:
                print(f"• 工具名稱: {tool.name}")
                print(f"  說明: {tool.description.strip() if tool.description else '無'}\n")

            # 4. 測試呼叫 1: get_line_metrics_summary
            print("=== [4/4] 測試呼叫: get_line_metrics_summary ===")
            test_args_1 = {
                "start_date": "2026-08-15",
                "end_date": "2026-08-17",
                "line": "LINE_SMT_01",
                "metrics": ["oee", "fpyr"]
            }
            print(f"傳入參數: {json.dumps(test_args_1, ensure_ascii=False)}")
            result_1 = await session.call_tool(name="get_line_metrics_summary", arguments=test_args_1)
            for content in result_1.content:
                print(content.text if hasattr(content, "text") else content)

            # 5. 測試呼叫 2: analyze_cross_process_correlation
            print("\n=== 測試呼叫: analyze_cross_process_correlation ===")
            test_args_2 = {
                "line": "LINE_SMT_01",
                "start_time": "2026-08-17 08:00:00",
                "end_time": "2026-08-17 17:00:00",
                "defect_type": "solder_bridge"
            }
            result_2 = await session.call_tool(name="analyze_cross_process_correlation", arguments=test_args_2)
            for content in result_2.content:
                print(content.text if hasattr(content, "text") else content)

if __name__ == "__main__":
    asyncio.run(main())

