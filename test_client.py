# test_client.py
import asyncio
import json
import os
from pathlib import Path
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    # 1. 動態相對路徑
    server_script = str(Path(__file__).parent / "server.py")

    server_params = StdioServerParameters(
        command="python",
        args=[server_script]
    )

    print("[1/4] 連線至 SMT SQL MCP Server (server.py)...")
    
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            print("[2/4] 初始化成功！\n")

            # 3. 列出可用 Tools
            tools_response = await session.list_tools()
            print(f"=== [3/4] 可用工具清單 ({len(tools_response.tools)} 個 SQL Tools) ===")
            for tool in tools_response.tools:
                print(f"• 工具名稱: {tool.name}")
                print(f"  說明: {tool.description.strip() if tool.description else '無'}\n")

            # 4. 測試呼叫 Tool 1: list_available_tables
            print("=== [4/6] 測試 1: list_available_tables ===")
            res1 = await session.call_tool(name="list_available_tables", arguments={"keyword": "printer"})
            print(res1.content[0].text if res1.content else "無內容")

            # 5. 測試呼叫 Tool 2: get_table_schema
            print("\n=== [5/6] 測試 2: get_table_schema ===")
            res2 = await session.call_tool(name="get_table_schema", arguments={"table_name": "aiot_smt_printer_real_processing_data_wihn2"})
            print(res2.content[0].text if res2.content else "無內容")

            # 6. 測試呼叫 Tool 3: execute_sql_query (含 AVG 統計與 event_time 時間過濾)
            print("\n=== [6/6] 測試 3: execute_sql_query ===")
            sql_test = "SELECT AVG(CAST(front_pressure AS FLOAT)) AS avg_front_pressure, COUNT(*) AS total_cnt FROM aiot_smt_printer_real_processing_data_wihn2 WHERE event_time >= '2026-08-01'"
            res3 = await session.call_tool(name="execute_sql_query", arguments={"sql_query": sql_test})
            print(res3.content[0].text if res3.content else "無內容")

if __name__ == "__main__":
    asyncio.run(main())


