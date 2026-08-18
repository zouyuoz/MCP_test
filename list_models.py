# list_models.py
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

def main():
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")

    if not api_key or api_key.strip() in ["...", "YOUR_API_KEY"]:
        print("⚠️ 請先在專案目錄下的 `.env` 檔案中填入有效的 GEMINI_API_KEY 再執行此腳本。")
        print("範例 .env 內容：")
        print("GEMINI_API_KEY=AIzaSy...")
        return

    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"

    print("🔍 正在透過 Google API 查詢你的 API Key 可用的模型清單...\n")
    try:
        response = httpx.get(url, timeout=15.0)
        if response.status_code != 200:
            print(f"❌ 查詢失敗 (HTTP {response.status_code}):")
            print(response.text)
            return

        data = response.json()
        models = data.get("models", [])
        
        print(f"✅ 成功獲取 {len(models)} 個可用模型：\n")
        print(f"{'模型名稱 (程式碼中傳入的名稱)':<35} {'支援的生成方法 (Methods)'}")
        print("-" * 75)

        chat_models = []
        for m in models:
            # 去除 "models/" 前綴，取得實際填入程式碼的名稱
            model_id = m.get("name", "").replace("models/", "")
            methods = m.get("supportedGenerationMethods", [])
            methods_str = ", ".join(methods)
            
            # 篩選支援 generateContent (可用於 Agent / Chat / Tool Calling) 的模型
            if "generateContent" in methods:
                chat_models.append(model_id)
                print(f"• {model_id:<33} [{methods_str}]")

        print("\n" + "=" * 75)
        print("💡 推薦用於 DeepAgents (支援 Tool Calling / Function Calling) 的模型名稱：")
        for cm in chat_models:
            if "flash" in cm or "pro" in cm:
                print(f"   👉 \"{cm}\"")

    except Exception as e:
        print("❌ 連線或查詢時發生錯誤:", str(e))

if __name__ == "__main__":
    main()
