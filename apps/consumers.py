import json
import pandas as pd
import httpx
from channels.generic.websocket import AsyncWebsocketConsumer
from django.conf import settings
from asgiref.sync import sync_to_async
from .services.AIDataService import fetch_all_sheets_data


# === 🔹 Ambil Data Google Sheets (dari AIDataService) ===
@sync_to_async
def fetch_data_async():
    """Fetch all sheets data from Google Sheets asynchronously."""
    return fetch_all_sheets_data()

@sync_to_async
def optimize_data_context(df):
    """Optimize DataFrame untuk AI context (kurangi null values, filter kolom penting)."""
    if df.empty:
        return ""
    
    # Drop columns yang mostly null (>80% null)
    null_ratio = df.isnull().sum() / len(df)
    relevant_cols = df.columns[null_ratio < 0.8].tolist()
    df_clean = df[relevant_cols].copy()
    
    # Drop rows yang mostly null
    df_clean = df_clean.dropna(how='all')
    
    # Limit to max 50 rows untuk performa
    df_clean = df_clean.head(50)
    
    return df_clean.to_string()

# === 🔹 Rules agar LLM hanya menjawab berdasarkan data Spreadsheet ===
RULES = """
Peraturan:
1. Kamu TIDAK BOLEH menggunakan pengetahuan luar.
2. Jawaban harus berasal dari data spreadsheet di bawah.
3. Jika tidak ada di data → jawab persis:
"Saya tidak menemukan informasi tersebut di data."
4. Jangan menebak atau membuat data baru.
5. Berikan jawaban dalam Bahasa Indonesia.
6. Jika ditanya tentang data spesifik, cari di tabel berdasarkan kolom/tahun/kategori.
"""


# ============================================================
#               🔥 WebSocket AI Consumer 🔥
# ============================================================
class ChatConsumer(AsyncWebsocketConsumer):
    async def setup_data(self):
        """Mengambil dan menyimpan data dari semua sheets untuk koneksi ini."""
        self.df_all = await fetch_data_async()
        print("📌 All Sheets Loaded for new connection:", not self.df_all.empty)
        if not self.df_all.empty:
            print(f"   → Total rows: {len(self.df_all)}, Total columns: {len(self.df_all.columns)}")

    async def connect(self):
        await self.accept()
        await self.setup_data()

    async def disconnect(self, close_code):
        """Called when the WebSocket closes."""
        print(f"WebSocket disconnected with code: {close_code}")
        
    async def receive(self, text_data):
        user_message = json.loads(text_data)["message"]

        if self.df_all.empty:
            await self.send(text_data=json.dumps({"message": "⚠️ Maaf, saya tidak dapat mengambil data saat ini."}))
            return

        # Optimize data untuk AI context
        data_context = await optimize_data_context(self.df_all)
        
        if not data_context or len(data_context) < 100:
            await self.send(text_data=json.dumps({"message": "⚠️ Data tidak cukup untuk menjawab pertanyaan."}))
            return

        # Limit context ke max 30KB untuk menghindari token overflow
        if len(data_context) > 30000:
            data_context = data_context[:30000] + "\n\n... (data dipotong)"

        prompt = f"{RULES}\n\nData Spreadsheet:\n{data_context}\n\nPertanyaan: {user_message}"
        
        print(f"📤 Sending {len(prompt)} chars to AI...")
        
        # === 🔹 KONFIGURASI API OpenRouter ===
        OPENROUTER_API_KEY = settings.OPENROUTER_API_KEY
        # === 🔹 CALL API OpenRouter ===
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": "tngtech/deepseek-r1t-chimera:free",
                        "messages": [{"role": "user", "content": prompt}],
                    },
                    timeout=60.0  # Increased timeout dari 30 ke 60 detik
                )
                response.raise_for_status()
                result = response.json()
                ai_reply = result.get("choices", [{}])[0].get("message", {}).get("content", "⚠️ Terjadi kesalahan API!")
                print(f"✅ AI replied: {len(ai_reply)} chars")
            except httpx.TimeoutException:
                print("⏱️ API timeout - request took too long")
                ai_reply = "⏱️ Permintaan terlalu lama. Coba pertanyaan yang lebih sederhana atau spesifik."
            except httpx.HTTPStatusError as e:
                print(f"❌ HTTP Error {e.status_code}: {e.response.text[:200]}")
                ai_reply = "❌ Terjadi kesalahan dengan layanan AI. Silakan coba lagi."
            except (httpx.RequestError, KeyError, IndexError) as e:
                print(f"❌ API Error: {e}")
                ai_reply = "⚠️ Terjadi kesalahan saat menghubungi layanan AI."

        # Kirim kembali ke WebSocket Client
        await self.send(text_data=json.dumps({"message": ai_reply}))