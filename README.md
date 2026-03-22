# ⚡ Qlik Sense ETL Script Generator

AI-powered Streamlit app that generates production-ready Qlik Sense RAW & Intermediate QVD scripts from your Metadata.xlsx using Claude.

**No secrets.toml needed** — users enter their own Anthropic API key directly on the app's front page.

---

## 📁 Project Structure

```
qlik_etl_app/
├── app.py                  ← Main Streamlit application
├── prompts.py              ← RAW & Intermediate system prompts
├── requirements.txt        ← Python dependencies
├── .streamlit/
│   └── config.toml         ← Theme & server config
├── .gitignore
└── README.md
```

---

## 🚀 Quick Start (Local)

```bash
# 1. Unzip and enter folder
unzip qlik_etl_app.zip && cd qlik_etl_app

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run
streamlit run app.py
```

Open `http://localhost:8501` — enter your API key on the front page and start generating.

---

## ☁️ Deploy to Streamlit Community Cloud (Free)

1. Push this folder to a **GitHub repository**
2. Go to [share.streamlit.io](https://share.streamlit.io) → **New app**
3. Select your repo, branch, and `app.py` as the main file
4. Click **Deploy** — no secrets needed, users enter their own API key

---

## 🐳 Docker

```bash
docker build -t qlik-etl .
docker run -p 8501:8501 qlik-etl
```

`Dockerfile`:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

---

## 🔑 API Key

Users get their key from **https://console.anthropic.com/settings/keys**

- Keys are session-only — never stored, never logged
- New accounts get $5 free credits (~500+ script generation runs)

---

## ✨ Features

- Front-page API key input (no config files needed)
- Upload Metadata.xlsx or paste pipe-separated metadata
- RAW layer: Full Load, Incremental Insert, Incremental Upsert
- Intermediate layer: Joins, Aggregates, Transformations, Derived Fields
- Generate Both layers in one click
- Per-table `.qvs` download + bulk ZIP download
- Syntax-highlighted script preview
- Full audit logging in every generated script
