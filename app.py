import streamlit as st
import pandas as pd
import anthropic
import io
import zipfile
import re
from datetime import datetime

from prompts import RAW_SYSTEM_PROMPT, INT_SYSTEM_PROMPT

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="QlikForge — ETL Script Generator",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── PREMIUM CSS ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── GLOBAL RESET ── */
*, *::before, *::after { box-sizing: border-box; }

html, body, [class*="css"], .stApp {
    background-color: #080c14 !important;
    color: #e2e8f0 !important;
    font-family: 'Space Grotesk', sans-serif !important;
}

.main .block-container {
    padding: 2rem 3rem 4rem !important;
    max-width: 1200px !important;
}

/* ── ANIMATED BACKGROUND GRID ── */
.stApp::before {
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background-image:
        linear-gradient(rgba(0,210,255,0.03) 1px, transparent 1px),
        linear-gradient(90deg, rgba(0,210,255,0.03) 1px, transparent 1px);
    background-size: 60px 60px;
    pointer-events: none;
    z-index: 0;
}

/* ── HERO HEADER ── */
.hero-wrap {
    position: relative;
    padding: 3rem 0 2rem;
    margin-bottom: 2.5rem;
    text-align: center;
}
.hero-glow {
    position: absolute;
    top: 50%; left: 50%;
    transform: translate(-50%, -60%);
    width: 600px; height: 300px;
    background: radial-gradient(ellipse, rgba(0,210,255,0.12) 0%, transparent 70%);
    pointer-events: none;
    z-index: 0;
}
.hero-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: rgba(0,210,255,0.08);
    border: 1px solid rgba(0,210,255,0.25);
    border-radius: 100px;
    padding: 5px 16px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #00d2ff;
    margin-bottom: 1.2rem;
}
.hero-title {
    font-size: 3.2rem;
    font-weight: 700;
    line-height: 1.1;
    background: linear-gradient(135deg, #ffffff 0%, #00d2ff 50%, #7b2fff 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin: 0 0 0.8rem;
    position: relative;
    z-index: 1;
}
.hero-sub {
    font-size: 1rem;
    color: #64748b;
    font-weight: 400;
    position: relative;
    z-index: 1;
}

/* ── STEP PILLS ── */
.steps-row {
    display: flex;
    align-items: center;
    gap: 0;
    margin-bottom: 2.5rem;
    padding: 0 1rem;
}
.step-pill {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 10px 20px;
    border-radius: 100px;
    font-size: 0.82rem;
    font-weight: 500;
    white-space: nowrap;
}
.step-pill.active {
    background: rgba(0,210,255,0.1);
    border: 1px solid rgba(0,210,255,0.35);
    color: #00d2ff;
}
.step-pill.done {
    background: rgba(0,255,136,0.07);
    border: 1px solid rgba(0,255,136,0.2);
    color: #00ff88;
}
.step-pill.idle {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(255,255,255,0.08);
    color: #475569;
}
.step-num {
    width: 22px; height: 22px;
    border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-size: 0.72rem; font-weight: 700;
}
.step-pill.active .step-num { background: #00d2ff; color: #080c14; }
.step-pill.done .step-num   { background: #00ff88; color: #080c14; }
.step-pill.idle .step-num   { background: rgba(255,255,255,0.08); color: #64748b; }
.step-connector {
    flex: 1;
    height: 1px;
    background: rgba(255,255,255,0.06);
    margin: 0 4px;
}

/* ── SECTION CARDS ── */
.section-card {
    background: rgba(255,255,255,0.025);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 16px;
    padding: 1.8rem 2rem;
    margin-bottom: 1.5rem;
    position: relative;
    overflow: hidden;
}
.section-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, rgba(0,210,255,0.4), transparent);
}
.section-card.key-card::before {
    background: linear-gradient(90deg, transparent, rgba(0,210,255,0.5), transparent);
}
.section-card.prompt-card::before {
    background: linear-gradient(90deg, transparent, rgba(123,47,255,0.5), transparent);
}
.section-card.meta-card::before {
    background: linear-gradient(90deg, transparent, rgba(255,170,0,0.4), transparent);
}
.section-card.gen-card::before {
    background: linear-gradient(90deg, transparent, rgba(0,255,136,0.4), transparent);
}

.section-label {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 1.2rem;
}
.section-label .icon-wrap {
    width: 36px; height: 36px;
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: 1rem;
}
.icon-blue  { background: rgba(0,210,255,0.12); border: 1px solid rgba(0,210,255,0.2); }
.icon-purple{ background: rgba(123,47,255,0.12); border: 1px solid rgba(123,47,255,0.2); }
.icon-amber { background: rgba(255,170,0,0.12);  border: 1px solid rgba(255,170,0,0.2); }
.icon-green { background: rgba(0,255,136,0.12);  border: 1px solid rgba(0,255,136,0.2); }

.section-title {
    font-size: 1rem;
    font-weight: 600;
    color: #e2e8f0;
}
.section-hint {
    font-size: 0.78rem;
    color: #475569;
    margin-top: 1px;
}

/* ── STATUS BADGES ── */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    border-radius: 100px;
    padding: 4px 12px;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.02em;
}
.badge-cyan   { background: rgba(0,210,255,0.1);  border: 1px solid rgba(0,210,255,0.25); color: #00d2ff; }
.badge-green  { background: rgba(0,255,136,0.1);  border: 1px solid rgba(0,255,136,0.25); color: #00ff88; }
.badge-amber  { background: rgba(255,170,0,0.1);  border: 1px solid rgba(255,170,0,0.25); color: #ffaa00; }
.badge-red    { background: rgba(255,80,80,0.1);   border: 1px solid rgba(255,80,80,0.25);  color: #ff5050; }
.badge-purple { background: rgba(123,47,255,0.1); border: 1px solid rgba(123,47,255,0.25); color: #a78bfa; }

/* ── STREAMLIT OVERRIDES ── */
/* Inputs */
.stTextInput input, .stTextArea textarea {
    background: #f8fafc !important;
    border: 1px solid rgba(255,255,255,0.15) !important;
    border-radius: 10px !important;
    color: #0f172a !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 0.9rem !important;
    transition: border-color 0.2s !important;
}
.stTextInput input:focus, .stTextArea textarea:focus {
    border-color: rgba(0,210,255,0.5) !important;
    box-shadow: 0 0 0 3px rgba(0,210,255,0.08) !important;
}
.stTextInput input[type="password"] {
    font-family: 'JetBrains Mono', monospace !important;
    letter-spacing: 0.1em !important;
}

/* Buttons */
.stButton > button {
    background: linear-gradient(135deg, #00d2ff, #7b2fff) !important;
    color: #fff !important;
    border: none !important;
    border-radius: 10px !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.88rem !important;
    padding: 0.6rem 1.4rem !important;
    transition: all 0.2s !important;
    letter-spacing: 0.02em !important;
}
.stButton > button:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 8px 24px rgba(0,210,255,0.25) !important;
}
.stButton > button:active { transform: translateY(0) !important; }

/* Secondary buttons (reset etc.) */
.stButton > button[kind="secondary"] {
    background: rgba(255,255,255,0.05) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    color: #94a3b8 !important;
}

/* Download buttons */
.stDownloadButton > button {
    background: rgba(0,255,136,0.1) !important;
    border: 1px solid rgba(0,255,136,0.3) !important;
    color: #00ff88 !important;
    border-radius: 10px !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-weight: 600 !important;
    transition: all 0.2s !important;
}
.stDownloadButton > button:hover {
    background: rgba(0,255,136,0.18) !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 6px 20px rgba(0,255,136,0.2) !important;
}

/* Tabs */
.stTabs [data-baseweb="tab-list"] {
    background: rgba(255,255,255,0.03) !important;
    border-radius: 12px !important;
    padding: 4px !important;
    gap: 2px !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border-radius: 9px !important;
    color: #64748b !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-weight: 500 !important;
    font-size: 0.85rem !important;
    padding: 8px 20px !important;
    transition: all 0.2s !important;
}
.stTabs [aria-selected="true"] {
    background: rgba(0,210,255,0.12) !important;
    color: #00d2ff !important;
    border: 1px solid rgba(0,210,255,0.25) !important;
}
.stTabs [data-baseweb="tab-panel"] {
    padding-top: 1.5rem !important;
}

/* Radio */
.stRadio > div {
    gap: 8px !important;
}
.stRadio label {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    border-radius: 10px !important;
    padding: 8px 16px !important;
    color: #94a3b8 !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 0.85rem !important;
    cursor: pointer !important;
    transition: all 0.15s !important;
}
.stRadio label:hover {
    border-color: rgba(0,210,255,0.3) !important;
    color: #e2e8f0 !important;
}

/* File uploader */
[data-testid="stFileUploader"] {
    background: rgba(255,255,255,0.02) !important;
    border: 1.5px dashed rgba(0,210,255,0.2) !important;
    border-radius: 12px !important;
    transition: all 0.2s !important;
}
[data-testid="stFileUploader"]:hover {
    border-color: rgba(0,210,255,0.4) !important;
    background: rgba(0,210,255,0.03) !important;
}

/* Expanders */
.streamlit-expanderHeader {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 10px !important;
    color: #94a3b8 !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 0.88rem !important;
}
.streamlit-expanderContent {
    background: rgba(255,255,255,0.01) !important;
    border: 1px solid rgba(255,255,255,0.05) !important;
    border-top: none !important;
    border-radius: 0 0 10px 10px !important;
}

/* Metrics */
[data-testid="stMetric"] {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 12px !important;
    padding: 1rem 1.2rem !important;
}
[data-testid="stMetricLabel"] {
    color: #64748b !important;
    font-size: 0.78rem !important;
    font-family: 'Space Grotesk', sans-serif !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
}
[data-testid="stMetricValue"] {
    color: #00d2ff !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-weight: 700 !important;
}

/* Code blocks */
.stCodeBlock {
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 12px !important;
    overflow: hidden !important;
}
.stCodeBlock code {
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.8rem !important;
}

/* Dataframe */
[data-testid="stDataFrame"] {
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 12px !important;
    overflow: hidden !important;
}

/* Divider */
hr { border-color: rgba(255,255,255,0.06) !important; }

/* Alert / info boxes */
.stAlert {
    background: rgba(255,255,255,0.03) !important;
    border-radius: 10px !important;
    border: 1px solid rgba(255,255,255,0.08) !important;
    color: #94a3b8 !important;
    font-family: 'Space Grotesk', sans-serif !important;
}

/* Labels and captions */
label, .stTextInput label, .stTextArea label {
    color: #64748b !important;
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.04em !important;
}
.stCaption, [data-testid="stCaptionContainer"] {
    color: #334155 !important;
    font-size: 0.75rem !important;
}

/* Spinner */
.stSpinner > div { border-top-color: #00d2ff !important; }

/* Status widget */
[data-testid="stStatusWidget"] {
    background: rgba(255,255,255,0.03) !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 12px !important;
}

/* Scrollbar */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: rgba(255,255,255,0.02); }
::-webkit-scrollbar-thumb { background: rgba(0,210,255,0.2); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: rgba(0,210,255,0.35); }

/* Select / dropdown */
.stSelectbox select, [data-baseweb="select"] {
    background: rgba(255,255,255,0.04) !important;
    border: 1px solid rgba(255,255,255,0.1) !important;
    color: #e2e8f0 !important;
    border-radius: 10px !important;
}

/* Markdown headings */
h1,h2,h3,h4 {
    font-family: 'Space Grotesk', sans-serif !important;
    color: #e2e8f0 !important;
}

/* Hide streamlit branding */
#MainMenu, footer, header { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ─── SESSION STATE ────────────────────────────────────────────────────────────
if "raw_prompt" not in st.session_state:
    st.session_state["raw_prompt"] = RAW_SYSTEM_PROMPT
if "int_prompt" not in st.session_state:
    st.session_state["int_prompt"] = INT_SYSTEM_PROMPT


# ─── HELPERS ──────────────────────────────────────────────────────────────────
def get_client():
    key = st.session_state.get("api_key", "").strip()
    return anthropic.Anthropic(api_key=key) if key else None

def metadata_to_text(df): return df.to_csv(sep="|", index=False)

def call_claude(client, system_prompt, user_content):
    with st.spinner("✦ Claude is generating your scripts..."):
        msg = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
        )
    return msg.content[0].text

def split_scripts(raw_text):
    parts = re.split(r"//\s*={3,}\s*TABLE:\s*", raw_text, flags=re.IGNORECASE)
    parts = [p for p in parts if p.strip()]
    if len(parts) <= 1:
        return [{"label": "Generated Script", "script": raw_text.strip()}]
    result = []
    for p in parts:
        lines = p.split("\n")
        label = lines[0].replace("=", "").strip()
        script = "\n".join(lines[1:]).strip()
        if script:
            result.append({"label": label or "Script", "script": script})
    return result

def build_zip(scripts, prefix):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        combined = []
        for s in scripts:
            fname = f"{s['label'].replace(' ','_').replace('/','_')}.qvs"
            zf.writestr(fname, s["script"])
            combined.append(f"// ===== TABLE: {s['label']} =====\n{s['script']}")
        zf.writestr(f"{prefix}_ALL_SCRIPTS.qvs", "\n\n".join(combined))
    return buf.getvalue()

def prompt_is_edited(key, default):
    return st.session_state.get(key, default).strip() != default.strip()

def render_scripts(scripts, prefix, key_prefix):
    if not scripts: return
    st.markdown(
        f'<div class="badge badge-green" style="margin-bottom:1rem;">✓ &nbsp;{len(scripts)} script{"s" if len(scripts)>1 else ""} generated</div>',
        unsafe_allow_html=True
    )
    col1, _ = st.columns([1,3])
    with col1:
        st.download_button(
            label=f"⬇  Download All  ({len(scripts)} files)",
            data=build_zip(scripts, prefix),
            file_name=f"{prefix}_scripts_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            mime="application/zip",
            use_container_width=True,
        )
    st.divider()
    for i, s in enumerate(scripts):
        with st.expander(f"📄  {s['label']}", expanded=(i == 0)):
            ca, cb = st.columns([5, 1])
            with cb:
                st.download_button(
                    "⬇ .qvs", data=s["script"],
                    file_name=f"{s['label'].replace(' ','_')}.qvs",
                    mime="text/plain",
                    key=f"{key_prefix}_dl_{i}",
                    use_container_width=True,
                )
            with ca:
                st.code(s["script"], language="sql")


# ─── HERO ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero-wrap">
    <div class="hero-glow"></div>
    <div class="hero-badge">⚡ &nbsp; AI-Powered &nbsp;·&nbsp; Qlik Sense &nbsp;·&nbsp; ETL Automation</div>
    <h1 class="hero-title">QlikForge</h1>
    <p class="hero-sub">Transform metadata into production-ready QVD scripts in seconds — powered by Claude AI</p>
</div>
""", unsafe_allow_html=True)

# ─── STEP TRACKER ─────────────────────────────────────────────────────────────
has_key  = bool(st.session_state.get("api_key","").strip())
has_meta = ("raw_df" in st.session_state and st.session_state.raw_df is not None) or \
           ("int_df" in st.session_state and st.session_state.int_df is not None)
has_out  = bool(st.session_state.get("raw_scripts") or st.session_state.get("int_scripts"))

def pill(num, label, state):
    cls = {"active":"active","done":"done","idle":"idle"}[state]
    return f'<div class="step-pill {cls}"><span class="step-num">{num}</span>{label}</div>'

steps_html = f"""
<div class="steps-row">
    {pill("1","API Key","done" if has_key else "active")}
    <div class="step-connector"></div>
    {pill("2","Prompts","done" if has_key else "idle")}
    <div class="step-connector"></div>
    {pill("3","Metadata","done" if has_meta else ("active" if has_key else "idle"))}
    <div class="step-connector"></div>
    {pill("4","Generate","done" if has_out else ("active" if has_meta else "idle"))}
</div>
"""
st.markdown(steps_html, unsafe_allow_html=True)


# ─── STEP 1 — API KEY ─────────────────────────────────────────────────────────
st.markdown("""
<div class="section-card key-card">
    <div class="section-label">
        <div class="icon-wrap icon-blue">🔑</div>
        <div>
            <div class="section-title">Step 1 &nbsp;—&nbsp; Anthropic API Key</div>
            <div class="section-hint">Session-only · never stored · used only to call Claude</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

col_k, col_s = st.columns([4, 1])
with col_k:
    raw_key = st.text_input(
        "API Key",
        type="password",
        placeholder="sk-ant-api03-···",
        value=st.session_state.get("api_key", ""),
        label_visibility="collapsed",
    )
    if raw_key:
        st.session_state["api_key"] = raw_key.strip()
    st.caption("🔒 Never persisted. Get yours at console.anthropic.com → API Keys")

with col_s:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.session_state.get("api_key","").strip():
        st.markdown('<div class="badge badge-green">✓ &nbsp;Connected</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="badge badge-amber">⚠ &nbsp;Required</div>', unsafe_allow_html=True)
        st.markdown('<a href="https://console.anthropic.com/settings/keys" target="_blank" style="font-size:0.78rem;color:#00d2ff;text-decoration:none;">Get key →</a>', unsafe_allow_html=True)

if not st.session_state.get("api_key","").strip():
    st.markdown('<div class="badge badge-amber" style="margin-top:1rem;">Enter your API key above to continue</div>', unsafe_allow_html=True)
    st.stop()

client = get_client()


# ─── STEP 2 — PROMPT EDITOR ───────────────────────────────────────────────────
st.markdown("""
<div class="section-card prompt-card">
    <div class="section-label">
        <div class="icon-wrap icon-purple">✏️</div>
        <div>
            <div class="section-title">Step 2 &nbsp;—&nbsp; Customize AI Prompts &nbsp;<span style="font-size:0.72rem;color:#475569;font-weight:400;">(optional)</span></div>
            <div class="section-hint">Edit the system prompts Claude uses to generate your scripts</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

with st.expander("✦  Open Prompt Editor", expanded=False):
    ptab1, ptab2 = st.tabs(["🗄  RAW Layer Prompt", "🔄  Intermediate Layer Prompt"])

    with ptab1:
        r1, r2, r3 = st.columns([3,1,1])
        with r1:
            if prompt_is_edited("raw_prompt", RAW_SYSTEM_PROMPT):
                st.markdown('<span class="badge badge-purple">✏ &nbsp;Modified</span>', unsafe_allow_html=True)
            else:
                st.markdown('<span class="badge badge-cyan">◈ &nbsp;Default</span>', unsafe_allow_html=True)
        with r3:
            if st.button("↺ Reset", key="reset_raw"):
                st.session_state["raw_prompt"] = RAW_SYSTEM_PROMPT
                st.rerun()

        new_raw = st.text_area("raw_ed", value=st.session_state["raw_prompt"],
                               height=380, label_visibility="collapsed", key="raw_prompt_area")
        if new_raw != st.session_state["raw_prompt"]:
            st.session_state["raw_prompt"] = new_raw
        st.caption(f"{len(new_raw):,} characters")

    with ptab2:
        i1, i2, i3 = st.columns([3,1,1])
        with i1:
            if prompt_is_edited("int_prompt", INT_SYSTEM_PROMPT):
                st.markdown('<span class="badge badge-purple">✏ &nbsp;Modified</span>', unsafe_allow_html=True)
            else:
                st.markdown('<span class="badge badge-cyan">◈ &nbsp;Default</span>', unsafe_allow_html=True)
        with i3:
            if st.button("↺ Reset", key="reset_int"):
                st.session_state["int_prompt"] = INT_SYSTEM_PROMPT
                st.rerun()

        new_int = st.text_area("int_ed", value=st.session_state["int_prompt"],
                               height=380, label_visibility="collapsed", key="int_prompt_area")
        if new_int != st.session_state["int_prompt"]:
            st.session_state["int_prompt"] = new_int
        st.caption(f"{len(new_int):,} characters")


# ─── STEP 3 — METADATA ────────────────────────────────────────────────────────
st.markdown("""
<div class="section-card meta-card">
    <div class="section-label">
        <div class="icon-wrap icon-amber">📊</div>
        <div>
            <div class="section-title">Step 3 &nbsp;—&nbsp; Provide Metadata</div>
            <div class="section-hint">Upload your Metadata.xlsx or paste pipe-separated rows</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

mode = st.radio("mode", ["📊  Upload Excel", "📋  Paste Metadata"],
                horizontal=True, label_visibility="collapsed")
use_excel = "Excel" in mode

raw_df = int_df = None
raw_paste = int_paste = ""

if use_excel:
    uploaded = st.file_uploader("Drop your Metadata.xlsx here", type=["xlsx","xls"])
    if uploaded:
        try:
            xl = pd.ExcelFile(uploaded)
            sheets = xl.sheet_names
            st.markdown(
                f'<div class="badge badge-cyan" style="margin:0.5rem 0 1rem;">📂 &nbsp;{uploaded.name} &nbsp;·&nbsp; {len(sheets)} sheets: {", ".join(sheets)}</div>',
                unsafe_allow_html=True
            )
            rs = next((s for s in sheets if "raw" in s.lower()), sheets[0])
            is_ = next((s for s in sheets if "inter" in s.lower()), sheets[min(1,len(sheets)-1)])
            raw_df = pd.read_excel(uploaded, sheet_name=rs).fillna("")
            int_df = pd.read_excel(uploaded, sheet_name=is_).fillna("")
            st.session_state["raw_df"] = raw_df
            st.session_state["int_df"] = int_df

            c1, c2 = st.columns(2)
            with c1:
                with st.expander(f"👁  RAW sheet preview  ({len(raw_df)} rows)"):
                    st.dataframe(raw_df, use_container_width=True)
            with c2:
                with st.expander(f"👁  Intermediate sheet preview  ({len(int_df)} rows)"):
                    st.dataframe(int_df, use_container_width=True)
        except Exception as e:
            st.error(f"Parse error: {e}")
else:
    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<p style="font-size:0.85rem;color:#64748b;font-weight:500;margin-bottom:6px;">RAW Layer Metadata</p>', unsafe_allow_html=True)
        raw_paste = st.text_area("raw_paste", height=160,
            placeholder="Layer|Table_Name|Source_Type|Source_Name|Source_Columns|...",
            label_visibility="collapsed")
    with c2:
        st.markdown('<p style="font-size:0.85rem;color:#64748b;font-weight:500;margin-bottom:6px;">Intermediate Layer Metadata</p>', unsafe_allow_html=True)
        int_paste = st.text_area("int_paste", height=160,
            placeholder="Layer|Table_Name|Source_Type|Source_QVDs|Key_Columns|...",
            label_visibility="collapsed")


# ─── STEP 4 — GENERATE ────────────────────────────────────────────────────────
st.markdown("""
<div class="section-card gen-card">
    <div class="section-label">
        <div class="icon-wrap icon-green">⚡</div>
        <div>
            <div class="section-title">Step 4 &nbsp;—&nbsp; Generate Scripts</div>
            <div class="section-hint">Claude will produce production-ready Qlik QVD scripts from your metadata</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# Active prompt indicator
r_mod = prompt_is_edited("raw_prompt", RAW_SYSTEM_PROMPT)
i_mod = prompt_is_edited("int_prompt", INT_SYSTEM_PROMPT)
if r_mod or i_mod:
    mods = " + ".join(filter(None, ["RAW" if r_mod else "", "INT" if i_mod else ""]))
    st.markdown(f'<div class="badge badge-purple" style="margin-bottom:1rem;">✏ &nbsp;Custom prompt active: {mods}</div>', unsafe_allow_html=True)

tab_r, tab_i, tab_b = st.tabs(["🗄  RAW Layer", "🔄  Intermediate Layer", "⚡  Generate Both"])

# ── RAW ──────────────────────────────────────────────────────────────────────
with tab_r:
    st.markdown('<p style="color:#475569;font-size:0.88rem;">Extracts from SQL source tables → QVDs · Full Load · Incremental Insert · Incremental Upsert</p>', unsafe_allow_html=True)
    ca, cb = st.columns([3,1])
    with ca:
        rdf = st.session_state.get("raw_df") if use_excel else None
        if rdf is not None and not rdf.empty:
            st.markdown(f'<span class="badge badge-cyan">◈ &nbsp;{len(rdf)} tables ready</span>', unsafe_allow_html=True)
        elif raw_paste.strip():
            st.markdown('<span class="badge badge-cyan">◈ &nbsp;Pasted metadata ready</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="badge badge-amber">⚠ &nbsp;Add metadata in Step 3</span>', unsafe_allow_html=True)
    with cb:
        gen_r = st.button("Generate RAW ▶", type="primary", use_container_width=True, key="btn_r")

    if gen_r:
        meta = metadata_to_text(rdf) if (use_excel and rdf is not None) else raw_paste
        if not meta or not meta.strip():
            st.error("No RAW metadata found.")
        else:
            try:
                res = call_claude(client, st.session_state["raw_prompt"],
                    f"Generate RAW layer Qlik scripts for the following metadata:\n\n{meta}")
                st.session_state["raw_scripts"] = split_scripts(res)
            except Exception as e:
                st.error(f"API Error: {e}")

    if st.session_state.get("raw_scripts"):
        render_scripts(st.session_state["raw_scripts"], "RAW", "raw")

# ── INTERMEDIATE ─────────────────────────────────────────────────────────────
with tab_i:
    st.markdown('<p style="color:#475569;font-size:0.88rem;">Joins RAW QVDs · Aggregations · Derived Fields · Transformations (FinYear, ratios, durations)</p>', unsafe_allow_html=True)
    ca2, cb2 = st.columns([3,1])
    with ca2:
        idf = st.session_state.get("int_df") if use_excel else None
        if idf is not None and not idf.empty:
            st.markdown(f'<span class="badge badge-cyan">◈ &nbsp;{len(idf)} tables ready</span>', unsafe_allow_html=True)
        elif int_paste.strip():
            st.markdown('<span class="badge badge-cyan">◈ &nbsp;Pasted metadata ready</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="badge badge-amber">⚠ &nbsp;Add metadata in Step 3</span>', unsafe_allow_html=True)
    with cb2:
        gen_i = st.button("Generate INT ▶", type="primary", use_container_width=True, key="btn_i")

    if gen_i:
        meta = metadata_to_text(idf) if (use_excel and idf is not None) else int_paste
        if not meta or not meta.strip():
            st.error("No Intermediate metadata found.")
        else:
            try:
                res = call_claude(client, st.session_state["int_prompt"],
                    f"Generate Intermediate layer Qlik scripts for the following metadata:\n\n{meta}")
                st.session_state["int_scripts"] = split_scripts(res)
            except Exception as e:
                st.error(f"API Error: {e}")

    if st.session_state.get("int_scripts"):
        render_scripts(st.session_state["int_scripts"], "INT", "int")

# ── BOTH ──────────────────────────────────────────────────────────────────────
with tab_b:
    st.markdown('<p style="color:#475569;font-size:0.88rem;">Generates RAW + Intermediate in sequence and bundles everything into one ZIP</p>', unsafe_allow_html=True)
    gen_b = st.button("⚡  Generate All Scripts", type="primary", key="btn_b")

    if gen_b:
        rdf2 = st.session_state.get("raw_df") if use_excel else None
        idf2 = st.session_state.get("int_df") if use_excel else None

        with st.status("Generating RAW layer...", expanded=True) as sr:
            rm = metadata_to_text(rdf2) if (use_excel and rdf2 is not None) else raw_paste
            if rm and rm.strip():
                try:
                    res = call_claude(client, st.session_state["raw_prompt"],
                        f"Generate RAW layer Qlik scripts for the following metadata:\n\n{rm}")
                    st.session_state["raw_scripts"] = split_scripts(res)
                    sr.update(label=f"✓ RAW done — {len(st.session_state['raw_scripts'])} scripts", state="complete")
                except Exception as e:
                    sr.update(label=f"✗ RAW failed: {e}", state="error")
            else:
                sr.update(label="— RAW skipped (no metadata)", state="complete")

        with st.status("Generating Intermediate layer...", expanded=True) as si:
            im = metadata_to_text(idf2) if (use_excel and idf2 is not None) else int_paste
            if im and im.strip():
                try:
                    res = call_claude(client, st.session_state["int_prompt"],
                        f"Generate Intermediate layer Qlik scripts for the following metadata:\n\n{im}")
                    st.session_state["int_scripts"] = split_scripts(res)
                    si.update(label=f"✓ Intermediate done — {len(st.session_state['int_scripts'])} scripts", state="complete")
                except Exception as e:
                    si.update(label=f"✗ Intermediate failed: {e}", state="error")
            else:
                si.update(label="— Intermediate skipped (no metadata)", state="complete")

    ro = bool(st.session_state.get("raw_scripts"))
    io_ = bool(st.session_state.get("int_scripts"))

    if ro or io_:
        st.divider()
        all_s = []
        if ro:  all_s += [{"label":f"RAW_{s['label']}","script":s["script"]} for s in st.session_state["raw_scripts"]]
        if io_: all_s += [{"label":f"INT_{s['label']}","script":s["script"]} for s in st.session_state["int_scripts"]]

        c1, c2, c3 = st.columns(3)
        c1.metric("RAW Scripts",  len(st.session_state.get("raw_scripts",[])))
        c2.metric("INT Scripts",  len(st.session_state.get("int_scripts",[])))
        c3.metric("Total Scripts", len(all_s))

        st.download_button(
            label=f"⬇  Download All {len(all_s)} Scripts  (.zip)",
            data=build_zip(all_s, "QLIK_ETL"),
            file_name=f"QLIK_ETL_ALL_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            mime="application/zip",
        )
        if ro:
            st.markdown("#### RAW Scripts")
            render_scripts(st.session_state["raw_scripts"], "RAW", "b_raw")
        if io_:
            st.markdown("#### Intermediate Scripts")
            render_scripts(st.session_state["int_scripts"], "INT", "b_int")

# ─── FOOTER ───────────────────────────────────────────────────────────────────
st.markdown("""
<div style="margin-top:4rem;padding-top:1.5rem;border-top:1px solid rgba(255,255,255,0.06);
            display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;">
    <span style="font-size:0.78rem;color:#1e293b;">
        QlikForge &nbsp;·&nbsp; Built with Claude AI &nbsp;·&nbsp; Anthropic
    </span>
    <span style="font-size:0.78rem;color:#1e293b;">
        RAW layer &nbsp;+&nbsp; Intermediate layer &nbsp;+&nbsp; Audit logging
    </span>
</div>
""", unsafe_allow_html=True)
