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
    page_title="Qlik ETL Script Generator",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─── CUSTOM CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { padding: 8px 24px; border-radius: 6px; }

    .api-key-box {
        background: #f0f4ff;
        border: 1.5px solid #4361ee;
        border-radius: 12px;
        padding: 1.5rem 2rem;
        margin-bottom: 2rem;
    }
    .api-key-label {
        font-size: 0.85rem;
        color: #4361ee;
        font-weight: 600;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        margin-bottom: 0.4rem;
    }
    .connected-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: #e8f5e9;
        border: 1px solid #a5d6a7;
        border-radius: 20px;
        padding: 4px 14px;
        font-size: 0.82rem;
        color: #2e7d32;
        font-weight: 500;
        margin-top: 0.5rem;
    }
    .not-connected-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: #fff3e0;
        border: 1px solid #ffcc80;
        border-radius: 20px;
        padding: 4px 14px;
        font-size: 0.82rem;
        color: #e65100;
        font-weight: 500;
        margin-top: 0.5rem;
    }
    .success-banner {
        background: #e8f5e9;
        border: 1px solid #a5d6a7;
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: #2e7d32;
        font-weight: 500;
    }
    .info-banner {
        background: #e3f2fd;
        border: 1px solid #90caf9;
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 1rem;
        color: #1565c0;
    }
    .section-divider {
        border: none;
        border-top: 1.5px solid #e0e0e0;
        margin: 1.5rem 0;
    }
    .stButton > button { border-radius: 6px; font-weight: 500; }
    h1 { font-size: 1.9rem !important; }
    h2 { font-size: 1.3rem !important; }
</style>
""", unsafe_allow_html=True)


# ─── HELPERS ──────────────────────────────────────────────────────────────────
def get_client():
    key = st.session_state.get("api_key", "").strip()
    if not key:
        return None
    return anthropic.Anthropic(api_key=key)


def metadata_to_text(df: pd.DataFrame) -> str:
    return df.to_csv(sep="|", index=False)


def call_claude(client, system_prompt: str, user_content: str) -> str:
    with st.spinner("Calling Claude API — generating scripts..."):
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8000,
            system=system_prompt,
            messages=[{"role": "user", "content": user_content}],
        )
    return message.content[0].text


def split_scripts(raw_text: str) -> list[dict]:
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


def build_zip(scripts: list[dict], prefix: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        combined = []
        for s in scripts:
            filename = f"{s['label'].replace(' ', '_').replace('/', '_')}.qvs"
            zf.writestr(filename, s["script"])
            combined.append(f"// ===== TABLE: {s['label']} =====\n{s['script']}")
        zf.writestr(f"{prefix}_ALL_SCRIPTS.qvs", "\n\n".join(combined))
    return buf.getvalue()


def render_scripts(scripts: list[dict], prefix: str, key_prefix: str):
    if not scripts:
        return
    st.markdown(
        f'<div class="success-banner">✅ {len(scripts)} script{"s" if len(scripts) > 1 else ""} generated successfully</div>',
        unsafe_allow_html=True,
    )
    col1, _ = st.columns([1, 3])
    with col1:
        st.download_button(
            label=f"⬇ Download All ({len(scripts)} .qvs files)",
            data=build_zip(scripts, prefix),
            file_name=f"{prefix}_scripts_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            mime="application/zip",
            use_container_width=True,
        )
    st.divider()
    for i, s in enumerate(scripts):
        with st.expander(f"📄 {s['label']}", expanded=(i == 0)):
            col_a, col_b = st.columns([5, 1])
            with col_b:
                st.download_button(
                    label="⬇ .qvs",
                    data=s["script"],
                    file_name=f"{s['label'].replace(' ', '_')}.qvs",
                    mime="text/plain",
                    key=f"{key_prefix}_dl_{i}",
                    use_container_width=True,
                )
            with col_a:
                st.code(s["script"], language="sql")


# ─── HEADER ───────────────────────────────────────────────────────────────────
st.title("⚡ Qlik Sense ETL Script Generator")
st.markdown("Upload your **Metadata.xlsx** and auto-generate production-ready RAW & Intermediate QVD scripts using Claude AI.")

st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

# ─── STEP 1: API KEY (FRONT AND CENTER) ───────────────────────────────────────
st.markdown("### Step 1 — Enter your Anthropic API Key")

st.markdown('<div class="api-key-box">', unsafe_allow_html=True)

col_key, col_status = st.columns([3, 1])
with col_key:
    st.markdown('<div class="api-key-label">🔑 Anthropic API Key</div>', unsafe_allow_html=True)
    raw_key = st.text_input(
        label="api_key_input",
        type="password",
        placeholder="sk-ant-api03-xxxxxxxxxxxxxxxxxxxxxxxx",
        value=st.session_state.get("api_key", ""),
        label_visibility="collapsed",
        help="Your key is used only for this session and never stored. Get it at console.anthropic.com",
    )
    if raw_key:
        st.session_state["api_key"] = raw_key.strip()
    st.caption("🔒 Session-only — your key is never saved or sent anywhere except the Anthropic API.")

with col_status:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.session_state.get("api_key", "").strip():
        st.markdown('<div class="connected-badge">✓ Key entered</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="not-connected-badge">⚠ No key yet</div>', unsafe_allow_html=True)
        st.markdown(
            '<a href="https://console.anthropic.com/settings/keys" target="_blank" style="font-size:0.8rem;">Get API key →</a>',
            unsafe_allow_html=True,
        )

st.markdown('</div>', unsafe_allow_html=True)

# Gate: don't show the rest until key is entered
if not st.session_state.get("api_key", "").strip():
    st.info("👆 Enter your Anthropic API key above to unlock the script generator.")
    st.stop()

client = get_client()

# ─── STEP 2: METADATA INPUT ───────────────────────────────────────────────────
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.markdown("### Step 2 — Provide Metadata")

input_mode = st.radio(
    "Input method",
    ["📊 Upload Excel (Metadata.xlsx)", "📋 Paste Metadata (pipe-separated)"],
    horizontal=True,
    label_visibility="collapsed",
)
use_excel = "Excel" in input_mode

raw_df = int_df = None
raw_paste = int_paste = ""

if use_excel:
    uploaded = st.file_uploader(
        "Upload Metadata.xlsx",
        type=["xlsx", "xls"],
        help="Must have 'Raw' and 'Intermediate' sheets.",
    )
    if uploaded:
        try:
            xl = pd.ExcelFile(uploaded)
            sheet_names = xl.sheet_names
            st.markdown(
                f'<div class="info-banner">📂 <strong>{uploaded.name}</strong> loaded — sheets: {", ".join(sheet_names)}</div>',
                unsafe_allow_html=True,
            )
            raw_sheet = next((s for s in sheet_names if "raw" in s.lower()), sheet_names[0])
            int_sheet = next((s for s in sheet_names if "inter" in s.lower()), sheet_names[min(1, len(sheet_names) - 1)])

            raw_df = pd.read_excel(uploaded, sheet_name=raw_sheet).fillna("")
            int_df = pd.read_excel(uploaded, sheet_name=int_sheet).fillna("")

            col_p1, col_p2 = st.columns(2)
            with col_p1:
                with st.expander(f"👁 Preview RAW sheet ({len(raw_df)} rows)"):
                    st.dataframe(raw_df, use_container_width=True)
            with col_p2:
                with st.expander(f"👁 Preview Intermediate sheet ({len(int_df)} rows)"):
                    st.dataframe(int_df, use_container_width=True)
        except Exception as e:
            st.error(f"Failed to parse Excel: {e}")
else:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**RAW Layer Metadata**")
        raw_paste = st.text_area(
            "RAW metadata",
            height=180,
            placeholder="Layer|Table_Name|Source_Type|Source_Name|Source_Columns|Key_Columns|Load_Type|Incremental_Mode|Qlik_Target|Validation_Rules\nFact|fact_claims|SQL|claim_transaction_tbl|...",
            label_visibility="collapsed",
        )
    with col2:
        st.markdown("**Intermediate Layer Metadata**")
        int_paste = st.text_area(
            "INT metadata",
            height=180,
            placeholder="Layer|Table_Name|Source_Type|Source_QVDs|Key_Columns|Qlik_Target|Validation_Rules|Join_Mapping|Aggregate_Columns|Derived_Columns|Filter_Conditions|Transformations\nIntermediate|int_claims_summary|QVD|...",
            label_visibility="collapsed",
        )

# ─── STEP 3: GENERATE ─────────────────────────────────────────────────────────
st.markdown('<hr class="section-divider">', unsafe_allow_html=True)
st.markdown("### Step 3 — Generate Scripts")

tab_raw, tab_int, tab_both = st.tabs(["🗄 RAW Layer", "🔄 Intermediate Layer", "⚡ Generate Both"])

# ── RAW TAB ───────────────────────────────────────────────────────────────────
with tab_raw:
    st.markdown("Extracts from SQL source tables into QVDs. Supports **Full Load**, **Incremental Insert**, and **Incremental Upsert** — with validation and audit logging.")
    col_r1, col_r2 = st.columns([3, 1])
    with col_r1:
        if raw_df is not None:
            st.markdown(f"Ready: **{len(raw_df)} table(s)** from RAW sheet")
        elif raw_paste.strip():
            st.markdown("Ready: pasted metadata detected")
        else:
            st.markdown("Provide metadata above first.")
    with col_r2:
        gen_raw = st.button("Generate RAW Scripts ▶", type="primary", use_container_width=True, key="btn_raw")

    if gen_raw:
        meta = metadata_to_text(raw_df) if (use_excel and raw_df is not None) else raw_paste
        if not meta or not meta.strip():
            st.error("No RAW metadata found. Please upload Excel or paste metadata.")
        else:
            try:
                result = call_claude(client, RAW_SYSTEM_PROMPT,
                    f"Generate RAW layer Qlik scripts for the following metadata:\n\n{meta}")
                st.session_state["raw_scripts"] = split_scripts(result)
            except Exception as e:
                st.error(f"API Error: {e}")

    if "raw_scripts" in st.session_state:
        render_scripts(st.session_state["raw_scripts"], "RAW", "raw")

# ── INTERMEDIATE TAB ──────────────────────────────────────────────────────────
with tab_int:
    st.markdown("Joins RAW QVDs and applies aggregations, derived fields, and transformations (Financial Year, approval ratios, policy durations, etc.).")
    col_i1, col_i2 = st.columns([3, 1])
    with col_i1:
        if int_df is not None:
            st.markdown(f"Ready: **{len(int_df)} table(s)** from Intermediate sheet")
        elif int_paste.strip():
            st.markdown("Ready: pasted metadata detected")
        else:
            st.markdown("Provide metadata above first.")
    with col_i2:
        gen_int = st.button("Generate INT Scripts ▶", type="primary", use_container_width=True, key="btn_int")

    if gen_int:
        meta = metadata_to_text(int_df) if (use_excel and int_df is not None) else int_paste
        if not meta or not meta.strip():
            st.error("No Intermediate metadata found. Please upload Excel or paste metadata.")
        else:
            try:
                result = call_claude(client, INT_SYSTEM_PROMPT,
                    f"Generate Intermediate layer Qlik scripts for the following metadata:\n\n{meta}")
                st.session_state["int_scripts"] = split_scripts(result)
            except Exception as e:
                st.error(f"API Error: {e}")

    if "int_scripts" in st.session_state:
        render_scripts(st.session_state["int_scripts"], "INT", "int")

# ── BOTH TAB ──────────────────────────────────────────────────────────────────
with tab_both:
    st.markdown("Runs both RAW and Intermediate generation in sequence and bundles everything into a single ZIP.")

    gen_both = st.button("⚡ Generate All Scripts", type="primary", key="btn_both")

    if gen_both:
        # RAW
        with st.status("Generating RAW layer scripts...", expanded=True) as s_raw:
            raw_meta = metadata_to_text(raw_df) if (use_excel and raw_df is not None) else raw_paste
            if raw_meta and raw_meta.strip():
                try:
                    res = call_claude(client, RAW_SYSTEM_PROMPT,
                        f"Generate RAW layer Qlik scripts for the following metadata:\n\n{raw_meta}")
                    st.session_state["raw_scripts"] = split_scripts(res)
                    s_raw.update(label=f"✅ RAW done — {len(st.session_state['raw_scripts'])} scripts", state="complete")
                except Exception as e:
                    s_raw.update(label=f"❌ RAW failed: {e}", state="error")
            else:
                s_raw.update(label="⚠ No RAW metadata — skipped", state="complete")

        # Intermediate
        with st.status("Generating Intermediate layer scripts...", expanded=True) as s_int:
            int_meta = metadata_to_text(int_df) if (use_excel and int_df is not None) else int_paste
            if int_meta and int_meta.strip():
                try:
                    res = call_claude(client, INT_SYSTEM_PROMPT,
                        f"Generate Intermediate layer Qlik scripts for the following metadata:\n\n{int_meta}")
                    st.session_state["int_scripts"] = split_scripts(res)
                    s_int.update(label=f"✅ Intermediate done — {len(st.session_state['int_scripts'])} scripts", state="complete")
                except Exception as e:
                    s_int.update(label=f"❌ Intermediate failed: {e}", state="error")
            else:
                s_int.update(label="⚠ No Intermediate metadata — skipped", state="complete")

    raw_ok = bool(st.session_state.get("raw_scripts"))
    int_ok = bool(st.session_state.get("int_scripts"))

    if raw_ok or int_ok:
        st.divider()
        all_scripts = []
        if raw_ok:
            all_scripts += [{"label": f"RAW_{s['label']}", "script": s["script"]} for s in st.session_state["raw_scripts"]]
        if int_ok:
            all_scripts += [{"label": f"INT_{s['label']}", "script": s["script"]} for s in st.session_state["int_scripts"]]

        c1, c2, c3 = st.columns(3)
        c1.metric("RAW Scripts", len(st.session_state.get("raw_scripts", [])))
        c2.metric("INT Scripts", len(st.session_state.get("int_scripts", [])))
        c3.metric("Total Scripts", len(all_scripts))

        st.download_button(
            label=f"⬇ Download All {len(all_scripts)} Scripts (.zip)",
            data=build_zip(all_scripts, "QLIK_ETL"),
            file_name=f"QLIK_ETL_ALL_{datetime.now().strftime('%Y%m%d_%H%M')}.zip",
            mime="application/zip",
        )
        if raw_ok:
            st.markdown("#### RAW Scripts")
            render_scripts(st.session_state["raw_scripts"], "RAW", "both_raw")
        if int_ok:
            st.markdown("#### Intermediate Scripts")
            render_scripts(st.session_state["int_scripts"], "INT", "both_int")
