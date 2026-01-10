# =========================================================
# Bake-Off Planer (MDE-Style) – Frontend (Streamlit) + Backend (Google Sheets)
#
# ✅ Mitarbeiter arbeitet NUR im Frontend:
#   TAB 1: Backvorschlag (HEUTE) -> Vorschlag sehen/überschreiben + Ist-Backen eintragen
#   TAB 2: Abschriften (GESTERN) -> Abschrift eintragen + Abschluss & Lernen
#   TAB 3: Dashboard
#
# ✅ Kein Datum auswählen. Keine Verwirrung:
#   Backen = HEUTE
#   Abschrift = GESTERN
#
# ✅ Lernen berücksichtigt Override:
#   - suggested_* = Systemvorschlag (eingefroren, sobald gespeichert)
#   - override_total = Mitarbeiter-Änderung (wichtig fürs Lernen & Auswertung)
#   - baked_* = Ist-Backen (heute bekannt)
#   - waste_qty / early_empty = Abschrift + OOS-Signal (erst am Folgetag)
#
# Google Sheet Tabs (werden automatisch angelegt):
# - articles:
#     sku | name | active | created_at
# - bake_log:
#     date | sku | suggested_total | suggested_morning | suggested_afternoon |
#     override_total | baked_morning | baked_afternoon | note | updated_at
# - waste_log:
#     date | sku | waste_qty | early_empty | closed | updated_at
# - demand_model:
#     sku | weekday | demand | morning_share | waste_rate | updated_at
#
# Streamlit Secrets (in Streamlit Cloud -> Settings -> Secrets):
# SHEET_ID = "..."
# [gcp_service_account]
# type="service_account"
# project_id="..."
# private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
# client_email="..."
# ... (restliche Felder)
# =========================================================

import time
from datetime import date, timedelta, datetime as dt

import numpy as np
import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# -------------------------
# App config
# -------------------------
st.set_page_config(page_title="Bake-Off Planer (MDE)", layout="wide")

# Lern-Parameter (MVP)
ALPHA = 0.22
START_DEMAND = 20.0
START_MORNING_SHARE = 0.78
START_WASTE_RATE = 0.08
TARGET_WASTE = 0.06

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
CACHE_TTL_SEC = 90

REQUIRED_TABS = {
    "articles": ["sku", "name", "active", "created_at"],
    "bake_log": [
        "date", "sku",
        "suggested_total", "suggested_morning", "suggested_afternoon",
        "override_total", "baked_morning", "baked_afternoon",
        "note", "updated_at",
    ],
    "waste_log": ["date", "sku", "waste_qty", "early_empty", "closed", "updated_at"],
    "demand_model": ["sku", "weekday", "demand", "morning_share", "waste_rate", "updated_at"],
}

# -------------------------
# Utils
# -------------------------
def now_iso() -> str:
    return dt.utcnow().isoformat()

def clean_sku(x) -> str:
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    return s

def weekday_name(d: date) -> str:
    return pd.to_datetime(d.isoformat()).day_name()

def parse_bool(x) -> bool:
    return str(x).strip().lower() in ("true", "1", "yes", "ja")

def clamp(x, lo, hi) -> float:
    return float(np.clip(float(x), float(lo), float(hi)))

def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy() if not df.empty else pd.DataFrame()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    return out

# -------------------------
# Google Sheets – stable layer
# -------------------------
@st.cache_resource
def gs_client():
    if "gcp_service_account" not in st.secrets:
        st.error("Streamlit Secrets fehlen: [gcp_service_account].")
        st.stop()
    if "SHEET_ID" not in st.secrets:
        st.error("Streamlit Secrets fehlen: SHEET_ID.")
        st.stop()

    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

@st.cache_resource
def open_spreadsheet():
    return gs_client().open_by_key(str(st.secrets["SHEET_ID"]).strip())

def retry(fn, tries=7, base_sleep=0.6):
    last = None
    for i in range(tries):
        try:
            return fn()
        except APIError as e:
            last = e
            time.sleep(min(base_sleep * (2 ** i), 8.0))
    raise last

@st.cache_resource
def ensure_schema_once():
    """
    WICHTIG: nur 1x pro App-Lauf, sonst entstehen viele worksheets()-Calls -> APIError.
    """
    sh = open_spreadsheet()

    def _ensure():
        titles = [w.title for w in retry(lambda: sh.worksheets())]
        for tab, headers in REQUIRED_TABS.items():
            if tab not in titles:
                retry(lambda: sh.add_worksheet(title=tab, rows=4000, cols=max(12, len(headers) + 2)))
            ws = sh.worksheet(tab)
            row1 = retry(lambda: ws.row_values(1))
            if [x.strip() for x in row1[: len(headers)]] != headers:
                retry(lambda: ws.clear())
                retry(lambda: ws.update([headers]))

    retry(_ensure)
    return True

def ws(tab: str):
    ensure_schema_once()
    return open_spreadsheet().worksheet(tab)

@st.cache_data(ttl=CACHE_TTL_SEC)
def load_all(sheet_id: str) -> dict:
    out = {}
    for tab, headers in REQUIRED_TABS.items():
        w = ws(tab)
        values = retry(lambda: w.get_all_values())
        if not values:
            out[tab] = pd.DataFrame(columns=headers)
            continue
        hdr = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=hdr if hdr else headers)
        df = ensure_cols(df, headers)
        out[tab] = df
    return out

def invalidate_cache():
    load_all.clear()

def upsert_rows(tab: str, key_cols: list[str], rows: list[dict]):
    """
    Upsert mit 1x read + batch_update + append. Kein "clear & rewrite".
    """
    if not rows:
        return

    w = ws(tab)
    headers = REQUIRED_TABS[tab]

    values = retry(lambda: w.get_all_values())
    if not values:
        retry(lambda: w.update([headers]))
        values = [headers]

    if values[0][: len(headers)] != headers:
        retry(lambda: w.clear())
        retry(lambda: w.update([headers]))
        values = [headers]

    # key -> row index (2..)
    key_to_row = {}
    for idx, r in enumerate(values[1:], start=2):
        row_map = {headers[j]: (r[j] if j < len(r) else "") for j in range(len(headers))}
        key = tuple(str(row_map.get(k, "")).strip() for k in key_cols)
        if all(key):
            key_to_row[key] = idx

    updates = []
    appends = []

    end_col = chr(ord("A") + len(headers) - 1)

    for row in rows:
        row_norm = {h: row.get(h, "") for h in headers}
        for k in key_cols:
            row_norm[k] = str(row_norm.get(k, "")).strip()
        key = tuple(row_norm[k] for k in key_cols)
        row_values = [row_norm[h] for h in headers]

        if all(key) and key in key_to_row:
            rix = key_to_row[key]
            rng = f"A{rix}:{end_col}{rix}"
            updates.append({"range": rng, "values": [row_values]})
        else:
            appends.append(row_values)

    if updates:
        retry(lambda: w.batch_update(updates))
    for av in appends:
        retry(lambda: w.append_row(av, value_input_option="USER_ENTERED"))

    invalidate_cache()

# -------------------------
# Model + recommendation logic
# -------------------------
def ensure_model_for_active(model: pd.DataFrame, skus: list[str]) -> pd.DataFrame:
    model = ensure_cols(model, REQUIRED_TABS["demand_model"])
    if not skus:
        return model

    base = pd.MultiIndex.from_product([skus, WEEKDAYS], names=["sku", "weekday"]).to_frame(index=False)
    out = base.merge(model, on=["sku", "weekday"], how="left")

    out["demand"] = pd.to_numeric(out["demand"], errors="coerce").fillna(START_DEMAND)
    out["morning_share"] = pd.to_numeric(out["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
    out["waste_rate"] = pd.to_numeric(out["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)
    out["updated_at"] = out["updated_at"].fillna("")
    out["sku"] = out["sku"].astype(str)
    out["weekday"] = out["weekday"].astype(str)
    return out

def recommend_total(demand: float, waste_rate: float) -> int:
    d = max(0.0, float(demand))
    wr = clamp(waste_rate, 0.0, 1.0)
    penalty = wr - TARGET_WASTE
    adj = 1.0 - 0.70 * max(0.0, penalty) + 0.15 * max(0.0, -penalty)
    adj = clamp(adj, 0.70, 1.20)
    return int(np.round(d * adj))

def split_qty(total: int, morning_share: float) -> tuple[int, int]:
    total = max(0, int(total))
    ms = clamp(morning_share, 0.55, 0.95)
    m = int(np.round(total * ms))
    a = max(0, total - m)
    if a <= 2:
        return total, 0
    return m, a

def learn_from_yesterday(model: pd.DataFrame, y: pd.DataFrame, wd_y: str) -> pd.DataFrame:
    """
    y columns: sku, baked_total, waste_qty, early_empty
    demand <- EMA(baked_total - waste)
    waste_rate <- EMA(waste/baked)
    morning_share <- EMA(adjustment via early_empty / high waste)
    """
    out = model.copy()
    out["demand"] = pd.to_numeric(out["demand"], errors="coerce").fillna(START_DEMAND)
    out["morning_share"] = pd.to_numeric(out["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
    out["waste_rate"] = pd.to_numeric(out["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)

    y = y.copy()
    y["baked_total"] = pd.to_numeric(y["baked_total"], errors="coerce").fillna(0.0)
    y["waste_qty"] = pd.to_numeric(y["waste_qty"], errors="coerce").fillna(0.0)
    y["early_empty"] = y["early_empty"].astype(bool)

    for _, r in y.iterrows():
        sku = clean_sku(r["sku"])
        if not sku:
            continue

        baked = float(r["baked_total"])
        waste = float(r["waste_qty"])
        sold_est = max(0.0, baked - waste)
        wr_obs = (waste / baked) if baked > 0 else 0.0
        wr_obs = clamp(wr_obs, 0.0, 1.0)

        mask = (out["sku"].astype(str) == sku) & (out["weekday"].astype(str) == wd_y)
        if not mask.any():
            continue
        i = out.index[mask][0]

        old_d = float(out.at[i, "demand"])
        old_ms = float(out.at[i, "morning_share"])
        old_wr = float(out.at[i, "waste_rate"])

        new_d = (1 - ALPHA) * old_d + ALPHA * sold_est
        new_wr = (1 - ALPHA) * old_wr + ALPHA * wr_obs
        new_wr = clamp(new_wr, 0.0, 1.0)

        # morning share target
        ms_target = old_ms
        if bool(r["early_empty"]):
            ms_target = min(0.95, old_ms + 0.06)
        else:
            if new_wr >= 0.12:
                ms_target = max(0.55, old_ms - 0.04)

        new_ms = (1 - ALPHA) * old_ms + ALPHA * ms_target
        new_ms = clamp(new_ms, 0.55, 0.95)

        out.at[i, "demand"] = max(0.0, float(new_d))
        out.at[i, "waste_rate"] = float(new_wr)
        out.at[i, "morning_share"] = float(new_ms)
        out.at[i, "updated_at"] = now_iso()

    return out

# =========================================================
# UI START
# =========================================================
st.title("🥐 Bake-Off Planer (MDE)")

c_top1, c_top2 = st.columns([5, 1])
with c_top2:
    if st.button("🔄 Neu laden", key="btn_reload_top"):
        invalidate_cache()
        st.rerun()

sheet_id = str(st.secrets["SHEET_ID"]).strip()
tabs = load_all(sheet_id)

articles = ensure_cols(tabs["articles"], REQUIRED_TABS["articles"])
bake_log = ensure_cols(tabs["bake_log"], REQUIRED_TABS["bake_log"])
waste_log = ensure_cols(tabs["waste_log"], REQUIRED_TABS["waste_log"])
model = ensure_cols(tabs["demand_model"], REQUIRED_TABS["demand_model"])

# Clean articles
articles["sku"] = articles["sku"].astype(str).map(clean_sku)
articles = articles[articles["sku"] != ""].copy()
articles["name"] = articles["name"].astype(str)
articles["active"] = articles["active"].astype(str)

active = articles[articles["active"].str.lower().isin(["true", "1", "yes", "ja"])].copy()
active = active.sort_values("name")
active_skus = [clean_sku(x) for x in active["sku"].tolist() if clean_sku(x)]

# Artikelverwaltung (klein)
with st.expander("🧺 Artikel verwalten (selten)", expanded=(len(active_skus) == 0)):
    a1, a2, a3 = st.columns([1, 2, 1])
    with a1:
        new_sku = st.text_input("PLU / Artikelnummer", value="", key="new_sku")
    with a2:
        new_name = st.text_input("Artikelname", value="", key="new_name")
    with a3:
        new_active = st.checkbox("Aktiv", value=True, key="new_active")

    if st.button("➕ Artikel speichern", key="btn_add_article"):
        if not new_sku.strip() or not new_name.strip():
            st.warning("Bitte PLU und Artikelname ausfüllen.")
        else:
            upsert_rows(
                "articles",
                ["sku"],
                [{
                    "sku": new_sku.strip(),
                    "name": new_name.strip(),
                    "active": "TRUE" if bool(new_active) else "FALSE",
                    "created_at": now_iso(),
                }],
            )
            st.success("Artikel gespeichert.")
            st.rerun()

    if not articles.empty:
        ui = articles.copy()
        ui["active"] = ui["active"].str.lower().isin(["true", "1", "yes", "ja"])
        edited = st.data_editor(
            ui[["sku", "name", "active"]],
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="editor_articles",
        )
        if st.button("💾 Aktiv-Status übernehmen", key="btn_save_active"):
            rows = []
            for _, r in edited.iterrows():
                sku = clean_sku(r["sku"])
                if not sku:
                    continue
                rows.append({
                    "sku": sku,
                    "name": str(r["name"]),
                    "active": "TRUE" if bool(r["active"]) else "FALSE",
                    "created_at": now_iso(),
                })
            upsert_rows("articles", ["sku"], rows)
            st.success("Gespeichert.")
            st.rerun()

# After possible changes, reload
tabs = load_all(sheet_id)
articles = ensure_cols(tabs["articles"], REQUIRED_TABS["articles"])
articles["sku"] = articles["sku"].astype(str).map(clean_sku)
articles = articles[articles["sku"] != ""].copy()
articles["name"] = articles["name"].astype(str)
articles["active"] = articles["active"].astype(str)

active = articles[articles["active"].str.lower().isin(["true", "1", "yes", "ja"])].copy().sort_values("name")
active_skus = [clean_sku(x) for x in active["sku"].tolist() if clean_sku(x)]

if not active_skus:
    st.warning("Bitte mindestens 1 Artikel anlegen/aktivieren.")
    st.stop()

# Ensure model completeness; write back only if missing rows
model = ensure_cols(tabs["demand_model"], REQUIRED_TABS["demand_model"])
model_full = ensure_model_for_active(model, active_skus)
if model_full.shape[0] != model.shape[0]:
    rows = []
    for _, r in model_full.iterrows():
        rows.append({
            "sku": str(r["sku"]),
            "weekday": str(r["weekday"]),
            "demand": float(r["demand"]),
            "morning_share": float(r["morning_share"]),
            "waste_rate": float(r["waste_rate"]),
            "updated_at": str(r.get("updated_at", "") or ""),
        })
    upsert_rows("demand_model", ["sku", "weekday"], rows)
    tabs = load_all(sheet_id)
    model = ensure_cols(tabs["demand_model"], REQUIRED_TABS["demand_model"])
    model_full = ensure_model_for_active(model, active_skus)

# Dates
today = date.today()
yesterday = today - timedelta(days=1)
today_s = today.isoformat()
yesterday_s = yesterday.isoformat()
wd_today = weekday_name(today)
wd_yesterday = weekday_name(yesterday)

# =========================
# TABS (MDE)
# =========================
tab1, tab2, tab3 = st.tabs(["📌 Backvorschlag (Heute)", "🧾 Abschriften (Gestern)", "📊 Dashboard"])

# =========================================================
# TAB 1: Backvorschlag heute + Überschreiben + Ist Backen
# =========================================================
with tab1:
    st.subheader("Backvorschlag (Heute)")
    st.caption("Vorschlag ansehen, ggf. überschreiben, und Ist-Backen eintragen. "
               "Der Vorschlag wird eingefroren, sobald du speicherst.")

    bake_log = ensure_cols(tabs["bake_log"], REQUIRED_TABS["bake_log"])
    bake_log["date"] = bake_log["date"].astype(str)
    bake_log["sku"] = bake_log["sku"].astype(str).map(clean_sku)
    b_today = bake_log[bake_log["date"] == today_s].copy()

    # Model for today
    m_today = model_full[model_full["weekday"].astype(str) == wd_today].copy()
    m_today = m_today.merge(active[["sku", "name"]], on="sku", how="inner")
    m_today["demand"] = pd.to_numeric(m_today["demand"], errors="coerce").fillna(START_DEMAND)
    m_today["morning_share"] = pd.to_numeric(m_today["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
    m_today["waste_rate"] = pd.to_numeric(m_today["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)

    # Current suggestion (if never saved today)
    m_today["suggested_total"] = m_today.apply(lambda r: recommend_total(r["demand"], r["waste_rate"]), axis=1)
    sp = m_today.apply(lambda r: split_qty(int(r["suggested_total"]), float(r["morning_share"])), axis=1)
    m_today["suggested_morning"] = [a for a, b in sp]
    m_today["suggested_afternoon"] = [b for a, b in sp]

    # Merge saved state to freeze suggestion once saved
    view = m_today.merge(
        b_today[[
            "sku",
            "suggested_total", "suggested_morning", "suggested_afternoon",
            "override_total", "baked_morning", "baked_afternoon", "note"
        ]],
        on="sku",
        how="left",
        suffixes=("", "_saved"),
    )

    # Freeze suggestion if saved values exist
    for c in ["suggested_total", "suggested_morning", "suggested_afternoon"]:
        view[c] = pd.to_numeric(view[c], errors="coerce")
        saved = pd.to_numeric(view.get(c + "_saved"), errors="coerce")
        view[c] = saved.where(saved.notna(), view[c])
        if c + "_saved" in view.columns:
            view.drop(columns=[c + "_saved"], inplace=True)

    view["override_total"] = pd.to_numeric(view["override_total"], errors="coerce").fillna(view["suggested_total"]).astype(int)
    view["baked_morning"] = pd.to_numeric(view["baked_morning"], errors="coerce").fillna(0).astype(int)
    view["baked_afternoon"] = pd.to_numeric(view["baked_afternoon"], errors="coerce").fillna(0).astype(int)
    view["note"] = view["note"].fillna("")

    # Filters (keys unique!)
    f1, f2, f3 = st.columns([2, 1, 1])
    with f1:
        q_today = st.text_input("Suche", value="", key="q_today")
    with f2:
        only_pos_today = st.checkbox("Nur Vorschlag > 0", value=True, key="only_pos_today")
    with f3:
        only_edited_today = st.checkbox("Nur bearbeitet", value=False, key="only_edited_today")

    v = view.copy()
    if q_today.strip():
        qq = q_today.strip().lower()
        v = v[v["name"].str.lower().str.contains(qq) | v["sku"].str.lower().str.contains(qq)]
    if only_pos_today:
        v = v[v["suggested_total"] > 0]
    if only_edited_today:
        v = v[
            (v["override_total"] != v["suggested_total"]) |
            ((v["baked_morning"] + v["baked_afternoon"]) > 0) |
            (v["note"].astype(str).str.len() > 0)
        ]

    editor_today = st.data_editor(
        v[[
            "sku", "name",
            "suggested_total", "suggested_morning", "suggested_afternoon",
            "override_total",
            "baked_morning", "baked_afternoon",
            "note",
        ]],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key="editor_today",
        column_config={
            "suggested_total": st.column_config.NumberColumn("Vorschlag gesamt", disabled=True),
            "suggested_morning": st.column_config.NumberColumn("Vorschlag morgens", disabled=True),
            "suggested_afternoon": st.column_config.NumberColumn("Vorschlag nachm.", disabled=True),
            "override_total": st.column_config.NumberColumn("Überschreiben (gesamt)", min_value=0, step=1),
            "baked_morning": st.column_config.NumberColumn("Ist gebacken morgens", min_value=0, step=1),
            "baked_afternoon": st.column_config.NumberColumn("Ist gebacken nachm.", min_value=0, step=1),
            "note": st.column_config.TextColumn("Notiz"),
        }
    )

    if st.button("💾 Heute speichern", type="primary", key="btn_save_today"):
        base = view.set_index("sku")
        ed = editor_today.copy()
        ed["sku"] = ed["sku"].astype(str).map(clean_sku)
        ed = ed[ed["sku"] != ""].set_index("sku")
        common = base.index.intersection(ed.index)

        base.loc[common, "override_total"] = pd.to_numeric(ed.loc[common, "override_total"], errors="coerce").fillna(base.loc[common, "override_total"]).astype(int)
        base.loc[common, "baked_morning"] = pd.to_numeric(ed.loc[common, "baked_morning"], errors="coerce").fillna(0).astype(int)
        base.loc[common, "baked_afternoon"] = pd.to_numeric(ed.loc[common, "baked_afternoon"], errors="coerce").fillna(0).astype(int)
        base.loc[common, "note"] = ed.loc[common, "note"].fillna("").astype(str)

        base = base.reset_index()

        rows = []
        for _, r in base.iterrows():
            rows.append({
                "date": today_s,
                "sku": str(r["sku"]),
                "suggested_total": int(r["suggested_total"]),
                "suggested_morning": int(r["suggested_morning"]),
                "suggested_afternoon": int(r["suggested_afternoon"]),
                "override_total": int(r["override_total"]),
                "baked_morning": int(r["baked_morning"]),
                "baked_afternoon": int(r["baked_afternoon"]),
                "note": str(r.get("note", "") or ""),
                "updated_at": now_iso(),
            })
        upsert_rows("bake_log", ["date", "sku"], rows)
        st.success("Heute gespeichert ✅")
        st.rerun()

# =========================================================
# TAB 2: Gestern Abschrift + Abschluss & Lernen
# =========================================================
with tab2:
    st.subheader("Abschriften (Gestern)")
    st.caption("Hier trägst du ein, was **gestern** abgeschrieben wurde. "
               "Danach: Abschluss & Lernen -> Vorschläge werden besser.")

    bake_log = ensure_cols(load_all(sheet_id)["bake_log"], REQUIRED_TABS["bake_log"])
    waste_log = ensure_cols(load_all(sheet_id)["waste_log"], REQUIRED_TABS["waste_log"])

    bake_log["date"] = bake_log["date"].astype(str)
    bake_log["sku"] = bake_log["sku"].astype(str).map(clean_sku)
    b_y = bake_log[bake_log["date"] == yesterday_s].copy()
    b_y["baked_morning"] = pd.to_numeric(b_y["baked_morning"], errors="coerce").fillna(0).astype(int)
    b_y["baked_afternoon"] = pd.to_numeric(b_y["baked_afternoon"], errors="coerce").fillna(0).astype(int)
    b_y["baked_total"] = b_y["baked_morning"] + b_y["baked_afternoon"]
    b_y["override_total"] = pd.to_numeric(b_y["override_total"], errors="coerce").fillna(np.nan)

    waste_log["date"] = waste_log["date"].astype(str)
    waste_log["sku"] = waste_log["sku"].astype(str).map(clean_sku)
    w_y = waste_log[waste_log["date"] == yesterday_s].copy()

    if w_y.empty:
        w_y = pd.DataFrame(columns=REQUIRED_TABS["waste_log"])

    w_y["waste_qty"] = pd.to_numeric(w_y.get("waste_qty", 0), errors="coerce").fillna(0).astype(int)
    w_y["early_empty"] = w_y.get("early_empty", "FALSE").apply(parse_bool)
    w_y["closed"] = w_y.get("closed", "FALSE").apply(parse_bool)

    base = active[["sku", "name"]].copy()
    base = base.merge(b_y[["sku", "baked_total", "override_total"]], on="sku", how="left")
    base = base.merge(w_y[["sku", "waste_qty", "early_empty", "closed"]], on="sku", how="left")

    base["baked_total"] = pd.to_numeric(base["baked_total"], errors="coerce").fillna(0).astype(int)
    base["override_total"] = pd.to_numeric(base["override_total"], errors="coerce")
    base["waste_qty"] = pd.to_numeric(base["waste_qty"], errors="coerce").fillna(0).astype(int)
    base["early_empty"] = base["early_empty"].fillna(False).astype(bool)
    base["closed"] = base["closed"].fillna(False).astype(bool)

    if bool(base["closed"].all()) and len(base) > 0:
        st.success("Gestern ist bereits abgeschlossen ✅ (du kannst korrigieren und erneut abschließen).")

    # Filters (unique keys)
    g1, g2, g3 = st.columns([2, 1, 1])
    with g1:
        q_y = st.text_input("Suche", value="", key="q_yesterday")
    with g2:
        only_baked_y = st.checkbox("Nur mit Backen", value=True, key="only_baked_yesterday")
    with g3:
        only_edited_y = st.checkbox("Nur bearbeitet", value=False, key="only_edited_yesterday")

    v = base.copy()
    if q_y.strip():
        qq = q_y.strip().lower()
        v = v[v["name"].str.lower().str.contains(qq) | v["sku"].str.lower().str.contains(qq)]
    if only_baked_y:
        v = v[v["baked_total"] > 0]
    if only_edited_y:
        v = v[(v["waste_qty"] > 0) | (v["early_empty"] == True)]

    editor_y = st.data_editor(
        v[["sku", "name", "baked_total", "override_total", "waste_qty", "early_empty"]],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key="editor_yesterday",
        column_config={
            "baked_total": st.column_config.NumberColumn("Gestern gebacken (Info)", disabled=True),
            "override_total": st.column_config.NumberColumn("Gestern Override (Info)", disabled=True),
            "waste_qty": st.column_config.NumberColumn("Gestern Abschrift", min_value=0, step=1),
            "early_empty": st.column_config.CheckboxColumn("Vor 14 Uhr leer"),
        },
    )

    s1, s2 = st.columns([1, 2])
    do_save = s1.button("💾 Speichern", key="btn_save_yesterday")
    do_close = s2.button("✅ Abschluss & Lernen", type="primary", key="btn_close_learn")

    if do_save or do_close:
        b0 = base.set_index("sku")
        ed = editor_y.copy()
        ed["sku"] = ed["sku"].astype(str).map(clean_sku)
        ed = ed[ed["sku"] != ""].set_index("sku")
        common = b0.index.intersection(ed.index)

        b0.loc[common, "waste_qty"] = pd.to_numeric(ed.loc[common, "waste_qty"], errors="coerce").fillna(0).astype(int)
        b0.loc[common, "early_empty"] = ed.loc[common, "early_empty"].astype(bool)
        if do_close:
            b0["closed"] = True
        b0 = b0.reset_index()

        rows = []
        for _, r in b0.iterrows():
            rows.append({
                "date": yesterday_s,
                "sku": str(r["sku"]),
                "waste_qty": int(r["waste_qty"]),
                "early_empty": "TRUE" if bool(r["early_empty"]) else "FALSE",
                "closed": "TRUE" if bool(r["closed"]) else "FALSE",
                "updated_at": now_iso(),
            })
        upsert_rows("waste_log", ["date", "sku"], rows)

        if do_close:
            # Reload fresh for learning
            invalidate_cache()
            tabsN = load_all(sheet_id)
            bakeN = ensure_cols(tabsN["bake_log"], REQUIRED_TABS["bake_log"])
            wasteN = ensure_cols(tabsN["waste_log"], REQUIRED_TABS["waste_log"])
            modelN = ensure_cols(tabsN["demand_model"], REQUIRED_TABS["demand_model"])
            modelN_full = ensure_model_for_active(modelN, active_skus)

            # yesterday merged rows
            bakeN["date"] = bakeN["date"].astype(str)
            bakeN["sku"] = bakeN["sku"].astype(str).map(clean_sku)
            by = bakeN[bakeN["date"] == yesterday_s].copy()
            by["baked_morning"] = pd.to_numeric(by["baked_morning"], errors="coerce").fillna(0).astype(int)
            by["baked_afternoon"] = pd.to_numeric(by["baked_afternoon"], errors="coerce").fillna(0).astype(int)
            by["baked_total"] = by["baked_morning"] + by["baked_afternoon"]

            wasteN["date"] = wasteN["date"].astype(str)
            wasteN["sku"] = wasteN["sku"].astype(str).map(clean_sku)
            wy = wasteN[wasteN["date"] == yesterday_s].copy()
            wy["waste_qty"] = pd.to_numeric(wy["waste_qty"], errors="coerce").fillna(0).astype(int)
            wy["early_empty"] = wy["early_empty"].apply(parse_bool)

            y_rows = active[["sku"]].merge(by[["sku", "baked_total"]], on="sku", how="left").merge(
                wy[["sku", "waste_qty", "early_empty"]], on="sku", how="left"
            )
            y_rows["baked_total"] = pd.to_numeric(y_rows["baked_total"], errors="coerce").fillna(0).astype(int)
            y_rows["waste_qty"] = pd.to_numeric(y_rows["waste_qty"], errors="coerce").fillna(0).astype(int)
            y_rows["early_empty"] = y_rows["early_empty"].fillna(False).astype(bool)

            # Snapshot BEFORE for delta
            before = modelN_full.copy()

            after = learn_from_yesterday(modelN_full, y_rows, wd_yesterday)

            # write model back
            rowsM = []
            for _, r in after.iterrows():
                rowsM.append({
                    "sku": str(r["sku"]),
                    "weekday": str(r["weekday"]),
                    "demand": float(pd.to_numeric(r["demand"], errors="coerce") or START_DEMAND),
                    "morning_share": float(pd.to_numeric(r["morning_share"], errors="coerce") or START_MORNING_SHARE),
                    "waste_rate": float(pd.to_numeric(r["waste_rate"], errors="coerce") or START_WASTE_RATE),
                    "updated_at": str(r.get("updated_at", "") or ""),
                })
            upsert_rows("demand_model", ["sku", "weekday"], rowsM)

            # Show change in today's recommendation
            def rec_table(mdf: pd.DataFrame) -> pd.DataFrame:
                mt = mdf[mdf["weekday"].astype(str) == wd_today].copy()
                mt = mt.merge(active[["sku", "name"]], on="sku", how="inner")
                mt["demand"] = pd.to_numeric(mt["demand"], errors="coerce").fillna(START_DEMAND)
                mt["morning_share"] = pd.to_numeric(mt["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
                mt["waste_rate"] = pd.to_numeric(mt["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)
                mt["total"] = mt.apply(lambda r: recommend_total(r["demand"], r["waste_rate"]), axis=1)
                sp2 = mt.apply(lambda r: split_qty(int(r["total"]), float(r["morning_share"])), axis=1)
                mt["m"] = [a for a, b in sp2]
                mt["a"] = [b for a, b in sp2]
                return mt[["sku", "name", "m", "a"]]

            after_reload = ensure_model_for_active(load_all(sheet_id)["demand_model"], active_skus)
            bR = rec_table(before).rename(columns={"m": "vor_m", "a": "vor_a"})
            aR = rec_table(after_reload).rename(columns={"m": "neu_m", "a": "neu_a"})
            delta = bR.merge(aR, on=["sku", "name"], how="inner")
            delta["Δ morgens"] = delta["neu_m"] - delta["vor_m"]
            delta["Δ nachm"] = delta["neu_a"] - delta["vor_a"]
            delta = delta.sort_values(["Δ morgens", "Δ nachm"], ascending=False)

            st.success("Abschluss gespeichert ✅ App hat gelernt. Empfehlungen wurden aktualisiert.")
            with st.expander("Änderung in der Empfehlung (heute)", expanded=True):
                st.dataframe(delta[["name", "vor_m", "neu_m", "Δ morgens", "vor_a", "neu_a", "Δ nachm"]],
                             use_container_width=True, hide_index=True)
        else:
            st.success("Gespeichert ✅")

        st.rerun()

# =========================================================
# TAB 3: Dashboard
# =========================================================
with tab3:
    st.subheader("Dashboard")
    st.caption("Kurzüberblick (letzte 14 Tage).")

    tabsD = load_all(sheet_id)
    bakeD = ensure_cols(tabsD["bake_log"], REQUIRED_TABS["bake_log"])
    wasteD = ensure_cols(tabsD["waste_log"], REQUIRED_TABS["waste_log"])

    bakeD["date_dt"] = pd.to_datetime(bakeD["date"], errors="coerce")
    wasteD["date_dt"] = pd.to_datetime(wasteD["date"], errors="coerce")
    bakeD = bakeD.dropna(subset=["date_dt"]).copy()
    wasteD = wasteD.dropna(subset=["date_dt"]).copy()

    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=14)
    bake14 = bakeD[bakeD["date_dt"] >= cutoff].copy()
    waste14 = wasteD[wasteD["date_dt"] >= cutoff].copy()

    bake14["sku"] = bake14["sku"].astype(str).map(clean_sku)
    waste14["sku"] = waste14["sku"].astype(str).map(clean_sku)

    bake14["baked_total"] = (
        pd.to_numeric(bake14["baked_morning"], errors="coerce").fillna(0)
        + pd.to_numeric(bake14["baked_afternoon"], errors="coerce").fillna(0)
    )
    bake14["suggested_total"] = pd.to_numeric(bake14["suggested_total"], errors="coerce").fillna(0)
    bake14["override_total"] = pd.to_numeric(bake14["override_total"], errors="coerce")

    waste14["waste_qty"] = pd.to_numeric(waste14["waste_qty"], errors="coerce").fillna(0)
    waste14["early_empty"] = waste14["early_empty"].apply(parse_bool)

    df = bake14.merge(waste14[["date_dt", "sku", "waste_qty", "early_empty"]], on=["date_dt", "sku"], how="left")
    df["waste_qty"] = df["waste_qty"].fillna(0)
    df["early_empty"] = df["early_empty"].fillna(False)

    name_map = active[["sku", "name"]].drop_duplicates()
    df = df.merge(name_map, on="sku", how="left")

    c1, c2, c3, c4 = st.columns(4)
    baked_sum = float(df["baked_total"].sum()) if not df.empty else 0.0
    waste_sum = float(df["waste_qty"].sum()) if not df.empty else 0.0
    waste_rate = (waste_sum / baked_sum) if baked_sum > 0 else 0.0

    c1.metric("Gebacken (14T)", int(baked_sum))
    c2.metric("Abschrift (14T)", int(waste_sum))
    c3.metric("Abschriftquote", f"{waste_rate*100:.1f}%")
    c4.metric("Vor 14 leer (14T)", int(df["early_empty"].sum()) if not df.empty else 0)

    if df.empty:
        st.info("Noch nicht genug Daten.")
    else:
        left, right = st.columns(2)
        with left:
            st.write("**Top Abschrift (14 Tage)**")
            top_w = df.groupby("name", as_index=False).agg(abschrift=("waste_qty", "sum")).sort_values("abschrift", ascending=False).head(10)
            st.dataframe(top_w, use_container_width=True, hide_index=True)

        with right:
            st.write("**Häufig vor 14 Uhr leer (14 Tage)**")
            top_e = df.groupby("name", as_index=False).agg(vor14_leer=("early_empty", "sum")).sort_values("vor14_leer", ascending=False).head(10)
            st.dataframe(top_e, use_container_width=True, hide_index=True)

        st.divider()
        st.write("**Override-Check (vereinfacht)**")
        st.caption("Wenn Mitarbeiter oft höher geht als Vorschlag und trotzdem Abschrift hoch ist, war Override evtl. zu aggressiv.")
        tmp = df.copy()
        tmp["override_delta"] = (tmp["override_total"].fillna(tmp["suggested_total"]) - tmp["suggested_total"]).fillna(0)
        ov = tmp.groupby("name", as_index=False).agg(
            override_plus=("override_delta", lambda s: float(np.sum(np.maximum(s, 0)))),
            abschrift=("waste_qty", "sum"),
            tage=("date_dt", "nunique"),
        ).sort_values(["override_plus", "abschrift"], ascending=False).head(15)
        st.dataframe(ov, use_container_width=True, hide_index=True)
