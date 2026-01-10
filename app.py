# =========================================================
# Bake-Off Planer (MDE-Style) – Frontend + Backend (Google Sheets)
#
# Ziel:
# - Mitarbeiter sieht nur Frontend-Reiter:
#     1) Backvorschlag (heute)  -> Vorschlag ansehen/überschreiben + Ist-Backen eintragen
#     2) Abschriften (gestern)  -> Abschrift eintragen + Abschluss drücken (Lernen)
#     3) Dashboard              -> kurze Auswertung
# - Kein Datum auswählen, keine Verwirrung:
#     Backen = HEUTE, Abschrift = GESTERN
# - Lernen:
#     Aus (gebacken - abschrift) wird Nachfrage geschätzt
#     Vorschlag wird smarter (Demand + WasteRate + MorningShare)
# - Stabiler Google-API Layer:
#     - Wenige Calls, Retry, batch_update, Caching
#
# Google Sheet Tabs:
# - articles      : sku | name | active | created_at
# - bake_log      : date | sku | suggested_total | suggested_morning | suggested_afternoon
#                  | override_total | baked_morning | baked_afternoon | note | updated_at
# - waste_log     : date | sku | waste_qty | early_empty | closed | updated_at
# - demand_model  : sku | weekday | demand | morning_share | waste_rate | updated_at
# =========================================================

import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, timedelta
from datetime import datetime as dt
import time

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# -------------------------
# Settings
# -------------------------
st.set_page_config(page_title="Bake-Off Planer", layout="wide")

# Lernparameter (MVP, aber brauchbar)
ALPHA = 0.22                 # wie schnell passt sich Modell an
START_DEMAND = 20.0
START_MORNING_SHARE = 0.78
START_WASTE_RATE = 0.08
TARGET_WASTE = 0.06

WEEKDAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

CACHE_TTL = 90  # Sekunden

# -------------------------
# Google auth
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

def retry(fn, tries=6, base_sleep=0.6):
    last = None
    for i in range(tries):
        try:
            return fn()
        except APIError as e:
            last = e
            time.sleep(min(base_sleep * (2 ** i), 8.0))
    raise last

# -------------------------
# Sheet schema + creation (cached)
# -------------------------
REQUIRED_TABS = {
    "articles": [
        "sku","name","active","created_at"
    ],
    "bake_log": [
        "date","sku",
        "suggested_total","suggested_morning","suggested_afternoon",
        "override_total","baked_morning","baked_afternoon",
        "note","updated_at"
    ],
    "waste_log": [
        "date","sku","waste_qty","early_empty","closed","updated_at"
    ],
    "demand_model": [
        "sku","weekday","demand","morning_share","waste_rate","updated_at"
    ],
}

@st.cache_resource
def ensure_schema_once():
    """
    Läuft selten. Verhindert ständige worksheet-metadata calls.
    """
    sh = open_spreadsheet()

    def _ensure():
        existing_titles = [ws.title for ws in retry(lambda: sh.worksheets())]
        for tab, headers in REQUIRED_TABS.items():
            if tab not in existing_titles:
                retry(lambda: sh.add_worksheet(title=tab, rows=4000, cols=max(12, len(headers)+2)))
            ws = sh.worksheet(tab)
            row1 = retry(lambda: ws.row_values(1))
            if [x.strip() for x in row1[:len(headers)]] != headers:
                retry(lambda: ws.clear())
                retry(lambda: ws.update([headers]))
    retry(_ensure)
    return True

def ws(tab: str):
    ensure_schema_once()
    sh = open_spreadsheet()
    return sh.worksheet(tab)

# -------------------------
# Data helpers
# -------------------------
def clean_sku(x) -> str:
    s = str(x).strip()
    if not s or s.lower() in ("nan","none","null"):
        return ""
    return s

def weekday_name(d: date) -> str:
    return pd.to_datetime(d.isoformat()).day_name()

def ensure_df_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy() if not df.empty else pd.DataFrame()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    return out

def now_iso():
    return dt.utcnow().isoformat()

# -------------------------
# Minimal-call table read (cached)
# -------------------------
@st.cache_data(ttl=CACHE_TTL)
def load_tables(_sheet_id: str) -> dict:
    # _sheet_id in signature so cache invalidates if user changes it
    out = {}
    for tab, headers in REQUIRED_TABS.items():
        w = ws(tab)
        vals = retry(lambda: w.get_all_values())
        if not vals:
            out[tab] = pd.DataFrame(columns=headers)
            continue
        hdr = vals[0]
        rows = vals[1:]
        df = pd.DataFrame(rows, columns=hdr if hdr else headers)
        df = ensure_df_cols(df, headers)
        out[tab] = df
    return out

def invalidate_cache():
    load_tables.clear()

# -------------------------
# Upsert rows efficiently (batch_update + append)
# -------------------------
def upsert_rows(tab: str, key_cols: list[str], rows: list[dict]):
    """
    Upsert rows into a worksheet with minimal calls.
    - reads all values once to map key->row index
    - batch updates changed rows
    - appends new rows
    """
    if not rows:
        return

    w = ws(tab)
    headers = REQUIRED_TABS[tab]

    # read all current values once
    values = retry(lambda: w.get_all_values())
    if not values:
        retry(lambda: w.update([headers]))
        values = [headers]

    current_headers = values[0]
    if current_headers[:len(headers)] != headers:
        # fix schema if broken
        retry(lambda: w.clear())
        retry(lambda: w.update([headers]))
        values = [headers]

    # build key->row index (1-based, header row=1)
    key_to_row = {}
    for i, r in enumerate(values[1:], start=2):
        row_map = {headers[j]: (r[j] if j < len(r) else "") for j in range(len(headers))}
        key = tuple(str(row_map.get(k, "")).strip() for k in key_cols)
        if all(key):
            key_to_row[key] = i

    # prepare batch updates + appends
    updates = []
    appends = []

    for row in rows:
        row_norm = {h: row.get(h, "") for h in headers}
        # clean keys
        for k in key_cols:
            row_norm[k] = str(row_norm.get(k, "")).strip()

        key = tuple(row_norm[k] for k in key_cols)
        row_values = [row_norm[h] for h in headers]

        if all(key) and key in key_to_row:
            rix = key_to_row[key]
            # Update full row (A..)
            end_col = chr(ord("A") + len(headers) - 1)
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
# Model logic
# -------------------------
def ensure_model_for_active(model: pd.DataFrame, skus: list[str]) -> pd.DataFrame:
    model = ensure_df_cols(model, REQUIRED_TABS["demand_model"])
    if not skus:
        return model

    base = pd.MultiIndex.from_product([skus, WEEKDAYS], names=["sku","weekday"]).to_frame(index=False)
    out = base.merge(model, on=["sku","weekday"], how="left")

    out["demand"] = pd.to_numeric(out["demand"], errors="coerce").fillna(START_DEMAND)
    out["morning_share"] = pd.to_numeric(out["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
    out["waste_rate"] = pd.to_numeric(out["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)
    out["updated_at"] = out["updated_at"].fillna("")
    return out

def recommend_total(demand: float, waste_rate: float) -> int:
    demand = max(0.0, float(demand))
    wr = float(np.clip(float(waste_rate), 0.0, 1.0))

    # wenn Waste zu hoch -> konservativer; wenn Waste sehr niedrig -> leicht aggressiver
    penalty = wr - TARGET_WASTE
    adj = 1.0 - 0.70 * max(0.0, penalty) + 0.15 * max(0.0, -penalty)
    adj = float(np.clip(adj, 0.70, 1.20))

    return int(np.round(demand * adj))

def split_qty(total: int, morning_share: float) -> tuple[int, int]:
    total = max(0, int(total))
    ms = float(np.clip(float(morning_share), 0.55, 0.95))
    m = int(np.round(total * ms))
    a = max(0, total - m)
    if a <= 2:
        return total, 0
    return m, a

def learn_from_yesterday(model: pd.DataFrame, y_rows: pd.DataFrame, wd_y: str) -> pd.DataFrame:
    """
    y_rows: waste_log + bake_log merged by sku for yesterday
    Learn:
      demand <- EMA(sold_est = baked_total - waste)
      waste_rate <- EMA(waste / baked_total)
      morning_share <- EMA(target adjustments using early_empty signal)
    """
    out = model.copy()
    # numeric
    out["demand"] = pd.to_numeric(out["demand"], errors="coerce").fillna(START_DEMAND)
    out["morning_share"] = pd.to_numeric(out["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
    out["waste_rate"] = pd.to_numeric(out["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)

    y_rows = y_rows.copy()
    y_rows["baked_total"] = pd.to_numeric(y_rows.get("baked_total", 0), errors="coerce").fillna(0.0)
    y_rows["waste_qty"] = pd.to_numeric(y_rows.get("waste_qty", 0), errors="coerce").fillna(0.0)
    y_rows["early_empty"] = y_rows.get("early_empty", False).astype(bool)

    for _, r in y_rows.iterrows():
        sku = clean_sku(r.get("sku", ""))
        if not sku:
            continue

        baked_total = float(r["baked_total"])
        waste = float(r["waste_qty"])
        sold_est = max(0.0, baked_total - waste)
        wr_obs = (waste / baked_total) if baked_total > 0 else 0.0
        wr_obs = float(np.clip(wr_obs, 0.0, 1.0))

        mask = (out["sku"].astype(str) == sku) & (out["weekday"].astype(str) == wd_y)
        if not mask.any():
            continue
        i = out.index[mask][0]

        old_d = float(out.at[i, "demand"])
        old_ms = float(out.at[i, "morning_share"])
        old_wr = float(out.at[i, "waste_rate"])

        # Demand update
        new_d = (1 - ALPHA) * old_d + ALPHA * sold_est

        # Waste rate update
        new_wr = (1 - ALPHA) * old_wr + ALPHA * wr_obs
        new_wr = float(np.clip(new_wr, 0.0, 1.0))

        # Morning share target
        ms_target = old_ms
        if bool(r["early_empty"]):
            ms_target = min(0.95, old_ms + 0.06)
        else:
            if new_wr >= 0.12:
                ms_target = max(0.55, old_ms - 0.04)

        new_ms = (1 - ALPHA) * old_ms + ALPHA * ms_target
        new_ms = float(np.clip(new_ms, 0.55, 0.95))

        out.at[i, "demand"] = max(0.0, float(new_d))
        out.at[i, "waste_rate"] = float(new_wr)
        out.at[i, "morning_share"] = float(new_ms)
        out.at[i, "updated_at"] = now_iso()

    return out

# -------------------------
# Frontend
# -------------------------
st.title("🥐 Bake-Off (MDE)")

colA, colB = st.columns([5, 1])
with colB:
    if st.button("🔄 Neu laden"):
        invalidate_cache()
        st.rerun()

sheet_id = str(st.secrets["SHEET_ID"]).strip()
tabs = load_tables(sheet_id)

articles = ensure_df_cols(tabs["articles"], REQUIRED_TABS["articles"])
bake_log = ensure_df_cols(tabs["bake_log"], REQUIRED_TABS["bake_log"])
waste_log = ensure_df_cols(tabs["waste_log"], REQUIRED_TABS["waste_log"])
model = ensure_df_cols(tabs["demand_model"], REQUIRED_TABS["demand_model"])

# Clean + active articles
articles["sku"] = articles["sku"].astype(str).map(clean_sku)
articles = articles[articles["sku"] != ""].copy()
articles["name"] = articles["name"].astype(str)
articles["active"] = articles["active"].astype(str)

active = articles[articles["active"].str.lower().isin(["true","1","yes","ja"])].copy()
active = active.sort_values("name")
active_skus = [clean_sku(x) for x in active["sku"].tolist() if clean_sku(x)]

# Article management (small)
with st.expander("🧺 Artikel verwalten (selten)", expanded=(len(active_skus) == 0)):
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        new_sku = st.text_input("PLU / Artikelnummer", value="")
    with c2:
        new_name = st.text_input("Artikelname", value="")
    with c3:
        new_active = st.checkbox("Aktiv", value=True)

    if st.button("➕ Artikel speichern"):
        if not new_sku.strip() or not new_name.strip():
            st.warning("Bitte PLU und Artikelname ausfüllen.")
        else:
            row = {
                "sku": new_sku.strip(),
                "name": new_name.strip(),
                "active": "TRUE" if new_active else "FALSE",
                "created_at": now_iso(),
            }
            upsert_rows("articles", ["sku"], [row])
            st.success("Gespeichert.")
            st.rerun()

    if not articles.empty:
        ui = articles.copy()
        ui["active"] = ui["active"].str.lower().isin(["true","1","yes","ja"])
        edited = st.data_editor(ui[["sku","name","active"]], use_container_width=True, hide_index=True, num_rows="fixed")
        if st.button("💾 Aktiv-Status übernehmen"):
            rows = []
            for _, r in edited.iterrows():
                rows.append({
                    "sku": clean_sku(r["sku"]),
                    "name": str(r["name"]),
                    "active": "TRUE" if bool(r["active"]) else "FALSE",
                    "created_at": now_iso(),
                })
            upsert_rows("articles", ["sku"], rows)
            st.success("Gespeichert.")
            st.rerun()

# Must have active
if not active_skus:
    st.warning("Bitte mindestens 1 Artikel anlegen/aktivieren.")
    st.stop()

# Ensure model rows exist for active SKUs (in memory; write only if missing)
model_full = ensure_model_for_active(model, active_skus)
if model_full.shape[0] != model.shape[0] or set(model_full.columns) != set(model.columns):
    # write back to ensure backend complete
    rows = []
    for _, r in model_full.iterrows():
        rows.append({
            "sku": str(r["sku"]),
            "weekday": str(r["weekday"]),
            "demand": float(r["demand"]),
            "morning_share": float(r["morning_share"]),
            "waste_rate": float(r["waste_rate"]),
            "updated_at": str(r.get("updated_at", "")) or "",
        })
    upsert_rows("demand_model", ["sku","weekday"], rows)
    tabs = load_tables(sheet_id)
    model = ensure_df_cols(tabs["demand_model"], REQUIRED_TABS["demand_model"])
    model_full = ensure_model_for_active(model, active_skus)

# Dates
today = date.today()
yesterday = today - timedelta(days=1)
today_s = today.isoformat()
yesterday_s = yesterday.isoformat()
wd_today = weekday_name(today)
wd_yesterday = weekday_name(yesterday)

# Tabs like MDE
tab1, tab2, tab3 = st.tabs(["📌 Backvorschlag (Heute)", "🧾 Abschriften (Gestern)", "📊 Dashboard"])

# ---------------------------------------------------------
# TAB 1: Backvorschlag (Heute) + Überschreiben + Ist-Backen
# ---------------------------------------------------------
with tab1:
    st.subheader("Backvorschlag (Heute)")
    st.caption("Du kannst den Vorschlag pro Artikel überschreiben. Das wird gespeichert und später fürs Lernen berücksichtigt.")

    # Build today suggestion from model
    m_today = model_full[model_full["weekday"].astype(str) == wd_today].copy()
    m_today = m_today.merge(active[["sku","name"]], on="sku", how="inner")

    m_today["demand"] = pd.to_numeric(m_today["demand"], errors="coerce").fillna(START_DEMAND)
    m_today["morning_share"] = pd.to_numeric(m_today["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
    m_today["waste_rate"] = pd.to_numeric(m_today["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)

    m_today["suggested_total"] = m_today.apply(lambda r: recommend_total(r["demand"], r["waste_rate"]), axis=1)
    sp = m_today.apply(lambda r: split_qty(int(r["suggested_total"]), float(r["morning_share"])), axis=1)
    m_today["suggested_morning"] = [a for a, b in sp]
    m_today["suggested_afternoon"] = [b for a, b in sp]

    # Merge with today's existing bake_log (if any)
    bake_log["date"] = bake_log["date"].astype(str)
    bake_log["sku"] = bake_log["sku"].astype(str).map(clean_sku)
    b_today = bake_log[bake_log["date"] == today_s].copy()

    view = m_today.merge(
        b_today[[
            "sku","override_total","baked_morning","baked_afternoon","note",
            "suggested_total","suggested_morning","suggested_afternoon"
        ]],
        on="sku",
        how="left",
        suffixes=("","_saved")
    )

    # If stored suggested exists, keep it (so suggestion doesn't shift mid-day)
    for c in ["suggested_total","suggested_morning","suggested_afternoon"]:
        view[c] = pd.to_numeric(view[c], errors="coerce")
        view[c + "_saved"] = pd.to_numeric(view.get(c + "_saved"), errors="coerce")
        view[c] = view[c + "_saved"].where(view[c + "_saved"].notna(), view[c])
        if c + "_saved" in view.columns:
            view = view.drop(columns=[c + "_saved"])

    view["override_total"] = pd.to_numeric(view["override_total"], errors="coerce").fillna(view["suggested_total"]).astype(int)
    view["baked_morning"] = pd.to_numeric(view["baked_morning"], errors="coerce").fillna(0).astype(int)
    view["baked_afternoon"] = pd.to_numeric(view["baked_afternoon"], errors="coerce").fillna(0).astype(int)
    view["note"] = view["note"].fillna("")

    # Filters
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        q = st.text_input("Suche", value="", key="q_today")
    with c2:
        only_pos = st.checkbox("Nur Vorschlag > 0", value=True)
    with c3:
        only_touched = st.checkbox("Nur bearbeitet", value=False)

    v = view.copy()
    if q.strip():
        qq = q.strip().lower()
        v = v[v["name"].str.lower().str.contains(qq) | v["sku"].str.lower().str.contains(qq)]
    if only_pos:
        v = v[v["suggested_total"] > 0]
    if only_touched:
        v = v[(v["baked_morning"] + v["baked_afternoon"] > 0) | (v["override_total"] != v["suggested_total"]) | (v["note"].astype(str).str.len() > 0)]

    editor = st.data_editor(
        v[[
            "sku","name",
            "suggested_total","suggested_morning","suggested_afternoon",
            "override_total",
            "baked_morning","baked_afternoon",
            "note"
        ]],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "suggested_total": st.column_config.NumberColumn("Vorschlag gesamt", disabled=True),
            "suggested_morning": st.column_config.NumberColumn("Vorschlag morgens", disabled=True),
            "suggested_afternoon": st.column_config.NumberColumn("Vorschlag nachm.", disabled=True),
            "override_total": st.column_config.NumberColumn("Überschreiben (gesamt)", min_value=0, step=1),
            "baked_morning": st.column_config.NumberColumn("Ist gebacken morgens", min_value=0, step=1),
            "baked_afternoon": st.column_config.NumberColumn("Ist gebacken nachm.", min_value=0, step=1),
            "note": st.column_config.TextColumn("Notiz (optional)"),
        }
    )

    if st.button("💾 Heute speichern (Backvorschlag + Ist)", type="primary"):
        # Merge edited rows back into full set by sku
        base = view.set_index("sku")
        ed = editor.copy()
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
                "note": str(r.get("note","") or ""),
                "updated_at": now_iso(),
            })

        upsert_rows("bake_log", ["date","sku"], rows)
        st.success("Heute gespeichert ✅")
        st.rerun()

# ---------------------------------------------------------
# TAB 2: Abschriften (Gestern) + Abschluss -> Lernen
# ---------------------------------------------------------
with tab2:
    st.subheader("Abschriften (Gestern)")
    st.caption("Hier trägst du die Abschrift für **gestern** ein. Danach: „Abschluss & Lernen“ → Vorschläge werden besser.")

    # Build yesterday view: show what was baked yesterday + waste inputs
    bake_log["date"] = bake_log["date"].astype(str)
    b_y = bake_log[bake_log["date"] == yesterday_s].copy()
    b_y["sku"] = b_y["sku"].astype(str).map(clean_sku)
    b_y["baked_morning"] = pd.to_numeric(b_y["baked_morning"], errors="coerce").fillna(0).astype(int)
    b_y["baked_afternoon"] = pd.to_numeric(b_y["baked_afternoon"], errors="coerce").fillna(0).astype(int)
    b_y["baked_total"] = b_y["baked_morning"] + b_y["baked_afternoon"]

    waste_log["date"] = waste_log["date"].astype(str)
    waste_log["sku"] = waste_log["sku"].astype(str).map(clean_sku)
    w_y = waste_log[waste_log["date"] == yesterday_s].copy()
    if w_y.empty:
        w_y = pd.DataFrame(columns=REQUIRED_TABS["waste_log"])

    w_y["waste_qty"] = pd.to_numeric(w_y.get("waste_qty", 0), errors="coerce").fillna(0).astype(int)
    w_y["early_empty"] = w_y.get("early_empty","FALSE").astype(str).str.lower().isin(["true","1","yes","ja"])
    w_y["closed"] = w_y.get("closed","FALSE").astype(str).str.lower().isin(["true","1","yes","ja"])

    base = active[["sku","name"]].copy()
    base = base.merge(b_y[["sku","baked_total"]], on="sku", how="left").fillna({"baked_total": 0})
    base = base.merge(w_y[["sku","waste_qty","early_empty","closed"]], on="sku", how="left")

    base["baked_total"] = pd.to_numeric(base["baked_total"], errors="coerce").fillna(0).astype(int)
    base["waste_qty"] = pd.to_numeric(base["waste_qty"], errors="coerce").fillna(0).astype(int)
    base["early_empty"] = base["early_empty"].fillna(False).astype(bool)
    base["closed"] = base["closed"].fillna(False).astype(bool)

    already_closed = bool(base["closed"].all()) if len(base) else False
    if already_closed:
        st.success("Gestern ist bereits abgeschlossen ✅ (du kannst trotzdem korrigieren und erneut abschließen).")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        q2 = st.text_input("Suche", value="", key="q_y")
    with c2:
        only_baked = st.checkbox("Nur mit Backen", value=True)
    with c3:
        only_edited = st.checkbox("Nur bearbeitet", value=False)

    v = base.copy()
    if q2.strip():
        qq = q2.strip().lower()
        v = v[v["name"].str.lower().str.contains(qq) | v["sku"].str.lower().str.contains(qq)]
    if only_baked:
        v = v[v["baked_total"] > 0]
    if only_edited:
        v = v[(v["waste_qty"] > 0) | (v["early_empty"] == True)]

    editor2 = st.data_editor(
        v[["sku","name","baked_total","waste_qty","early_empty"]],
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "baked_total": st.column_config.NumberColumn("Gestern gebacken (Info)", disabled=True),
            "waste_qty": st.column_config.NumberColumn("Gestern Abschrift", min_value=0, step=1),
            "early_empty": st.column_config.CheckboxColumn("Vor 14 Uhr leer"),
        }
    )

    b_save, b_close = st.columns([1, 2])
    do_save = b_save.button("💾 Speichern", type="secondary")
    do_close = b_close.button("✅ Abschluss & Lernen", type="primary")

    if do_save or do_close:
        # merge back into base
        b0 = base.set_index("sku")
        ed = editor2.copy()
        ed["sku"] = ed["sku"].astype(str).map(clean_sku)
        ed = ed[ed["sku"] != ""].set_index("sku")

        common = b0.index.intersection(ed.index)
        b0.loc[common, "waste_qty"] = pd.to_numeric(ed.loc[common, "waste_qty"], errors="coerce").fillna(0).astype(int)
        b0.loc[common, "early_empty"] = ed.loc[common, "early_empty"].astype(bool)

        if do_close:
            b0["closed"] = True

        b0 = b0.reset_index()

        # write waste_log for yesterday
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
        upsert_rows("waste_log", ["date","sku"], rows)

        if do_close:
            # Learn from yesterday only if we have yesterday baked totals
            # Merge again from cached tables
            invalidate_cache()
            tabsN = load_tables(sheet_id)
            bakeN = ensure_df_cols(tabsN["bake_log"], REQUIRED_TABS["bake_log"])
            wasteN = ensure_df_cols(tabsN["waste_log"], REQUIRED_TABS["waste_log"])
            modelN = ensure_df_cols(tabsN["demand_model"], REQUIRED_TABS["demand_model"])

            bakeN["date"] = bakeN["date"].astype(str)
            bakeN["sku"] = bakeN["sku"].astype(str).map(clean_sku)
            b_y2 = bakeN[bakeN["date"] == yesterday_s].copy()
            b_y2["baked_morning"] = pd.to_numeric(b_y2["baked_morning"], errors="coerce").fillna(0).astype(int)
            b_y2["baked_afternoon"] = pd.to_numeric(b_y2["baked_afternoon"], errors="coerce").fillna(0).astype(int)
            b_y2["baked_total"] = b_y2["baked_morning"] + b_y2["baked_afternoon"]

            wasteN["date"] = wasteN["date"].astype(str)
            wasteN["sku"] = wasteN["sku"].astype(str).map(clean_sku)
            w_y2 = wasteN[wasteN["date"] == yesterday_s].copy()
            w_y2["waste_qty"] = pd.to_numeric(w_y2["waste_qty"], errors="coerce").fillna(0).astype(int)
            w_y2["early_empty"] = w_y2["early_empty"].astype(str).str.lower().isin(["true","1","yes","ja"])

            y_rows = active[["sku"]].merge(b_y2[["sku","baked_total"]], on="sku", how="left").merge(
                w_y2[["sku","waste_qty","early_empty"]], on="sku", how="left"
            )
            y_rows["baked_total"] = pd.to_numeric(y_rows["baked_total"], errors="coerce").fillna(0).astype(int)
            y_rows["waste_qty"] = pd.to_numeric(y_rows["waste_qty"], errors="coerce").fillna(0).astype(int)
            y_rows["early_empty"] = y_rows["early_empty"].fillna(False).astype(bool)

            # Snapshot BEFORE for quick delta
            model_before = ensure_model_for_active(modelN, active_skus)

            # Learn
            model_after = learn_from_yesterday(ensure_model_for_active(modelN, active_skus), y_rows, wd_yesterday)

            # Write model back (upsert)
            rowsM = []
            for _, r in model_after.iterrows():
                rowsM.append({
                    "sku": str(r["sku"]),
                    "weekday": str(r["weekday"]),
                    "demand": float(r["demand"]),
                    "morning_share": float(r["morning_share"]),
                    "waste_rate": float(r["waste_rate"]),
                    "updated_at": str(r.get("updated_at","") or ""),
                })
            upsert_rows("demand_model", ["sku","weekday"], rowsM)

            # Show effect for today's recommendation
            def rec_table(model_df):
                mt = model_df[model_df["weekday"].astype(str) == wd_today].copy()
                mt = mt.merge(active[["sku","name"]], on="sku", how="inner")
                mt["demand"] = pd.to_numeric(mt["demand"], errors="coerce").fillna(START_DEMAND)
                mt["morning_share"] = pd.to_numeric(mt["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
                mt["waste_rate"] = pd.to_numeric(mt["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)
                mt["total"] = mt.apply(lambda r: recommend_total(r["demand"], r["waste_rate"]), axis=1)
                sp = mt.apply(lambda r: split_qty(int(r["total"]), float(r["morning_share"])), axis=1)
                mt["m"] = [a for a,b in sp]
                mt["a"] = [b for a,b in sp]
                return mt[["sku","name","m","a"]]

            beforeR = rec_table(model_before).rename(columns={"m":"vor_m","a":"vor_a"})
            afterR  = rec_table(model_after).rename(columns={"m":"neu_m","a":"neu_a"})
            delta = beforeR.merge(afterR, on=["sku","name"], how="inner")
            delta["Δ morgens"] = delta["neu_m"] - delta["vor_m"]
            delta["Δ nachm"] = delta["neu_a"] - delta["vor_a"]
            delta = delta.sort_values(["Δ morgens","Δ nachm"], ascending=False)

            st.success("Abschluss gespeichert ✅ App hat gelernt. Empfehlungen wurden aktualisiert.")
            with st.expander("Änderung in der Empfehlung (heute)", expanded=True):
                st.dataframe(delta[["name","vor_m","neu_m","Δ morgens","vor_a","neu_a","Δ nachm"]], use_container_width=True, hide_index=True)

        else:
            st.success("Gespeichert ✅")
        st.rerun()

# ---------------------------------------------------------
# TAB 3: Dashboard
# ---------------------------------------------------------
with tab3:
    st.subheader("Dashboard")
    st.caption("Kurzüberblick aus den letzten Tagen.")

    # Build last 14 days aggregates
    tabsD = load_tables(sheet_id)
    bakeD = ensure_df_cols(tabsD["bake_log"], REQUIRED_TABS["bake_log"])
    wasteD = ensure_df_cols(tabsD["waste_log"], REQUIRED_TABS["waste_log"])

    bakeD["date"] = pd.to_datetime(bakeD["date"], errors="coerce")
    wasteD["date"] = pd.to_datetime(wasteD["date"], errors="coerce")

    bakeD = bakeD.dropna(subset=["date"]).copy()
    wasteD = wasteD.dropna(subset=["date"]).copy()

    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=14)
    bake14 = bakeD[bakeD["date"] >= cutoff].copy()
    waste14 = wasteD[wasteD["date"] >= cutoff].copy()

    bake14["sku"] = bake14["sku"].astype(str).map(clean_sku)
    waste14["sku"] = waste14["sku"].astype(str).map(clean_sku)

    bake14["baked_total"] = (
        pd.to_numeric(bake14["baked_morning"], errors="coerce").fillna(0)
        + pd.to_numeric(bake14["baked_afternoon"], errors="coerce").fillna(0)
    )
    waste14["waste_qty"] = pd.to_numeric(waste14["waste_qty"], errors="coerce").fillna(0)

    df = bake14.merge(waste14[["date","sku","waste_qty","early_empty"]], on=["date","sku"], how="left")
    df["waste_qty"] = df["waste_qty"].fillna(0)
    df["early_empty"] = df["early_empty"].astype(str).str.lower().isin(["true","1","yes","ja"])

    name_map = active[["sku","name"]].drop_duplicates()
    df = df.merge(name_map, on="sku", how="left")

    c1, c2, c3 = st.columns(3)
    c1.metric("Gebacken (14 Tage)", int(df["baked_total"].sum()) if not df.empty else 0)
    c2.metric("Abschrift (14 Tage)", int(df["waste_qty"].sum()) if not df.empty else 0)
    waste_rate = (df["waste_qty"].sum() / df["baked_total"].sum()) if (not df.empty and df["baked_total"].sum() > 0) else 0.0
    c3.metric("Abschriftquote", f"{waste_rate*100:.1f}%")

    if df.empty:
        st.info("Noch nicht genug Daten für Dashboard.")
    else:
        top_waste = df.groupby("name", as_index=False).agg(abschrift=("waste_qty","sum")).sort_values("abschrift", ascending=False).head(10)
        top_empty = df.groupby("name", as_index=False).agg(vor14_leer=("early_empty","sum")).sort_values("vor14_leer", ascending=False).head(10)

        cc1, cc2 = st.columns(2)
        with cc1:
            st.write("**Top Abschrift (14 Tage)**")
            st.dataframe(top_waste, use_container_width=True, hide_index=True)
        with cc2:
            st.write("**Häufig vor 14 Uhr leer (14 Tage)**")
            st.dataframe(top_empty, use_container_width=True, hide_index=True)
