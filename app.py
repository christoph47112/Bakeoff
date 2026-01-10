# =========================================================
# Bake-Off Planer – Finale Markt-Version (1 Seite)
# Workflow ohne Verwirrung:
#   🔵 HEUTE: Backen eintragen (morgens / nachmittags)
#   🟡 GESTERN: Abschrift eintragen + "vor 14 Uhr leer" + abschließen -> LERNEN
#
# Warum so?
# - Backmenge weiß man am Backtag (heute).
# - Abschrift weiß man erst am Folgetag (gestern).
# - Die App ordnet das automatisch zu -> keine Datumsverwechslung.
#
# Google Sheet Tabs (werden automatisch angelegt):
# - articles:      sku | name | active | created_at
# - daily_log:     date | sku | baked_morning | baked_afternoon | waste_qty | early_empty | closed | created_at
# - demand_model:  sku | weekday | demand | morning_share | waste_rate | updated_at
# =========================================================

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import time as pytime

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# -------------------------
# Config
# -------------------------
st.set_page_config(page_title="Bake-Off Planer", layout="wide")

ALPHA = 0.18  # etwas schnelleres Lernen (MVP)
START_DEMAND = 20.0
START_MORNING_SHARE = 0.78
START_WASTE_RATE = 0.08

CACHE_TTL_SEC = 120

WEEKDAYS = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

# -------------------------
# Google Sheets (stabil)
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

def gspread_retry(fn, tries=6, base_sleep=0.7):
    last = None
    for i in range(tries):
        try:
            return fn()
        except APIError as e:
            last = e
            pytime.sleep(min(base_sleep * (2 ** i), 8.0))
    raise last

def ensure_tabs(sh):
    required = {
        "articles": ["sku", "name", "active", "created_at"],
        "daily_log": ["date", "sku", "baked_morning", "baked_afternoon", "waste_qty", "early_empty", "closed", "created_at"],
        "demand_model": ["sku", "weekday", "demand", "morning_share", "waste_rate", "updated_at"],
    }
    existing = {w.title for w in sh.worksheets()}
    for tab, headers in required.items():
        if tab not in existing:
            gspread_retry(lambda: sh.add_worksheet(title=tab, rows=4000, cols=max(12, len(headers) + 2)))
        ws = sh.worksheet(tab)
        row1 = ws.row_values(1)
        if [x.strip() for x in row1[:len(headers)]] != headers:
            gspread_retry(lambda: ws.clear())
            gspread_retry(lambda: ws.update([headers]))

def read_tab(sh, tab: str) -> pd.DataFrame:
    ws = sh.worksheet(tab)
    values = gspread_retry(lambda: ws.get_all_values())
    if not values:
        return pd.DataFrame()
    headers = values[0]
    rows = values[1:]
    if not any(str(h).strip() for h in headers):
        return pd.DataFrame()
    rows2 = [r[:len(headers)] + [""] * max(0, len(headers) - len(r)) for r in rows]
    return pd.DataFrame(rows2, columns=headers)

def write_tab(sh, tab: str, df: pd.DataFrame):
    ws = sh.worksheet(tab)
    df2 = df.copy().replace({np.nan: ""})
    values = [df2.columns.tolist()] + df2.astype(object).values.tolist()
    gspread_retry(lambda: ws.clear())
    gspread_retry(lambda: ws.update(values))

def upsert_tab(sh, tab: str, df_new: pd.DataFrame, key_cols: list[str]):
    df_old = read_tab(sh, tab)
    if df_old.empty:
        df = df_new.copy()
    else:
        df = pd.concat([df_old, df_new], ignore_index=True)

    for c in key_cols:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].astype(str)

    df = df.drop_duplicates(subset=key_cols, keep="last")
    write_tab(sh, tab, df)

@st.cache_data(ttl=CACHE_TTL_SEC)
def load_all_cached(sheet_id: str) -> dict:
    sh = open_spreadsheet()
    ensure_tabs(sh)
    return {
        "articles": read_tab(sh, "articles"),
        "daily_log": read_tab(sh, "daily_log"),
        "demand_model": read_tab(sh, "demand_model"),
    }

def load_all(force=False) -> dict:
    sid = str(st.secrets["SHEET_ID"]).strip()
    if force:
        load_all_cached.clear()
    return load_all_cached(sid)

# -------------------------
# Helper functions
# -------------------------
def ensure_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy() if not df.empty else pd.DataFrame()
    for c in cols:
        if c not in out.columns:
            out[c] = ""
    return out

def clean_sku(s) -> str:
    s = str(s).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return ""
    return s

def clean_sku_list(series: pd.Series) -> list[str]:
    out = []
    for x in series.tolist():
        s = clean_sku(x)
        if s:
            out.append(s)
    seen = set()
    uniq = []
    for s in out:
        if s not in seen:
            uniq.append(s)
            seen.add(s)
    return uniq

def weekday_name(d: date) -> str:
    return pd.to_datetime(d.isoformat()).day_name()

def ensure_model_rows(model: pd.DataFrame, active_skus: list[str]) -> pd.DataFrame:
    model = ensure_columns(model, ["sku","weekday","demand","morning_share","waste_rate","updated_at"])
    if not active_skus:
        return model

    base = pd.MultiIndex.from_product([active_skus, WEEKDAYS], names=["sku","weekday"]).to_frame(index=False)
    out = base.merge(model, on=["sku","weekday"], how="left")

    out["demand"] = pd.to_numeric(out["demand"], errors="coerce").fillna(START_DEMAND)
    out["morning_share"] = pd.to_numeric(out["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
    out["waste_rate"] = pd.to_numeric(out["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)
    out["updated_at"] = out["updated_at"].fillna("")
    out["sku"] = out["sku"].astype(str)
    out["weekday"] = out["weekday"].astype(str)
    return out

def ensure_day_rows(sh, day_s: str, skus: list[str]):
    """Sorgt dafür, dass daily_log für (day, sku) existiert (sonst leere Standardzeile)."""
    if not skus:
        return
    existing = read_tab(sh, "daily_log")
    existing = ensure_columns(existing, ["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed","created_at"])
    if existing.empty:
        existing_keys = set()
    else:
        existing["date"] = existing["date"].astype(str)
        existing["sku"] = existing["sku"].astype(str)
        existing_keys = set(zip(existing["date"], existing["sku"]))

    rows = []
    for sku in skus:
        key = (day_s, sku)
        if key not in existing_keys:
            rows.append({
                "date": day_s,
                "sku": sku,
                "baked_morning": 0,
                "baked_afternoon": 0,
                "waste_qty": 0,
                "early_empty": "FALSE",
                "closed": "FALSE",
                "created_at": pd.Timestamp.utcnow().isoformat(),
            })
    if rows:
        upsert_tab(sh, "daily_log", pd.DataFrame(rows), key_cols=["date","sku"])

def recommend_total(demand: float, waste_rate: float) -> int:
    base = max(0.0, float(demand))
    # Wenn Abschriftquote hoch, leicht konservativer
    penalty = max(0.0, float(waste_rate) - 0.06)  # Ziel 6%
    adj = 1.0 - 0.7 * penalty
    adj = float(np.clip(adj, 0.70, 1.15))
    return int(np.round(base * adj))

def split_qty(total: int, morning_share: float) -> tuple[int, int]:
    total = max(0, int(total))
    ms = float(np.clip(morning_share, 0.55, 0.95))
    m = int(np.round(total * ms))
    a = max(0, total - m)
    # wenn Nachmittagsmenge minimal: als 1× behandeln
    if a <= 2:
        return total, 0
    return m, a

def parse_bool(x) -> bool:
    return str(x).strip().lower() in ("true","1","yes","ja")

def clamp01(x: float) -> float:
    return float(np.clip(float(x), 0.0, 1.0))

# -------------------------
# UI Header
# -------------------------
st.title("🥐 Bake-Off Planer")
st.caption("Einfach: **Heute backen eintragen** + **Gestern Abschrift eintragen** → App lernt und gibt bessere Backvorschläge.")

top_l, top_r = st.columns([5, 1])
with top_r:
    if st.button("🔄 Neu laden"):
        load_all(force=True)
        st.rerun()

# -------------------------
# Load data
# -------------------------
try:
    tabs = load_all(force=False)
except Exception as e:
    st.error("Google API Problem. Bitte nochmal „Neu laden“ drücken.")
    st.exception(e)
    st.stop()

articles = ensure_columns(tabs["articles"], ["sku","name","active","created_at"])
daily_log = ensure_columns(tabs["daily_log"], ["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed","created_at"])
model = ensure_columns(tabs["demand_model"], ["sku","weekday","demand","morning_share","waste_rate","updated_at"])

# Clean articles
articles["sku"] = articles["sku"].astype(str).map(clean_sku)
articles = articles[articles["sku"] != ""].copy()
articles["name"] = articles["name"].astype(str)
articles["active"] = articles["active"].astype(str)

active_articles = articles[articles["active"].str.lower().isin(["true","1","yes","ja"])].copy()
active_articles = active_articles.sort_values("name")
active_skus = clean_sku_list(active_articles["sku"])

# No active articles -> prompt
st.markdown("### 🧺 Artikel (Stamm)")
with st.expander("Artikel anlegen / aktivieren (selten nötig)", expanded=(len(active_skus) == 0)):
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        new_sku = st.text_input("PLU / Artikelnummer")
    with c2:
        new_name = st.text_input("Artikelname")
    with c3:
        new_active = st.checkbox("Aktiv", value=True)

    if st.button("➕ Artikel hinzufügen"):
        if not new_sku.strip() or not new_name.strip():
            st.warning("Bitte PLU und Artikelname ausfüllen.")
        else:
            sh = open_spreadsheet()
            row = pd.DataFrame([{
                "sku": new_sku.strip(),
                "name": new_name.strip(),
                "active": "TRUE" if new_active else "FALSE",
                "created_at": pd.Timestamp.utcnow().isoformat(),
            }])
            upsert_tab(sh, "articles", row, key_cols=["sku"])
            load_all(force=True)
            st.success("Artikel gespeichert.")
            st.rerun()

    if not articles.empty:
        ui = articles.copy()
        ui["active"] = ui["active"].str.lower().isin(["true","1","yes","ja"])
        edited = st.data_editor(ui[["sku","name","active"]], use_container_width=True, num_rows="fixed", hide_index=True)
        if st.button("💾 Artikelstatus speichern"):
            sh = open_spreadsheet()
            out = edited.copy()
            out["active"] = out["active"].apply(lambda v: "TRUE" if bool(v) else "FALSE")
            out["created_at"] = pd.Timestamp.utcnow().isoformat()
            upsert_tab(sh, "articles", out[["sku","name","active","created_at"]], key_cols=["sku"])
            load_all(force=True)
            st.success("Gespeichert.")
            st.rerun()

# Refresh active after possible changes
tabs = load_all(force=False)
articles = ensure_columns(tabs["articles"], ["sku","name","active","created_at"])
articles["sku"] = articles["sku"].astype(str).map(clean_sku)
articles = articles[articles["sku"] != ""].copy()
articles["name"] = articles["name"].astype(str)
articles["active"] = articles["active"].astype(str)

active_articles = articles[articles["active"].str.lower().isin(["true","1","yes","ja"])].copy()
active_articles = active_articles.sort_values("name")
active_skus = clean_sku_list(active_articles["sku"])

if not active_skus:
    st.warning("Bitte mindestens 1 Artikel aktivieren/anlegen.")
    st.stop()

# Ensure model rows for active skus
model = ensure_model_rows(model, active_skus)

# Dates
today = date.today()
yesterday = today - timedelta(days=1)
today_s = today.isoformat()
yesterday_s = yesterday.isoformat()
wd_today = weekday_name(today)
wd_yesterday = weekday_name(yesterday)

# Ensure daily rows exist for today & yesterday (so Tabellen immer vollständig sind)
sh = open_spreadsheet()
ensure_tabs(sh)
ensure_day_rows(sh, today_s, active_skus)
ensure_day_rows(sh, yesterday_s, active_skus)

# Reload logs after ensuring rows
tabs = load_all(force=True)
daily_log = ensure_columns(tabs["daily_log"], ["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed","created_at"])
daily_log["date"] = daily_log["date"].astype(str)
daily_log["sku"] = daily_log["sku"].astype(str)

today_log = daily_log[daily_log["date"] == today_s].copy()
yest_log = daily_log[daily_log["date"] == yesterday_s].copy()

# Normalize numeric fields
for df in (today_log, yest_log):
    df["baked_morning"] = pd.to_numeric(df["baked_morning"], errors="coerce").fillna(0).astype(int)
    df["baked_afternoon"] = pd.to_numeric(df["baked_afternoon"], errors="coerce").fillna(0).astype(int)
    df["waste_qty"] = pd.to_numeric(df["waste_qty"], errors="coerce").fillna(0).astype(int)
    df["early_empty"] = df["early_empty"].apply(parse_bool)
    df["closed"] = df["closed"].apply(parse_bool)

# -------------------------
# PLANUNG (Heute) – Empfehlung
# -------------------------
st.divider()
st.markdown("## 🔵 Heute: Backvorschlag (automatisch)")

plan = model[model["weekday"].astype(str) == wd_today].copy()
plan = plan.merge(active_articles[["sku","name"]], on="sku", how="inner")
plan["demand"] = pd.to_numeric(plan["demand"], errors="coerce").fillna(START_DEMAND)
plan["morning_share"] = pd.to_numeric(plan["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
plan["waste_rate"] = pd.to_numeric(plan["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)

plan["rec_total"] = plan.apply(lambda r: recommend_total(r["demand"], r["waste_rate"]), axis=1)
spl = plan.apply(lambda r: split_qty(int(r["rec_total"]), float(r["morning_share"])), axis=1)
plan["rec_morning"] = [m for m, a in spl]
plan["rec_afternoon"] = [a for m, a in spl]
plan["mode"] = np.where(plan["rec_afternoon"] > 0, "2× (morgens + nachm.)", "1× (nur morgens)")

def hint_row(r):
    wr = float(r["waste_rate"])
    if wr >= 0.14:
        return "Abschrift hoch → vorsichtiger planen."
    if r["rec_afternoon"] == 0:
        return "Heute reicht meist morgens."
    return "Nachmittags kleiner Nachschub."

plan["hint"] = plan.apply(hint_row, axis=1)
plan_view = plan[["name","rec_morning","rec_afternoon","mode","hint"]].sort_values(["rec_morning","rec_afternoon"], ascending=False)
st.dataframe(plan_view, use_container_width=True, hide_index=True)

# -------------------------
# 🔵 HEUTE: Backen eintragen
# -------------------------
st.divider()
st.markdown("## 🔵 Heute: Backen eintragen")
st.caption("Hier trägst du **heute** ein, wie viel wirklich gebacken wurde. Abschrift kommt morgen in den Abschluss (automatisch als „Gestern“).")

# Build today work table
today_tbl = active_articles[["sku","name"]].merge(
    plan[["sku","rec_morning","rec_afternoon","mode"]],
    on="sku", how="left"
).merge(
    today_log[["sku","baked_morning","baked_afternoon"]],
    on="sku", how="left"
)

today_tbl["baked_morning"] = pd.to_numeric(today_tbl["baked_morning"], errors="coerce").fillna(0).astype(int)
today_tbl["baked_afternoon"] = pd.to_numeric(today_tbl["baked_afternoon"], errors="coerce").fillna(0).astype(int)

t1, t2, t3 = st.columns([2, 1, 1])
with t1:
    q_today = st.text_input("Suche (heute)", value="")
with t2:
    only_rec_today = st.checkbox("Nur Empfehlung > 0 (heute)", value=True)
with t3:
    only_edited_today = st.checkbox("Nur bearbeitete (heute)", value=False)

today_view = today_tbl.copy()
if q_today.strip():
    qq = q_today.strip().lower()
    today_view = today_view[
        today_view["name"].astype(str).str.lower().str.contains(qq) |
        today_view["sku"].astype(str).str.lower().str.contains(qq)
    ]
if only_rec_today:
    today_view = today_view[(today_view["rec_morning"].fillna(0) + today_view["rec_afternoon"].fillna(0)) > 0]
if only_edited_today:
    today_view = today_view[(today_view["baked_morning"] + today_view["baked_afternoon"]) > 0]

today_editor = st.data_editor(
    today_view[["sku","name","rec_morning","rec_afternoon","mode","baked_morning","baked_afternoon"]],
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    column_config={
        "rec_morning": st.column_config.NumberColumn("Empf. morgens", disabled=True),
        "rec_afternoon": st.column_config.NumberColumn("Empf. nachm.", disabled=True),
        "mode": st.column_config.TextColumn("Modus", disabled=True),
        "baked_morning": st.column_config.NumberColumn("Heute morgens gebacken", min_value=0, step=1),
        "baked_afternoon": st.column_config.NumberColumn("Heute nachmittags gebacken", min_value=0, step=1),
    }
)

save_today = st.button("💾 Heute speichern", type="secondary")
if save_today:
    # merge back into full today table by sku
    upd = today_editor.copy()
    upd["sku"] = upd["sku"].astype(str).map(clean_sku)
    upd = upd[upd["sku"] != ""].copy()

    # apply updates to base today_tbl
    base = today_tbl.copy()
    base["sku"] = base["sku"].astype(str)
    base = base.set_index("sku")
    upd = upd.set_index("sku")

    common = base.index.intersection(upd.index)
    base.loc[common, "baked_morning"] = pd.to_numeric(upd.loc[common, "baked_morning"], errors="coerce").fillna(0).astype(int)
    base.loc[common, "baked_afternoon"] = pd.to_numeric(upd.loc[common, "baked_afternoon"], errors="coerce").fillna(0).astype(int)
    base = base.reset_index()

    rows = []
    for _, r in base.iterrows():
        rows.append({
            "date": today_s,
            "sku": str(r["sku"]),
            "baked_morning": int(r["baked_morning"]),
            "baked_afternoon": int(r["baked_afternoon"]),
            # Abschlussfelder bleiben wie in Sheet (heute noch nicht relevant)
            "waste_qty": int(today_log.loc[today_log["sku"] == str(r["sku"]), "waste_qty"].iloc[0]) if (today_log["sku"] == str(r["sku"])).any() else 0,
            "early_empty": "TRUE" if (today_log.loc[today_log["sku"] == str(r["sku"]), "early_empty"].iloc[0] if (today_log["sku"] == str(r["sku"])).any() else False) else "FALSE",
            "closed": "TRUE" if (today_log.loc[today_log["sku"] == str(r["sku"]), "closed"].iloc[0] if (today_log["sku"] == str(r["sku"])).any() else False) else "FALSE",
            "created_at": pd.Timestamp.utcnow().isoformat(),
        })

    upsert_tab(sh, "daily_log", pd.DataFrame(rows), key_cols=["date","sku"])
    load_all(force=True)
    st.success("Heute gespeichert ✅")
    st.rerun()

# -------------------------
# 🟡 GESTERN: Abschluss & Lernen
# -------------------------
st.divider()
st.markdown("## 🟡 Gestern: Abschrift eintragen + abschließen (damit die App lernt)")
st.caption("Hier trägst du **morgen/früh** ein, was **gestern** abgeschrieben wurde. Beim Abschluss lernt die App und aktualisiert die Empfehlungen.")

# Build yesterday close table
y_tbl = active_articles[["sku","name"]].merge(
    yest_log[["sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed"]],
    on="sku", how="left"
)

y_tbl["baked_morning"] = pd.to_numeric(y_tbl["baked_morning"], errors="coerce").fillna(0).astype(int)
y_tbl["baked_afternoon"] = pd.to_numeric(y_tbl["baked_afternoon"], errors="coerce").fillna(0).astype(int)
y_tbl["waste_qty"] = pd.to_numeric(y_tbl["waste_qty"], errors="coerce").fillna(0).astype(int)
y_tbl["early_empty"] = y_tbl["early_empty"].fillna(False).astype(bool)
y_tbl["closed"] = y_tbl["closed"].fillna(False).astype(bool)

is_closed = bool(y_tbl["closed"].all()) if len(y_tbl) > 0 else False
if is_closed:
    st.success("Gestern ist bereits abgeschlossen ✅ (Du kannst trotzdem Werte korrigieren und erneut abschließen, falls nötig.)")

y1, y2, y3 = st.columns([2, 1, 1])
with y1:
    q_y = st.text_input("Suche (gestern)", value="")
with y2:
    only_baked_y = st.checkbox("Nur Artikel mit Backen (gestern)", value=False)
with y3:
    only_edited_y = st.checkbox("Nur bearbeitete (gestern)", value=False)

y_view = y_tbl.copy()
if q_y.strip():
    qq = q_y.strip().lower()
    y_view = y_view[
        y_view["name"].astype(str).str.lower().str.contains(qq) |
        y_view["sku"].astype(str).str.lower().str.contains(qq)
    ]
if only_baked_y:
    y_view = y_view[(y_view["baked_morning"] + y_view["baked_afternoon"]) > 0]
if only_edited_y:
    y_view = y_view[(y_view["waste_qty"] > 0) | (y_view["early_empty"] == True)]

y_editor = st.data_editor(
    y_view[["sku","name","baked_morning","baked_afternoon","waste_qty","early_empty"]],
    use_container_width=True,
    hide_index=True,
    num_rows="fixed",
    column_config={
        "baked_morning": st.column_config.NumberColumn("Gestern morgens gebacken", disabled=True),
        "baked_afternoon": st.column_config.NumberColumn("Gestern nachm. gebacken", disabled=True),
        "waste_qty": st.column_config.NumberColumn("Gestern Abschrift", min_value=0, step=1),
        "early_empty": st.column_config.CheckboxColumn("Gestern vor 14 Uhr leer"),
    }
)

c_save, c_finish = st.columns([1, 2])
save_y = c_save.button("💾 Gestern speichern", type="secondary")
finish_y = c_finish.button("✅ Gestern abschließen & Lernen", type="primary")

def apply_yesterday_updates(y_editor_df: pd.DataFrame) -> pd.DataFrame:
    upd = y_editor_df.copy()
    upd["sku"] = upd["sku"].astype(str).map(clean_sku)
    upd = upd[upd["sku"] != ""].copy()
    upd["waste_qty"] = pd.to_numeric(upd["waste_qty"], errors="coerce").fillna(0).astype(int)
    upd["early_empty"] = upd["early_empty"].astype(bool)

    base = y_tbl.copy()
    base["sku"] = base["sku"].astype(str)
    base = base.set_index("sku")
    upd = upd.set_index("sku")
    common = base.index.intersection(upd.index)
    base.loc[common, "waste_qty"] = upd.loc[common, "waste_qty"]
    base.loc[common, "early_empty"] = upd.loc[common, "early_empty"]
    base = base.reset_index()
    return base

if save_y or finish_y:
    base = apply_yesterday_updates(y_editor)

    # write yesterday rows
    rows = []
    for _, r in base.iterrows():
        rows.append({
            "date": yesterday_s,
            "sku": str(r["sku"]),
            "baked_morning": int(r["baked_morning"]),
            "baked_afternoon": int(r["baked_afternoon"]),
            "waste_qty": int(r["waste_qty"]),
            "early_empty": "TRUE" if bool(r["early_empty"]) else "FALSE",
            "closed": "TRUE" if bool(finish_y) else ("TRUE" if bool(r["closed"]) else "FALSE"),
            "created_at": pd.Timestamp.utcnow().isoformat(),
        })
    upsert_tab(sh, "daily_log", pd.DataFrame(rows), key_cols=["date","sku"])

    if finish_y:
        # -------- Lernen auf Grundlage von GESTERN --------
        # model BEFORE snapshot (für sichtbaren Effekt)
        model_before = model.copy()
        model_before["demand"] = pd.to_numeric(model_before["demand"], errors="coerce").fillna(START_DEMAND)
        model_before["morning_share"] = pd.to_numeric(model_before["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
        model_before["waste_rate"] = pd.to_numeric(model_before["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)

        # rows for yesterday (fresh read)
        tabs2 = load_all(force=True)
        logs2 = ensure_columns(tabs2["daily_log"], ["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed","created_at"])
        logs2["date"] = logs2["date"].astype(str)
        logs2["sku"] = logs2["sku"].astype(str)
        y_rows = logs2[logs2["date"] == yesterday_s].copy()

        y_rows["baked_morning"] = pd.to_numeric(y_rows["baked_morning"], errors="coerce").fillna(0.0)
        y_rows["baked_afternoon"] = pd.to_numeric(y_rows["baked_afternoon"], errors="coerce").fillna(0.0)
        y_rows["waste_qty"] = pd.to_numeric(y_rows["waste_qty"], errors="coerce").fillna(0.0)
        y_rows["early_empty"] = y_rows["early_empty"].apply(parse_bool)

        model2 = ensure_model_rows(tabs2["demand_model"], active_skus)
        model2["demand"] = pd.to_numeric(model2["demand"], errors="coerce").fillna(START_DEMAND)
        model2["morning_share"] = pd.to_numeric(model2["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
        model2["waste_rate"] = pd.to_numeric(model2["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)

        # Update per SKU for yesterday weekday
        for _, r in y_rows.iterrows():
            sku = clean_sku(r["sku"])
            if not sku:
                continue
            baked_total = float(r["baked_morning"] + r["baked_afternoon"])
            waste = float(r["waste_qty"])
            sold_est = max(0.0, baked_total - waste)

            mask = (model2["sku"].astype(str) == sku) & (model2["weekday"].astype(str) == wd_yesterday)
            if not mask.any():
                continue
            i = model2.index[mask][0]

            old_demand = float(model2.at[i, "demand"])
            old_ms = float(model2.at[i, "morning_share"])
            old_wr = float(model2.at[i, "waste_rate"])

            # Demand lernt aus "verkauft geschätzt" (gebacken - abschrift)
            new_demand = (1 - ALPHA) * old_demand + ALPHA * sold_est

            # Waste-rate lernt aus Abschriftquote
            wr_obs = (waste / baked_total) if baked_total > 0 else 0.0
            new_wr = (1 - ALPHA) * old_wr + ALPHA * wr_obs
            new_wr = clamp01(new_wr)

            # Morning-share: wenn gestern vor 14 leer -> mehr Anteil morgens
            ms_target = old_ms
            if bool(r["early_empty"]):
                ms_target = min(0.95, old_ms + 0.06)
            else:
                # wenn Abschrift hoch -> weniger aggressiv morgens
                if new_wr >= 0.12:
                    ms_target = max(0.55, old_ms - 0.04)

            new_ms = (1 - ALPHA) * old_ms + ALPHA * ms_target
            new_ms = float(np.clip(new_ms, 0.55, 0.95))

            model2.at[i, "demand"] = max(0.0, float(new_demand))
            model2.at[i, "waste_rate"] = float(new_wr)
            model2.at[i, "morning_share"] = float(new_ms)
            model2.at[i, "updated_at"] = pd.Timestamp.utcnow().isoformat()

        # Write model back
        write_tab(sh, "demand_model", model2[["sku","weekday","demand","morning_share","waste_rate","updated_at"]])

        # Show effect (sofort sichtbar)
        # recompute today rec before/after for a quick delta table
        mb_today = model_before[model_before["weekday"].astype(str) == wd_today].copy()
        ma_today = model2[model2["weekday"].astype(str) == wd_today].copy()

        mb_today = mb_today.merge(active_articles[["sku","name"]], on="sku", how="inner")
        ma_today = ma_today.merge(active_articles[["sku","name"]], on="sku", how="inner")

        def rec_df(df):
            df = df.copy()
            df["demand"] = pd.to_numeric(df["demand"], errors="coerce").fillna(START_DEMAND)
            df["morning_share"] = pd.to_numeric(df["morning_share"], errors="coerce").fillna(START_MORNING_SHARE)
            df["waste_rate"] = pd.to_numeric(df["waste_rate"], errors="coerce").fillna(START_WASTE_RATE)
            df["rec_total"] = df.apply(lambda r: recommend_total(r["demand"], r["waste_rate"]), axis=1)
            sp = df.apply(lambda r: split_qty(int(r["rec_total"]), float(r["morning_share"])), axis=1)
            df["rec_morning"] = [m for m, a in sp]
            df["rec_afternoon"] = [a for m, a in sp]
            return df[["sku","name","rec_morning","rec_afternoon"]]

        before_rec = rec_df(mb_today).rename(columns={"rec_morning":"vor_morgens","rec_afternoon":"vor_nachm"})
        after_rec = rec_df(ma_today).rename(columns={"rec_morning":"neu_morgens","rec_afternoon":"neu_nachm"})

        delta = before_rec.merge(after_rec, on=["sku","name"], how="inner")
        delta["Δ morgens"] = delta["neu_morgens"] - delta["vor_morgens"]
        delta["Δ nachm"] = delta["neu_nachm"] - delta["vor_nachm"]
        delta = delta.sort_values(["Δ morgens","Δ nachm"], ascending=False)

        st.success("Gestern abgeschlossen ✅ Die App hat gelernt und die Empfehlungen wurden aktualisiert.")
        with st.expander("Was hat sich in der Empfehlung geändert? (heute)", expanded=True):
            st.dataframe(delta[["name","vor_morgens","neu_morgens","Δ morgens","vor_nachm","neu_nachm","Δ nachm"]], use_container_width=True, hide_index=True)

    else:
        st.success("Gestern gespeichert ✅ (Lernen passiert erst bei „Gestern abschließen & Lernen“).")

    load_all(force=True)
    st.rerun()

# -------------------------
# Mini Dashboard (optional)
# -------------------------
st.divider()
st.markdown("### 📊 Überblick (letzte 14 Tage)")

tabs3 = load_all(force=False)
logs = ensure_columns(tabs3["daily_log"], ["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed","created_at"])
if logs.empty:
    st.info("Noch keine Daten.")
else:
    logs["date_dt"] = pd.to_datetime(logs["date"], errors="coerce")
    logs = logs.dropna(subset=["date_dt"]).copy()
    logs["waste_qty"] = pd.to_numeric(logs["waste_qty"], errors="coerce").fillna(0.0)
    logs["early_empty"] = logs["early_empty"].apply(parse_bool)
    logs["baked_total"] = pd.to_numeric(logs["baked_morning"], errors="coerce").fillna(0.0) + pd.to_numeric(logs["baked_afternoon"], errors="coerce").fillna(0.0)

    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=14)
    df14 = logs[logs["date_dt"] >= cutoff].copy()
    name_map = active_articles[["sku","name"]].drop_duplicates()
    df14["sku"] = df14["sku"].astype(str)
    df14 = df14.merge(name_map, on="sku", how="left")

    c1, c2, c3 = st.columns(3)
    c1.metric("Abschrift (14 Tage)", int(df14["waste_qty"].sum()))
    c2.metric("Vor 14 Uhr leer (14 Tage)", int(df14["early_empty"].sum()))
    c3.metric("Blech-/Backmenge (14 Tage)", int(df14["baked_total"].sum()))

    top_waste = df14.groupby("name", as_index=False).agg(abschrift=("waste_qty","sum")).sort_values("abschrift", ascending=False).head(10)
    st.write("**Top Abschrift**")
    st.dataframe(top_waste, use_container_width=True, hide_index=True)
