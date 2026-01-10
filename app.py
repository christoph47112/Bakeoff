# =========================================================
# Bake-Off Planer – Finale Version (1 Seite, markt-tauglich)
# - Artikel anlegen in kleinem Bereich (stört Tagesarbeit nicht)
# - Planung (nur lesen)
# - Arbeitstabelle wie Excel (A=Artikel, daneben Eingaben)
# - Zwischenspeichern + "Fertig für heute" (Pflicht fürs Lernen)
# - Zeit-Orientierung + Warnung außerhalb Zeitfenster (nicht blockierend)
# - Stabil für Google Sheets (Cache + Retry + 1× Laden)
# =========================================================

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import time as pytime

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# -------------------------
# UI / Defaults
# -------------------------
st.set_page_config(page_title="Bake-Off Planer", layout="wide")

# Lern-Parameter (bewusst simpel)
ALPHA = 0.15
START_DEMAND = 20.0
START_MORNING_SHARE = 0.75
START_WASTE_RATE = 0.10

# Weiche Zeitfenster (nur Warnung)
WIN_MORNING = (5, 11)     # morgens
WIN_AFTERNOON = (12, 17)  # nachmittags
WIN_CLOSE = (18, 23)      # abends/abschluss

CACHE_TTL_SEC = 120

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
# Helpers (Logic)
# -------------------------
def weekday_name(d: date) -> str:
    return pd.to_datetime(d.isoformat()).day_name()

def in_window(hour: int, win: tuple[int, int]) -> bool:
    return win[0] <= hour <= win[1]

def clamp_int(x) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0

def clamp_float(x, lo=0.0, hi=1.0) -> float:
    try:
        v = float(x)
    except Exception:
        v = 0.0
    return float(max(lo, min(hi, v)))

def ensure_model_rows(model: pd.DataFrame, articles: pd.DataFrame) -> pd.DataFrame:
    weekdays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    if model.empty:
        model = pd.DataFrame(columns=["sku","weekday","demand","morning_share","waste_rate","updated_at"])
    if articles.empty:
        return model

    active = articles[articles["active"].astype(str).str.lower().isin(["true","1","yes","ja"])].copy()
    if active.empty:
        return model

    base = pd.MultiIndex.from_product(
        [active["sku"].astype(str).tolist(), weekdays],
        names=["sku","weekday"]
    ).to_frame(index=False)

    out = base.merge(model, on=["sku","weekday"], how="left")
    out["demand"] = pd.to_numeric(out.get("demand"), errors="coerce").fillna(START_DEMAND)
    out["morning_share"] = pd.to_numeric(out.get("morning_share"), errors="coerce").fillna(START_MORNING_SHARE)
    out["waste_rate"] = pd.to_numeric(out.get("waste_rate"), errors="coerce").fillna(START_WASTE_RATE)
    out["updated_at"] = out.get("updated_at", "").fillna("")
    return out

def recommend_total(demand: float, waste_rate: float) -> int:
    # Leicht konservativ bei hoher Abschriftquote
    base = max(0.0, float(demand))
    penalty = max(0.0, float(waste_rate) - 0.06)  # Ziel ~6%
    adj = 1.0 - 0.6 * penalty
    adj = float(np.clip(adj, 0.75, 1.20))
    return int(np.round(base * adj))

def split_qty(total: int, morning_share: float) -> tuple[int, int]:
    total = max(0, int(total))
    ms = float(np.clip(morning_share, 0.55, 0.95))
    m = int(np.round(total * ms))
    a = max(0, total - m)
    # wenn Nachmittag sehr klein -> als 1× behandeln
    if a <= 2:
        return total, 0

    # harte Untergrenze: Nachmittag nicht absurd groß
    if a > m:
        # wenn Model ausreißt, dämpfen
        m = int(np.round(total * 0.70))
        a = total - m
    return m, a

def build_work_table(active_articles: pd.DataFrame, plan_df: pd.DataFrame, today_log: pd.DataFrame, today_s: str) -> pd.DataFrame:
    base = active_articles[["sku","name"]].copy()
    base["date"] = today_s

    # Empfehlungsspalten
    plan_min = plan_df[["sku","rec_morning","rec_afternoon","mode","hint"]].copy()
    out = base.merge(plan_min, on="sku", how="left")

    # heutige Eingaben
    if today_log.empty:
        out["baked_morning"] = 0
        out["baked_afternoon"] = 0
        out["waste_qty"] = 0
        out["early_empty"] = False
        out["closed"] = False
    else:
        tl = today_log[["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed"]].copy()
        out = out.merge(tl, on=["date","sku"], how="left")

        out["baked_morning"] = pd.to_numeric(out.get("baked_morning"), errors="coerce").fillna(0).astype(int)
        out["baked_afternoon"] = pd.to_numeric(out.get("baked_afternoon"), errors="coerce").fillna(0).astype(int)
        out["waste_qty"] = pd.to_numeric(out.get("waste_qty"), errors="coerce").fillna(0).astype(int)

        ee = out.get("early_empty", "")
        out["early_empty"] = ee.astype(str).str.lower().isin(["true","1","yes","ja"])

        cl = out.get("closed", "")
        out["closed"] = cl.astype(str).str.lower().isin(["true","1","yes","ja"])

    # sort: empfohlene zuerst
    out["rec_total"] = pd.to_numeric(out["rec_morning"], errors="coerce").fillna(0) + pd.to_numeric(out["rec_afternoon"], errors="coerce").fillna(0)
    out = out.sort_values(["rec_total","name"], ascending=[False, True])
    return out

def status_hints(df: pd.DataFrame) -> list[str]:
    hints = []
    if df.empty:
        return ["Keine Artikel sichtbar (Filter?)."]
    # geschlossen?
    any_closed = bool(df["closed"].any()) if "closed" in df.columns else False
    if not any_closed:
        hints.append("Tagesabschluss ist noch nicht bestätigt („Fertig für heute“).")

    # Backen-Eingaben vorhanden?
    baked_any = ((df["baked_morning"] + df["baked_afternoon"]) > 0).sum()
    if baked_any == 0:
        hints.append("Noch keine Backmengen eingetragen.")
    return hints

# -------------------------
# UI
# -------------------------
st.title("🥐 Bake-Off Planer (1 Seite)")

top_l, top_r = st.columns([4, 1])
with top_r:
    if st.button("🔄 Daten neu laden"):
        load_all(force=True)
        st.success("Neu geladen.")
        st.rerun()

# Load data (once)
try:
    tabs = load_all(force=False)
except Exception as e:
    st.error("Google API hat gerade Probleme. Bitte nochmal „Daten neu laden“ drücken.")
    st.exception(e)
    st.stop()

articles = tabs["articles"]
daily_log = tabs["daily_log"]
model = tabs["demand_model"]

# Normalize tables
if articles.empty:
    articles = pd.DataFrame(columns=["sku","name","active","created_at"])
else:
    articles["sku"] = articles["sku"].astype(str)
    articles["name"] = articles.get("name", articles["sku"]).astype(str)
    articles["active"] = articles.get("active", "TRUE").astype(str)

today = date.today()
today_s = today.isoformat()
wd = weekday_name(today)
now = datetime.now()
hour = now.hour

# -------------------------
# Artikelverwaltung (klein & einklappbar)
# -------------------------
st.markdown("### 🧺 Artikel (Stamm)")

with st.expander("Artikel anlegen / aktivieren (selten nötig)", expanded=False):
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        new_sku = st.text_input("PLU / Artikelnummer", value="")
    with c2:
        new_name = st.text_input("Artikelname", value="")
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

            # Modellzeilen ergänzen
            tabs2 = load_all(force=True)
            arts2 = tabs2["articles"]
            mdl2 = tabs2["demand_model"]
            mdl2 = ensure_model_rows(mdl2, arts2)
            write_tab(sh, "demand_model", mdl2[["sku","weekday","demand","morning_share","waste_rate","updated_at"]])

            st.success("Artikel gespeichert.")
            load_all(force=True)
            st.rerun()

    st.write("**Aktive Artikel umschalten** (Haken weg = verschwindet aus Tagesarbeit)")
    if articles.empty:
        st.info("Noch keine Artikel.")
    else:
        arts_ui = articles.copy()
        arts_ui["active"] = arts_ui["active"].astype(str).str.lower().isin(["true","1","yes","ja"])
        edited_arts = st.data_editor(
            arts_ui[["sku","name","active"]],
            use_container_width=True,
            num_rows="fixed",
        )
        if st.button("💾 Artikelstatus speichern"):
            sh = open_spreadsheet()
            out = edited_arts.copy()
            out["active"] = out["active"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
            out["created_at"] = pd.Timestamp.utcnow().isoformat()
            upsert_tab(sh, "articles", out[["sku","name","active","created_at"]], key_cols=["sku"])

            # Modellzeilen sicherstellen
            arts2 = read_tab(sh, "articles")
            mdl2 = read_tab(sh, "demand_model")
            mdl2 = ensure_model_rows(mdl2, arts2)
            write_tab(sh, "demand_model", mdl2[["sku","weekday","demand","morning_share","waste_rate","updated_at"]])

            st.success("Gespeichert.")
            load_all(force=True)
            st.rerun()

# Reload normalized after potential changes
tabs = load_all(force=False)
articles = tabs["articles"]
daily_log = tabs["daily_log"]
model = tabs["demand_model"]

# Active articles
articles["active"] = articles["active"].astype(str)
active_articles = articles[articles["active"].str.lower().isin(["true","1","yes","ja"])].copy()
active_articles = active_articles.sort_values("name")

if active_articles.empty:
    st.warning("Keine aktiven Artikel. Bitte im Artikelbereich aktivieren.")
    st.stop()

# Ensure model rows
model = ensure_model_rows(model, articles)

# Today's log slice
if daily_log.empty:
    today_log = pd.DataFrame(columns=["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed","created_at"])
else:
    daily_log["date"] = daily_log["date"].astype(str)
    daily_log["sku"] = daily_log["sku"].astype(str)
    today_log = daily_log[daily_log["date"] == today_s].copy()

# -------------------------
# Orientierung (geführt)
# -------------------------
st.markdown("### 🧭 Orientierung")

c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
c1.metric("Heute", today.strftime("%d.%m.%Y"))
c2.metric("Uhrzeit", now.strftime("%H:%M"))
c3.metric("Wochentag", wd)

def time_message():
    if in_window(hour, WIN_MORNING):
        return "✅ Typischer Zeitpunkt für **Morgens gebacken**."
    if in_window(hour, WIN_AFTERNOON):
        return "✅ Typischer Zeitpunkt für **Nachmittags gebacken** (falls ihr nachbackt)."
    if in_window(hour, WIN_CLOSE):
        return "✅ Typischer Zeitpunkt für **Tagesabschluss** (Abschrift & „vor 14 Uhr leer“)."
    return "ℹ️ Ungewöhnliche Uhrzeit – Eingaben sind möglich, die App warnt nur."

with c4:
    st.info(time_message())

st.divider()

# -------------------------
# Schritt 1: Planung (nur lesen)
# -------------------------
st.markdown("## 🔵 Schritt 1 – Heute backen wir so")

plan = model[model["weekday"].astype(str) == wd].copy()
plan["sku"] = plan["sku"].astype(str)
plan = plan.merge(active_articles[["sku","name"]], on="sku", how="inner")

plan["demand"] = pd.to_numeric(plan.get("demand"), errors="coerce").fillna(START_DEMAND)
plan["morning_share"] = pd.to_numeric(plan.get("morning_share"), errors="coerce").fillna(START_MORNING_SHARE)
plan["waste_rate"] = pd.to_numeric(plan.get("waste_rate"), errors="coerce").fillna(START_WASTE_RATE)

plan["rec_total"] = plan.apply(lambda r: recommend_total(r["demand"], r["waste_rate"]), axis=1)
spl = plan.apply(lambda r: split_qty(int(r["rec_total"]), float(r["morning_share"])), axis=1)
plan["rec_morning"] = [m for m, a in spl]
plan["rec_afternoon"] = [a for m, a in spl]
plan["mode"] = np.where(plan["rec_afternoon"] > 0, "2×", "1×")

def hint_row(r):
    ms = float(r["morning_share"])
    wr = float(r["waste_rate"])
    if r["mode"] == "1×" and wr >= 0.12:
        return "Abschrift eher hoch → vorsichtiger."
    if r["mode"] == "2×" and ms >= 0.82:
        return "Nachmittag meist klein (mehr Bedarf morgens)."
    if r["mode"] == "2×" and ms <= 0.65:
        return "Relativ viel Nachmittagsbedarf (häufig vor 14 leer)."
    return "Empfehlung nach Lernstand."

plan["hint"] = plan.apply(hint_row, axis=1)

plan_view = plan[["name","rec_morning","rec_afternoon","mode","hint"]].sort_values(["mode","rec_morning"], ascending=[True, False])
st.dataframe(plan_view, use_container_width=True, hide_index=True)

st.divider()

# -------------------------
# Schritt 2 & 3: Arbeitstabelle (Excel-Stil)
# -------------------------
st.markdown("## 🟢 Schritt 2 & 🟡 Schritt 3 – Arbeitstabelle (einfach wie Excel)")
st.caption("Spalte A = Artikel, daneben trägst du Stückzahlen ein. Du musst nicht jeden Artikel „einzeln aufklappen“.")

work = build_work_table(active_articles, plan, today_log, today_s)

# Filters (speed!)
fc1, fc2, fc3, fc4 = st.columns([2, 1, 1, 1])
with fc1:
    q = st.text_input("Suche (Name oder PLU)", value="")
with fc2:
    only_recommended = st.checkbox("Nur Empfehlung > 0", value=True)
with fc3:
    only_edited = st.checkbox("Nur bearbeitete Zeilen", value=False)
with fc4:
    show_closed = st.checkbox("Auch geschlossene anzeigen", value=True)

view = work.copy()
if q.strip():
    qq = q.strip().lower()
    view = view[
        view["name"].astype(str).str.lower().str.contains(qq) |
        view["sku"].astype(str).str.lower().str.contains(qq)
    ]

if only_recommended:
    view = view[view["rec_total"] > 0]

if only_edited:
    view = view[
        (view["baked_morning"] + view["baked_afternoon"] + view["waste_qty"] > 0) |
        (view["early_empty"] == True)
    ]

if not show_closed:
    view = view[view["closed"] == False]

# Status hints
hints = status_hints(work)
if hints:
    for x in hints:
        st.warning(x)
else:
    st.success("Status wirkt vollständig ✅")

# Editor: A=Artikel, daneben Eingaben
edit_cols = [
    "sku","name",
    "rec_morning","rec_afternoon","mode",
    "baked_morning","baked_afternoon",
    "waste_qty","early_empty"
]
editor_df = view[edit_cols].copy()

edited = st.data_editor(
    editor_df,
    use_container_width=True,
    num_rows="fixed",
    column_config={
        "rec_morning": st.column_config.NumberColumn("Empf. morgens", disabled=True),
        "rec_afternoon": st.column_config.NumberColumn("Empf. nachm.", disabled=True),
        "mode": st.column_config.TextColumn("Modus", disabled=True),
        "baked_morning": st.column_config.NumberColumn("Morgens gebacken", min_value=0, step=1),
        "baked_afternoon": st.column_config.NumberColumn("Nachmittags gebacken", min_value=0, step=1),
        "waste_qty": st.column_config.NumberColumn("Abschrift", min_value=0, step=1),
        "early_empty": st.column_config.CheckboxColumn("Vor 14 Uhr leer"),
    },
    hide_index=True
)

# Buttons
b1, b2, b3 = st.columns([1, 1, 2])
with b1:
    save_btn = st.button("💾 Zwischenspeichern", type="secondary")
with b2:
    finish_btn = st.button("✅ Fertig für heute", type="primary")
with b3:
    st.caption("„Zwischenspeichern“ jederzeit. „Fertig für heute“ setzt den Tag auf abgeschlossen und **erst dann lernt** die App.")

# Determine if we need time-window warning
def needs_time_warning(edited_df: pd.DataFrame, original_df: pd.DataFrame) -> list[str]:
    msgs = []
    # Compare on sku
    o = original_df.set_index("sku")
    e = edited_df.set_index("sku")

    # Align
    common = e.index.intersection(o.index)
    if len(common) == 0:
        return msgs

    # Detect changed fields
    changed_m = (e.loc[common, "baked_morning"].astype(int) != o.loc[common, "baked_morning"].astype(int)).any()
    changed_a = (e.loc[common, "baked_afternoon"].astype(int) != o.loc[common, "baked_afternoon"].astype(int)).any()
    changed_close = (
        (e.loc[common, "waste_qty"].astype(int) != o.loc[common, "waste_qty"].astype(int)).any()
        or (e.loc[common, "early_empty"].astype(bool) != o.loc[common, "early_empty"].astype(bool)).any()
    )

    if changed_m and not in_window(hour, WIN_MORNING):
        msgs.append("⚠️ Du änderst **Morgens gebacken** außerhalb der üblichen Morgenzeit.")
    if changed_a and not in_window(hour, WIN_AFTERNOON):
        msgs.append("⚠️ Du änderst **Nachmittags gebacken** außerhalb der üblichen Nachmittagszeit.")
    if changed_close and not in_window(hour, WIN_CLOSE):
        msgs.append("⚠️ Du änderst **Abschlussfelder** (Abschrift/Vor14leer) außerhalb der üblichen Abendzeit.")
    return msgs

# Map edited back into full work table
def merge_back(full_work: pd.DataFrame, edited_view: pd.DataFrame) -> pd.DataFrame:
    out = full_work.copy()
    ed = edited_view.copy()
    ed["sku"] = ed["sku"].astype(str)

    # Only keep input columns
    ed = ed[["sku","baked_morning","baked_afternoon","waste_qty","early_empty"]].copy()
    ed["baked_morning"] = pd.to_numeric(ed["baked_morning"], errors="coerce").fillna(0).astype(int)
    ed["baked_afternoon"] = pd.to_numeric(ed["baked_afternoon"], errors="coerce").fillna(0).astype(int)
    ed["waste_qty"] = pd.to_numeric(ed["waste_qty"], errors="coerce").fillna(0).astype(int)
    ed["early_empty"] = ed["early_empty"].astype(bool)

    out = out.drop(columns=["baked_morning","baked_afternoon","waste_qty","early_empty"], errors="ignore")
    out = out.merge(ed, on="sku", how="left")

    # For rows not shown in editor, keep existing values by re-joining from original work:
    # (We handle by starting from original full_work and only overwriting for SKUs in ed)
    out2 = full_work.copy()
    out2["sku"] = out2["sku"].astype(str)
    for _, r in ed.iterrows():
        mask = out2["sku"] == r["sku"]
        out2.loc[mask, "baked_morning"] = int(r["baked_morning"])
        out2.loc[mask, "baked_afternoon"] = int(r["baked_afternoon"])
        out2.loc[mask, "waste_qty"] = int(r["waste_qty"])
        out2.loc[mask, "early_empty"] = bool(r["early_empty"])
    return out2

# Save / Finish actions
if save_btn or finish_btn:
    # Apply editor changes to full work data
    new_work = merge_back(work, edited)

    # Time window warnings
    warnings = needs_time_warning(edited, view)
    if warnings and "confirm_outside" not in st.session_state:
        st.session_state["confirm_outside"] = False

    can_continue = True
    if warnings and not st.session_state.get("confirm_outside", False):
        st.warning("Diese Eingabe ist außerhalb des üblichen Zeitfensters:")
        for m in warnings:
            st.write("- " + m)
        st.session_state["confirm_outside"] = st.checkbox("Trotzdem fortfahren", value=False)
        can_continue = st.session_state["confirm_outside"]

    if can_continue:
        sh = open_spreadsheet()

        # Prepare daily_log upsert for all active SKUs (full set), so the day is consistent
        to_write = new_work[["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed"]].copy()
        to_write["date"] = today_s
        to_write["sku"] = to_write["sku"].astype(str)

        # Preserve existing 'closed' if already closed unless finish pressed
        if not today_log.empty:
            prev_closed = today_log[["sku","closed"]].copy()
            prev_closed["sku"] = prev_closed["sku"].astype(str)
            prev_closed["closed"] = prev_closed["closed"].astype(str).str.lower().isin(["true","1","yes","ja"])
            to_write = to_write.merge(prev_closed, on="sku", how="left", suffixes=("","_prev"))
            to_write["closed"] = np.where(
                to_write["closed_prev"].fillna(False),
                True,
                to_write["closed"].astype(bool)
            )
            to_write = to_write.drop(columns=["closed_prev"], errors="ignore")

        if finish_btn:
            to_write["closed"] = True

        to_write["early_empty"] = to_write["early_empty"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        to_write["closed"] = to_write["closed"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        to_write["created_at"] = pd.Timestamp.utcnow().isoformat()

        upsert_tab(
            sh,
            "daily_log",
            to_write[["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed","created_at"]],
            key_cols=["date","sku"]
        )

        # Learn only on finish
        if finish_btn:
            # Reload latest model + articles
            arts2 = read_tab(sh, "articles")
            logs2 = read_tab(sh, "daily_log")
            mdl2 = read_tab(sh, "demand_model")
            mdl2 = ensure_model_rows(mdl2, arts2)

            logs2["date"] = logs2["date"].astype(str)
            logs2["sku"] = logs2["sku"].astype(str)

            rows = logs2[logs2["date"] == today_s].copy()
            if not rows.empty:
                rows["baked_morning"] = pd.to_numeric(rows.get("baked_morning", 0), errors="coerce").fillna(0.0)
                rows["baked_afternoon"] = pd.to_numeric(rows.get("baked_afternoon", 0), errors="coerce").fillna(0.0)
                rows["waste_qty"] = pd.to_numeric(rows.get("waste_qty", 0), errors="coerce").fillna(0.0)
                rows["early_empty"] = rows.get("early_empty","").astype(str).str.lower().isin(["true","1","yes","ja"])

                wd_today = weekday_name(today)

                for _, r in rows.iterrows():
                    sku = str(r["sku"])
                    baked_total = float(r["baked_morning"] + r["baked_afternoon"])
                    waste = float(r["waste_qty"])
                    sold_est = max(0.0, baked_total - waste)

                    mask = (mdl2["sku"].astype(str) == sku) & (mdl2["weekday"].astype(str) == wd_today)
                    if not mask.any():
                        continue

                    i = mdl2.index[mask][0]
                    old_demand = float(pd.to_numeric(mdl2.at[i, "demand"], errors="coerce") or START_DEMAND)
                    old_ms = float(pd.to_numeric(mdl2.at[i, "morning_share"], errors="coerce") or START_MORNING_SHARE)
                    old_wr = float(pd.to_numeric(mdl2.at[i, "waste_rate"], errors="coerce") or START_WASTE_RATE)

                    # Update demand (EMA)
                    new_demand = (1 - ALPHA) * old_demand + ALPHA * sold_est

                    # Update waste rate (EMA)
                    wr_obs = (waste / baked_total) if baked_total > 0 else 0.0
                    new_wr = (1 - ALPHA) * old_wr + ALPHA * wr_obs

                    # Update morning_share:
                    # - early_empty => morning share likely too low => increase
                    # - if waste is high and not early_empty => decrease
                    ms_target = old_ms
                    if bool(r["early_empty"]):
                        ms_target = min(0.95, old_ms + 0.06)
                    else:
                        if new_wr >= 0.12:
                            ms_target = max(0.55, old_ms - 0.05)

                    # If afternoon was always 0 and no early_empty, allow drift up toward 1×
                    if float(r["baked_afternoon"]) <= 0 and not bool(r["early_empty"]) and new_wr >= 0.10:
                        ms_target = min(0.95, max(ms_target, 0.80))

                    new_ms = (1 - ALPHA) * old_ms + ALPHA * ms_target
                    new_ms = float(np.clip(new_ms, 0.55, 0.95))

                    mdl2.at[i, "demand"] = float(max(0.0, new_demand))
                    mdl2.at[i, "waste_rate"] = clamp_float(new_wr, 0.0, 1.0)
                    mdl2.at[i, "morning_share"] = new_ms
                    mdl2.at[i, "updated_at"] = pd.Timestamp.utcnow().isoformat()

                write_tab(sh, "demand_model", mdl2[["sku","weekday","demand","morning_share","waste_rate","updated_at"]])

        # reset confirmation flag
        if "confirm_outside" in st.session_state:
            st.session_state["confirm_outside"] = False

        load_all(force=True)
        if finish_btn:
            st.success("✅ Tag abgeschlossen. Die App hat gelernt.")
        else:
            st.success("💾 Gespeichert.")
        st.rerun()

# -------------------------
# Mini-Überblick (optional)
# -------------------------
st.divider()
st.markdown("### 📊 Kurzer Überblick (optional)")

if daily_log.empty:
    st.info("Noch keine Tagesdaten vorhanden.")
else:
    df = daily_log.copy()
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])
    df["waste_qty"] = pd.to_numeric(df.get("waste_qty", 0), errors="coerce").fillna(0.0)
    df["early_empty"] = df.get("early_empty","").astype(str).str.lower().isin(["true","1","yes","ja"])
    df["sku"] = df["sku"].astype(str)

    # last 14 days
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=14)
    df14 = df[df["date_dt"] >= cutoff].copy()

    name_map = active_articles[["sku","name"]].drop_duplicates()
    df14 = df14.merge(name_map, on="sku", how="left")

    c1, c2, c3 = st.columns(3)
    c1.metric("Abschrift (14 Tage)", int(df14["waste_qty"].sum()))
    c2.metric("Vor 14 leer (14 Tage)", int(df14["early_empty"].sum()))
    c3.metric("Tage erfasst", int(df14["date_dt"].dt.date.nunique()))

    top_waste = df14.groupby("name", as_index=False).agg(abschrift=("waste_qty","sum")).sort_values("abschrift", ascending=False).head(10)
    top_empty = df14.groupby("name", as_index=False).agg(vor14_leer=("early_empty","sum")).sort_values("vor14_leer", ascending=False).head(10)

    cc1, cc2 = st.columns(2)
    with cc1:
        st.write("**Top Abschrift**")
        st.dataframe(top_waste, use_container_width=True, hide_index=True)
    with cc2:
        st.write("**Häufig vor 14 Uhr leer**")
        st.dataframe(top_empty, use_container_width=True, hide_index=True)
