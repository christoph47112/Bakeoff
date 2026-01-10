import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import time as pytime

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# =========================
# App Settings
# =========================
st.set_page_config(page_title="Bake-Off Planer", layout="wide")

# Default config
DEFAULT_CLOSE_HOUR = 21
DEFAULT_ALPHA = 0.15
DEFAULT_WASTE_TARGET = 0.06

START_DEMAND_DEFAULT = 20
START_MORNING_SHARE = 0.75

MIN_DAYS_FOR_SPLIT_LEARNING = 5

# Time windows (soft guidance)
WIN_MORNING = (5, 11)      # 05:00–10:59
WIN_AFTERNOON = (12, 17)   # 12:00–16:59
WIN_CLOSE = (18, 23)       # 18:00–22:59

CACHE_TTL_SEC = 120

# =========================
# Google Sheets: stable access
# =========================
@st.cache_resource
def gs_client():
    if "gcp_service_account" not in st.secrets:
        st.error("Secrets fehlen: [gcp_service_account] ist nicht gesetzt.")
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
def open_sheet():
    sheet_id = str(st.secrets.get("SHEET_ID", "")).strip()
    if not sheet_id:
        st.error("Secrets fehlen: SHEET_ID ist nicht gesetzt.")
        st.stop()
    return gs_client().open_by_key(sheet_id)

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
        "daily_log": [
            "date", "sku",
            "baked_morning", "baked_afternoon",
            "waste_qty", "early_empty",
            "submitted_close",
            "created_at"
        ],
        "demand_model": ["sku", "weekday", "demand_est", "morning_share", "waste_rate_est", "updated_at"],
        "config": ["key", "value"],
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

def read_tab_values(sh, tab: str) -> pd.DataFrame:
    ws = sh.worksheet(tab)
    values = gspread_retry(lambda: ws.get_all_values())
    if not values:
        return pd.DataFrame()
    headers = values[0]
    rows = values[1:]
    if not any(str(h).strip() for h in headers):
        headers = [f"col_{i+1}" for i in range(len(headers))]
    rows2 = [r[:len(headers)] + [""] * max(0, len(headers) - len(r)) for r in rows]
    return pd.DataFrame(rows2, columns=headers)

def write_tab(sh, tab: str, df: pd.DataFrame):
    ws = sh.worksheet(tab)
    df2 = df.copy().replace({np.nan: ""})
    values = [df2.columns.tolist()] + df2.astype(object).values.tolist()
    gspread_retry(lambda: ws.clear())
    gspread_retry(lambda: ws.update(values))

def upsert(sh, tab: str, df_new: pd.DataFrame, key_cols: list[str]):
    df_old = read_tab_values(sh, tab)
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
    sh = open_sheet()
    ensure_tabs(sh)
    return {
        "config": read_tab_values(sh, "config"),
        "articles": read_tab_values(sh, "articles"),
        "daily_log": read_tab_values(sh, "daily_log"),
        "demand_model": read_tab_values(sh, "demand_model"),
    }

def load_all(force=False) -> dict:
    sheet_id = str(st.secrets.get("SHEET_ID", "")).strip()
    if force:
        load_all_cached.clear()
    return load_all_cached(sheet_id)

# =========================
# Helpers
# =========================
def now_local():
    # Streamlit uses server tz; user requested Europe/Berlin.
    # We'll approximate using local server time; for strict tz you'd use pytz.
    return datetime.now()

def in_window(hour: int, win: tuple[int, int]):
    return win[0] <= hour <= win[1]

def clamp_int(x):
    try:
        return int(float(x))
    except Exception:
        return 0

def clamp_float(x, lo=0.0, hi=1.0):
    try:
        v = float(x)
    except Exception:
        v = 0.0
    return float(max(lo, min(hi, v)))

def get_cfg(cfg: pd.DataFrame, key: str, default):
    if cfg.empty or "key" not in cfg.columns or "value" not in cfg.columns:
        return default
    hit = cfg[cfg["key"].astype(str) == str(key)]
    if hit.empty:
        return default
    v = hit.iloc[0]["value"]
    try:
        if isinstance(default, int):
            return int(float(v))
        if isinstance(default, float):
            return float(v)
    except Exception:
        return default
    return v

def weekday_name(d: date) -> str:
    return pd.to_datetime(d.isoformat()).day_name()

def ensure_model_rows(model: pd.DataFrame, articles: pd.DataFrame) -> pd.DataFrame:
    weekdays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

    if model.empty:
        model = pd.DataFrame(columns=["sku","weekday","demand_est","morning_share","waste_rate_est","updated_at"])

    if articles.empty:
        return model

    active = articles[articles["active"].astype(str).str.lower().isin(["true","1","yes","ja"])].copy()
    if active.empty:
        return model

    base = pd.MultiIndex.from_product([active["sku"].astype(str).tolist(), weekdays], names=["sku","weekday"]).to_frame(index=False)
    out = base.merge(model, on=["sku","weekday"], how="left")

    out["demand_est"] = pd.to_numeric(out.get("demand_est"), errors="coerce").fillna(START_DEMAND_DEFAULT)
    out["morning_share"] = pd.to_numeric(out.get("morning_share"), errors="coerce").fillna(START_MORNING_SHARE)
    out["waste_rate_est"] = pd.to_numeric(out.get("waste_rate_est"), errors="coerce").fillna(0.10)
    out["updated_at"] = out.get("updated_at", "").fillna("")
    return out

def recommend_total(demand_est: float, waste_rate_est: float, waste_target: float):
    # Simple adjustment: if waste > target reduce slightly, if waste < target keep.
    base = max(0.0, float(demand_est))
    penalty = max(0.0, float(waste_rate_est) - float(waste_target))
    adj = 1.0 - 0.6 * penalty
    adj = float(np.clip(adj, 0.75, 1.20))
    return int(np.round(base * adj))

def split_qty(total: int, morning_share: float):
    total = max(0, int(total))
    ms = float(np.clip(morning_share, 0.50, 0.95))
    m = int(np.round(total * ms))
    a = max(0, total - m)
    # if afternoon extremely small, treat as 1x (keep it in morning)
    if a <= 2:
        return total, 0
    return m, a

def compute_status(today_df: pd.DataFrame, active_articles: pd.DataFrame):
    """
    Returns completion flags and hints.
    """
    total_articles = len(active_articles)
    if total_articles == 0:
        return {"seen": True, "bake_done": False, "close_done": False, "hints": ["Keine aktiven Artikel."]}

    if today_df.empty:
        return {
            "seen": True,
            "bake_done": False,
            "close_done": False,
            "hints": ["Noch keine Eingaben für heute gespeichert."]
        }

    # bake done if at least one of morning/afternoon > 0 for most items? We keep it simple:
    bm = pd.to_numeric(today_df.get("baked_morning", 0), errors="coerce").fillna(0)
    ba = pd.to_numeric(today_df.get("baked_afternoon", 0), errors="coerce").fillna(0)
    filled_bake = ((bm + ba) > 0).sum()

    # close done if submitted_close == TRUE
    sc = today_df.get("submitted_close", "").astype(str).str.lower().isin(["true","1","yes","ja"])
    close_done = bool(sc.any())

    hints = []
    if filled_bake == 0:
        hints.append("Morgens/Nachmittags gebacken ist noch nicht eingetragen.")
    if not close_done:
        hints.append("Tagesabschluss ist noch nicht bestätigt („Fertig für heute“).")

    return {
        "seen": True,
        "bake_done": filled_bake > 0,
        "close_done": close_done,
        "hints": hints
    }

# =========================
# UI
# =========================
st.title("🥐 Bake-Off Planer – geführter Tagesablauf (1 Seite)")

top_left, top_right = st.columns([3, 1])
with top_right:
    if st.button("🔄 Daten neu laden"):
        load_all(force=True)
        st.success("Neu geladen.")
        st.rerun()

# Load once
try:
    tabs = load_all(force=False)
except Exception as e:
    st.error("Google API hat gerade Probleme. Bitte gleich nochmal „Daten neu laden“.")
    st.exception(e)
    st.stop()

cfg = tabs["config"]
articles = tabs["articles"]
daily_log = tabs["daily_log"]
model = tabs["demand_model"]

close_hour = get_cfg(cfg, "close_hour", DEFAULT_CLOSE_HOUR)
alpha = get_cfg(cfg, "alpha", DEFAULT_ALPHA)
waste_target = get_cfg(cfg, "waste_target", DEFAULT_WASTE_TARGET)

# Normalize articles
if articles.empty:
    st.error("Tab 'articles' ist leer. Bitte mindestens 1 Artikel anlegen.")
    st.stop()

articles["sku"] = articles["sku"].astype(str)
articles["name"] = articles.get("name", articles["sku"]).astype(str)
articles["active"] = articles.get("active", "TRUE").astype(str)

active_articles = articles[articles["active"].str.lower().isin(["true","1","yes","ja"])].copy()
active_articles = active_articles.sort_values("name")

if active_articles.empty:
    st.warning("Keine aktiven Artikel. Setze in 'articles' active=TRUE.")
    st.stop()

# Normalize model
model = ensure_model_rows(model, articles)

# Today's and yesterday's dates
today = date.today()
today_s = today.isoformat()
yesterday = today - timedelta(days=1)
yesterday_s = yesterday.isoformat()

# Prepare today's log slice
if daily_log.empty:
    today_log = pd.DataFrame(columns=["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","submitted_close","created_at"])
else:
    daily_log["date"] = daily_log["date"].astype(str)
    daily_log["sku"] = daily_log["sku"].astype(str)
    today_log = daily_log[daily_log["date"] == today_s].copy()

# Status + time guidance
now = now_local()
h = now.hour
time_hint = []
if in_window(h, WIN_MORNING):
    time_hint.append("✅ Typische Zeit für **Morgens gebacken**.")
elif in_window(h, WIN_AFTERNOON):
    time_hint.append("✅ Typische Zeit für **Nachmittags gebacken** (wenn ihr nachbackt).")
elif in_window(h, WIN_CLOSE):
    time_hint.append("✅ Typische Zeit für **Tagesabschluss** (Abschrift & „vor 14 Uhr leer“).")
else:
    time_hint.append("ℹ️ Ungewöhnliche Uhrzeit – Eingaben sind möglich, aber ggf. außerhalb des Standardablaufs.")

status = compute_status(today_log, active_articles)

# -------------------------
# Orientierung / Status Box
# -------------------------
st.markdown("### 🧭 Orientierung")
c1, c2, c3 = st.columns(3)
c1.metric("Heute", today.strftime("%d.%m.%Y"))
c2.metric("Uhrzeit", now.strftime("%H:%M"))
c3.metric("Ladenschluss", f"{int(close_hour)}:00")

st.info(" ".join(time_hint))

# checklist style
chk1, chk2, chk3 = st.columns([1,1,2])
with chk1:
    st.write("**Status**")
    st.write("✅ Planung sichtbar")
    st.write(("✅" if status["bake_done"] else "⬜") + " Backen eingetragen")
    st.write(("✅" if status["close_done"] else "⬜") + " Abschluss bestätigt")
with chk2:
    st.write("**Hinweise**")
    if status["hints"]:
        for x in status["hints"]:
            st.warning(x)
    else:
        st.success("Alles wirkt vollständig ✅")
with chk3:
    st.write("**Regel**")
    st.caption("Die App lernt aus **Gebacken morgens + nachmittags − Abschrift** und der Checkbox **„vor 14 Uhr leer“**. "
               "Eingaben außerhalb der üblichen Zeitfenster lösen nur eine Warnung aus (nicht blockierend).")

st.divider()

# -------------------------
# Schritt 1: Planung
# -------------------------
st.markdown("## 🔵 Schritt 1 – Heute backen wir so")

wd = weekday_name(today)

plan = model[model["weekday"].astype(str) == wd].copy()
plan["sku"] = plan["sku"].astype(str)
plan = plan.merge(active_articles[["sku","name"]], on="sku", how="inner")

plan["demand_est"] = pd.to_numeric(plan.get("demand_est"), errors="coerce").fillna(START_DEMAND_DEFAULT)
plan["morning_share"] = pd.to_numeric(plan.get("morning_share"), errors="coerce").fillna(START_MORNING_SHARE)
plan["waste_rate_est"] = pd.to_numeric(plan.get("waste_rate_est"), errors="coerce").fillna(0.10)

plan["empf_total"] = plan.apply(lambda r: recommend_total(r["demand_est"], r["waste_rate_est"], waste_target), axis=1)
split = plan.apply(lambda r: split_qty(int(r["empf_total"]), float(r["morning_share"])), axis=1)
plan["empf_morgens"] = [m for m, a in split]
plan["empf_nachmittag"] = [a for m, a in split]
plan["modus"] = np.where(plan["empf_nachmittag"] > 0, "2×", "1×")

# Human-friendly reason
def reason_row(r):
    # If morning_share is very high -> afternoons are low
    ms = float(r["morning_share"])
    wr = float(r["waste_rate_est"])
    if ms >= 0.82:
        return "Nachmittagsbedarf eher klein (meist morgens stärker)."
    if wr >= 0.12:
        return "Abschrift zuletzt eher hoch → vorsichtiger."
    return "Normale Empfehlung nach Lernstand."

plan["hinweis"] = plan.apply(reason_row, axis=1)

plan_view = plan[["name","empf_morgens","empf_nachmittag","modus","hinweis"]].sort_values(["modus","empf_morgens"], ascending=[True, False])
st.dataframe(plan_view, use_container_width=True)

st.caption("Hinweis: Empfehlungen werden genauer, wenn täglich Zahlen eingetragen werden und der Tagesabschluss bestätigt wird.")

st.divider()

# -------------------------
# Prepare editable day frame (today)
# -------------------------
def build_today_editor_frame(active_articles: pd.DataFrame, today_log: pd.DataFrame):
    base = active_articles[["sku","name"]].copy()
    base["date"] = today_s

    if today_log.empty:
        base["baked_morning"] = 0
        base["baked_afternoon"] = 0
        base["waste_qty"] = 0
        base["early_empty"] = False
        base["submitted_close"] = False
        return base

    keep = today_log[["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","submitted_close"]].copy()
    merged = base.merge(keep, on=["date","sku"], how="left")

    merged["baked_morning"] = pd.to_numeric(merged.get("baked_morning"), errors="coerce").fillna(0).astype(int)
    merged["baked_afternoon"] = pd.to_numeric(merged.get("baked_afternoon"), errors="coerce").fillna(0).astype(int)
    merged["waste_qty"] = pd.to_numeric(merged.get("waste_qty"), errors="coerce").fillna(0).astype(int)

    ee = merged.get("early_empty", "")
    merged["early_empty"] = ee.astype(str).str.lower().isin(["true","1","yes","ja"])

    sc = merged.get("submitted_close", "")
    merged["submitted_close"] = sc.astype(str).str.lower().isin(["true","1","yes","ja"])

    return merged

day_edit = build_today_editor_frame(active_articles, today_log)

# =========================
# Time window warnings
# =========================
def need_warn(field: str, hour_now: int):
    if field == "baked_morning":
        return not in_window(hour_now, WIN_MORNING)
    if field == "baked_afternoon":
        return not in_window(hour_now, WIN_AFTERNOON)
    if field in ("waste_qty", "early_empty", "submitted_close"):
        return not in_window(hour_now, WIN_CLOSE)
    return False

def confirm_outside_window(msg: str, key: str) -> bool:
    st.warning(msg)
    ok = st.checkbox("Trotzdem fortfahren", key=key)
    return ok

# -------------------------
# Schritt 2: Backen eintragen
# -------------------------
st.markdown("## 🟢 Schritt 2 – Beim Backen eintragen (heute)")

st.caption("Trage direkt ein, was du **jetzt** gebacken hast. Das verhindert „Zettelwirtschaft“ und die App lernt sauber.")

# show compact table editor for baking fields only
bake_df = day_edit[["sku","name","baked_morning","baked_afternoon"]].copy()

edited_bake = st.data_editor(
    bake_df,
    use_container_width=True,
    num_rows="fixed",
    column_config={
        "baked_morning": st.column_config.NumberColumn("Morgens gebacken", min_value=0, step=1),
        "baked_afternoon": st.column_config.NumberColumn("Nachmittags gebacken", min_value=0, step=1),
    }
)

save_bake = st.button("💾 Backen speichern", type="secondary")

if save_bake:
    # time guidance + confirm if outside window
    changed_m = (edited_bake["baked_morning"].astype(int) != bake_df["baked_morning"].astype(int)).any()
    changed_a = (edited_bake["baked_afternoon"].astype(int) != bake_df["baked_afternoon"].astype(int)).any()

    can_proceed = True
    if changed_m and need_warn("baked_morning", h):
        can_proceed = confirm_outside_window("⚠️ Du trägst **Morgens gebacken** außerhalb der üblichen Morgenzeit ein.", "confirm_morning")

    if can_proceed and changed_a and need_warn("baked_afternoon", h):
        can_proceed = confirm_outside_window("⚠️ Du trägst **Nachmittags gebacken** außerhalb der üblichen Nachmittagszeit ein.", "confirm_afternoon")

    if can_proceed:
        # upsert daily_log rows with baking values
        sh = open_sheet()

        out = day_edit.copy()
        out = out.merge(edited_bake[["sku","baked_morning","baked_afternoon"]], on="sku", how="left", suffixes=("", "_new"))
        out["baked_morning"] = out["baked_morning_new"].fillna(out["baked_morning"])
        out["baked_afternoon"] = out["baked_afternoon_new"].fillna(out["baked_afternoon"])
        out = out.drop(columns=[c for c in out.columns if c.endswith("_new")])

        out["created_at"] = pd.Timestamp.utcnow().isoformat()
        # ensure types
        out["baked_morning"] = out["baked_morning"].astype(int)
        out["baked_afternoon"] = out["baked_afternoon"].astype(int)
        out["waste_qty"] = out["waste_qty"].astype(int)
        out["early_empty"] = out["early_empty"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        out["submitted_close"] = out["submitted_close"].apply(lambda x: "TRUE" if bool(x) else "FALSE")

        upsert(sh, "daily_log",
               out[["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","submitted_close","created_at"]],
               key_cols=["date","sku"])

        load_all(force=True)
        st.success("Backen gespeichert ✅")
        st.rerun()

st.divider()

# -------------------------
# Schritt 3: Tagesabschluss
# -------------------------
st.markdown("## 🟡 Schritt 3 – Tagesabschluss (wichtig für morgen)")

st.caption("Hier trägst du **Abschrift** ein und ob ein Artikel **vor 14 Uhr leer** war. "
           "Erst nach Klick auf **„Fertig für heute“** gilt der Tag als abgeschlossen.")

close_df = day_edit[["sku","name","waste_qty","early_empty"]].copy()
edited_close = st.data_editor(
    close_df,
    use_container_width=True,
    num_rows="fixed",
    column_config={
        "waste_qty": st.column_config.NumberColumn("Abschrift (Stück)", min_value=0, step=1),
        "early_empty": st.column_config.CheckboxColumn("Vor 14 Uhr leer"),
    }
)

col_save1, col_save2 = st.columns([1, 2])
with col_save1:
    save_close = st.button("💾 Abschluss speichern", type="secondary")
with col_save2:
    finish_day = st.button("✅ Fertig für heute", type="primary")

def persist_close(submit_close: bool):
    sh = open_sheet()

    out = day_edit.copy()
    out = out.merge(edited_close[["sku","waste_qty","early_empty"]], on="sku", how="left", suffixes=("", "_new"))
    out["waste_qty"] = out["waste_qty_new"].fillna(out["waste_qty"])
    out["early_empty"] = out["early_empty_new"].fillna(out["early_empty"])
    out = out.drop(columns=[c for c in out.columns if c.endswith("_new")])

    if submit_close:
        out["submitted_close"] = True

    # time warning if outside window for close action
    can_proceed = True
    if need_warn("waste_qty", h):
        can_proceed = confirm_outside_window("⚠️ Tagesabschluss wird außerhalb der üblichen Abendzeit eingetragen.", "confirm_close")

    if not can_proceed:
        return False

    out["created_at"] = pd.Timestamp.utcnow().isoformat()
    out["baked_morning"] = out["baked_morning"].astype(int)
    out["baked_afternoon"] = out["baked_afternoon"].astype(int)
    out["waste_qty"] = pd.to_numeric(out["waste_qty"], errors="coerce").fillna(0).astype(int)
    out["early_empty"] = out["early_empty"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
    out["submitted_close"] = out["submitted_close"].apply(lambda x: "TRUE" if bool(x) else "FALSE")

    upsert(sh, "daily_log",
           out[["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","submitted_close","created_at"]],
           key_cols=["date","sku"])

    return True

def learn_from_today_and_persist():
    """
    Learn demand and morning_share from today's confirmed entries.
    Only do this when "Fertig für heute" is pressed.
    """
    sh = open_sheet()
    tabs2 = load_all(force=True)
    mdl = tabs2["demand_model"]
    arts = tabs2["articles"]
    logs = tabs2["daily_log"]

    mdl = ensure_model_rows(mdl, arts)

    # today's rows
    logs["date"] = logs["date"].astype(str)
    logs["sku"] = logs["sku"].astype(str)
    rows = logs[logs["date"] == today_s].copy()
    if rows.empty:
        return

    rows["baked_morning"] = pd.to_numeric(rows.get("baked_morning", 0), errors="coerce").fillna(0)
    rows["baked_afternoon"] = pd.to_numeric(rows.get("baked_afternoon", 0), errors="coerce").fillna(0)
    rows["waste_qty"] = pd.to_numeric(rows.get("waste_qty", 0), errors="coerce").fillna(0)
    rows["early_empty"] = rows.get("early_empty", "").astype(str).str.lower().isin(["true","1","yes","ja"])

    wd_today = weekday_name(today)

    for _, r in rows.iterrows():
        sku = str(r["sku"])
        baked_total = float(r["baked_morning"] + r["baked_afternoon"])
        waste = float(r["waste_qty"])
        sold_est = max(0.0, baked_total - waste)

        mask = (mdl["sku"].astype(str) == sku) & (mdl["weekday"].astype(str) == wd_today)
        if not mask.any():
            continue

        i = mdl.index[mask][0]
        old_demand = float(pd.to_numeric(mdl.at[i, "demand_est"], errors="coerce") if "demand_est" in mdl.columns else START_DEMAND_DEFAULT)
        old_ms = float(pd.to_numeric(mdl.at[i, "morning_share"], errors="coerce") if "morning_share" in mdl.columns else START_MORNING_SHARE)
        old_wr = float(pd.to_numeric(mdl.at[i, "waste_rate_est"], errors="coerce") if "waste_rate_est" in mdl.columns else 0.10)

        # Demand update
        new_demand = (1 - alpha) * old_demand + alpha * sold_est

        # Waste rate update
        wr_obs = (waste / baked_total) if baked_total > 0 else 0.0
        new_wr = (1 - alpha) * old_wr + alpha * wr_obs

        # Morning share update using early_empty + whether afternoon happened
        # - If early_empty == True => morning share likely too low -> increase
        # - If waste high AND early_empty False => morning share too high -> decrease
        ms_obs = old_ms
        if bool(r["early_empty"]):
            ms_obs = min(0.95, old_ms + 0.05)
        else:
            if new_wr >= 0.12:
                ms_obs = max(0.55, old_ms - 0.04)

        new_ms = (1 - alpha) * old_ms + alpha * ms_obs
        new_ms = float(np.clip(new_ms, 0.55, 0.95))

        mdl.at[i, "demand_est"] = float(max(0.0, new_demand))
        mdl.at[i, "morning_share"] = new_ms
        mdl.at[i, "waste_rate_est"] = clamp_float(new_wr, 0.0, 1.0)
        mdl.at[i, "updated_at"] = pd.Timestamp.utcnow().isoformat()

    write_tab(sh, "demand_model", mdl[["sku","weekday","demand_est","morning_share","waste_rate_est","updated_at"]])

if save_close:
    ok = persist_close(submit_close=False)
    if ok:
        load_all(force=True)
        st.success("Abschluss gespeichert ✅")
        st.rerun()

if finish_day:
    # Persist + mark submitted_close + learn
    ok = persist_close(submit_close=True)
    if ok:
        learn_from_today_and_persist()
        load_all(force=True)
        st.success("Tag abgeschlossen ✅ Die App hat gelernt.")
        st.rerun()

st.divider()

# -------------------------
# Optional: Mini-Dashboard (unten)
# -------------------------
st.markdown("## 📊 Kurzer Überblick (optional)")

if daily_log.empty:
    st.info("Noch keine Tagesdaten vorhanden.")
else:
    df = daily_log.copy()
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date_dt"])
    df["waste_qty"] = pd.to_numeric(df.get("waste_qty", 0), errors="coerce").fillna(0)
    df["early_empty"] = df.get("early_empty","").astype(str).str.lower().isin(["true","1","yes","ja"])

    days = st.slider("Zeitraum (Tage)", 7, 56, 14, 7)
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=days)
    df = df[df["date_dt"] >= cutoff].copy()

    name_map = active_articles[["sku","name"]].drop_duplicates()
    df["sku"] = df["sku"].astype(str)
    df = df.merge(name_map, on="sku", how="left")

    c1, c2, c3 = st.columns(3)
    c1.metric("Abschrift gesamt", int(df["waste_qty"].sum()))
    c2.metric("Einträge 'vor 14 leer'", int(df["early_empty"].sum()))
    c3.metric("Tage im Zeitraum", int(df["date_dt"].dt.date.nunique()))

    top_waste = df.groupby(["name"], as_index=False).agg(abschrift=("waste_qty","sum")).sort_values("abschrift", ascending=False).head(10)
    st.write("### Top Abschrift")
    st.dataframe(top_waste, use_container_width=True)

    top_empty = df.groupby(["name"], as_index=False).agg(vor14_leer=("early_empty","sum")).sort_values("vor14_leer", ascending=False).head(10)
    st.write("### Häufig vor 14 Uhr leer")
    st.dataframe(top_empty, use_container_width=True)
