import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, timedelta, time
import time as pytime

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# =========================
# App Config
# =========================
st.set_page_config(page_title="Bake-Off Planer (lernend)", layout="wide")

CLOSE_HOUR_DEFAULT = 21
START_DEMAND_DEFAULT = 20       # Startwert je Artikel/Wochentag, bis Daten da sind
ALPHA_DEFAULT = 0.15            # Lernrate (0.10–0.25 gut)
WASTE_TARGET_DEFAULT = 0.06     # Ziel-Abschriftquote ~6%
MIN_DAYS_FOR_DECISION = 7       # Mindestdaten für 1x vs 2x
EARLY_OOS_HOUR = 19             # "zu früh leer" vor 19:00 -> 2x sinnvoller

# =========================
# Google Sheets
# =========================
@st.cache_resource
def get_gspread_client():
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
def open_spreadsheet():
    sheet_id = str(st.secrets.get("SHEET_ID", "")).strip()
    if not sheet_id:
        st.error("Secrets fehlen: SHEET_ID ist nicht gesetzt.")
        st.stop()
    gc = get_gspread_client()
    return gc.open_by_key(sheet_id)

def ensure_tabs(sh):
    required = {
        "articles": ["sku", "name", "active", "created_at"],
        "daily_log": ["date", "sku", "baked_total", "waste_qty", "oos_time", "notes", "created_at"],
        "demand_model": ["sku", "weekday", "demand_est", "waste_rate_est", "oos_rate_est", "updated_at"],
        "config": ["key", "value"],
    }
    existing = {w.title for w in sh.worksheets()}
    for tab, headers in required.items():
        if tab not in existing:
            sh.add_worksheet(title=tab, rows=4000, cols=max(12, len(headers) + 2))
        ws = sh.worksheet(tab)
        row1 = ws.row_values(1)
        if [x.strip() for x in row1[: len(headers)]] != headers:
            ws.clear()
            ws.update([headers])

def _read_tab_impl(sh, tab: str) -> pd.DataFrame:
    ws = sh.worksheet(tab)

    last_err = None
    for attempt in range(1, 6):  # retry up to 5x
        try:
            values = ws.get_all_values()  # more robust than get_all_records
            if not values:
                headers = ws.row_values(1)
                return pd.DataFrame(columns=headers if headers else [])

            headers = values[0]
            rows = values[1:]

            # If header row is empty, generate fallback columns
            if not any(h.strip() for h in headers):
                headers = [f"col_{i+1}" for i in range(len(headers))]

            # Normalize row lengths to header length
            rows2 = [r[: len(headers)] + [""] * max(0, len(headers) - len(r)) for r in rows]
            return pd.DataFrame(rows2, columns=headers)

        except APIError as e:
            last_err = e
            pytime.sleep(min(2.0 * attempt, 8.0))  # backoff for 429/503 etc.
        except Exception:
            raise

    raise last_err

@st.cache_data(ttl=20)
def read_tab_cached(sheet_id: str, tab: str) -> pd.DataFrame:
    sh = open_spreadsheet()
    return _read_tab_impl(sh, tab)

def read_tab(sh, tab: str) -> pd.DataFrame:
    sheet_id = str(st.secrets.get("SHEET_ID", "")).strip()
    return read_tab_cached(sheet_id, tab)

def write_tab(sh, tab: str, df: pd.DataFrame):
    ws = sh.worksheet(tab)
    df2 = df.copy().replace({np.nan: ""})
    values = [df2.columns.tolist()] + df2.astype(object).values.tolist()
    ws.clear()
    ws.update(values)

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

# =========================
# Helpers
# =========================
def to_weekday_name(d: date) -> str:
    return pd.to_datetime(d.isoformat()).day_name()

def parse_oos_time(s: str):
    """Parse '18:30' -> time(18,30). Return None if invalid/empty."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    try:
        parts = s.split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        if 0 <= h <= 23 and 0 <= m <= 59:
            return time(h, m)
    except Exception:
        return None
    return None

def clamp_int(x, lo=0, hi=10_000):
    try:
        v = int(float(x))
    except Exception:
        v = 0
    return int(max(lo, min(hi, v)))

def clamp_float(x, lo=0.0, hi=1.0):
    try:
        v = float(x)
    except Exception:
        v = 0.0
    return float(max(lo, min(hi, v)))

def get_config_value(cfg: pd.DataFrame, key: str, default):
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

# =========================
# Learning logic
# =========================
def ensure_model_rows(model_df: pd.DataFrame, articles_df: pd.DataFrame) -> pd.DataFrame:
    """Ensure each (sku, weekday) exists with defaults."""
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    if articles_df.empty:
        return pd.DataFrame(columns=["sku", "weekday", "demand_est", "waste_rate_est", "oos_rate_est", "updated_at"])

    articles_df = articles_df.copy()
    articles_df["active_bool"] = articles_df["active"].astype(str).str.lower().isin(["true", "1", "yes", "ja"])
    active = articles_df[articles_df["active_bool"]].copy()
    if active.empty:
        return pd.DataFrame(columns=["sku", "weekday", "demand_est", "waste_rate_est", "oos_rate_est", "updated_at"])

    base = pd.MultiIndex.from_product(
        [active["sku"].astype(str).tolist(), weekdays], names=["sku", "weekday"]
    ).to_frame(index=False)

    if model_df.empty:
        out = base.copy()
        out["demand_est"] = float(START_DEMAND_DEFAULT)
        out["waste_rate_est"] = 0.10
        out["oos_rate_est"] = 0.10
        out["updated_at"] = pd.Timestamp.utcnow().isoformat()
        return out

    out = base.merge(model_df, on=["sku", "weekday"], how="left")
    out["demand_est"] = pd.to_numeric(out["demand_est"], errors="coerce").fillna(float(START_DEMAND_DEFAULT))
    out["waste_rate_est"] = pd.to_numeric(out["waste_rate_est"], errors="coerce").fillna(0.10)
    out["oos_rate_est"] = pd.to_numeric(out["oos_rate_est"], errors="coerce").fillna(0.10)
    out["updated_at"] = out["updated_at"].fillna(pd.Timestamp.utcnow().isoformat())
    return out

def update_model_from_day(model_df: pd.DataFrame, day_row: dict, alpha: float):
    """
    Update demand_est / waste_rate_est / oos_rate_est for (sku, weekday)
    using yesterday's baked_total, waste, oos_time (optional).
    """
    sku = str(day_row["sku"])
    d = pd.to_datetime(day_row["date"], errors="coerce")
    if pd.isna(d):
        return model_df
    d = d.date()
    weekday = to_weekday_name(d)

    baked = clamp_int(day_row.get("baked_total", 0))
    waste = clamp_int(day_row.get("waste_qty", 0))
    oos_t = parse_oos_time(day_row.get("oos_time", ""))

    sold_est = max(0, baked - waste)  # simple but effective

    explicit_oos = oos_t is not None
    weak_oos = (waste == 0 and baked > 0)  # weak hint (optional)
    oos_flag = explicit_oos or weak_oos

    mask = (model_df["sku"].astype(str) == sku) & (model_df["weekday"].astype(str) == weekday)
    if not mask.any():
        return model_df

    i = model_df.index[mask][0]
    old_demand = float(model_df.at[i, "demand_est"])
    old_wr = float(model_df.at[i, "waste_rate_est"])
    old_or = float(model_df.at[i, "oos_rate_est"])

    # Demand update (EMA)
    observed = sold_est
    if explicit_oos:
        # If explicitly out-of-stock, sold_est is lower bound; use baked as minimum demand observed
        observed = max(sold_est, baked)
    new_demand = (1 - alpha) * old_demand + alpha * observed

    # Waste rate update
    waste_rate_obs = (waste / baked) if baked > 0 else 0.0
    new_wr = (1 - alpha) * old_wr + alpha * waste_rate_obs

    # OOS rate update
    oos_obs = 1.0 if oos_flag else 0.0
    new_or = (1 - alpha) * old_or + alpha * oos_obs

    model_df.at[i, "demand_est"] = max(0.0, float(new_demand))
    model_df.at[i, "waste_rate_est"] = clamp_float(new_wr, 0.0, 1.0)
    model_df.at[i, "oos_rate_est"] = clamp_float(new_or, 0.0, 1.0)
    model_df.at[i, "updated_at"] = pd.Timestamp.utcnow().isoformat()
    return model_df

def decide_bake_frequency(sku: str, log_df: pd.DataFrame):
    """
    Decide 1x vs 2x for a SKU based on recent history.
    - Needs at least MIN_DAYS_FOR_DECISION entries for that SKU (last 28 days).
    - 2x if often early OOS (or frequent "waste==0") and waste low.
    """
    if log_df.empty:
        return {"mode": "1x", "reason": "Noch keine Daten", "morning_share": 1.0}

    dfx = log_df[log_df["sku"].astype(str) == str(sku)].copy()
    if dfx.empty:
        return {"mode": "1x", "reason": "Noch keine Daten", "morning_share": 1.0}

    dfx["date"] = pd.to_datetime(dfx["date"], errors="coerce")
    dfx = dfx.dropna(subset=["date"]).sort_values("date").tail(28)

    if len(dfx) < MIN_DAYS_FOR_DECISION:
        return {"mode": "1x", "reason": f"Zu wenig Daten ({len(dfx)}/{MIN_DAYS_FOR_DECISION})", "morning_share": 1.0}

    baked = pd.to_numeric(dfx.get("baked_total", 0), errors="coerce").fillna(0).clip(lower=0)
    waste = pd.to_numeric(dfx.get("waste_qty", 0), errors="coerce").fillna(0).clip(lower=0)

    waste_rate = np.where(baked > 0, (waste / baked), 0.0)
    avg_waste_rate = float(np.mean(waste_rate))

    def is_early(t):
        tt = parse_oos_time(t)
        return tt is not None and tt < time(EARLY_OOS_HOUR, 0)

    oos_time_series = dfx.get("oos_time", "").astype(str)
    early_oos = oos_time_series.apply(is_early)
    early_oos_rate = float(np.mean(early_oos.astype(int)))

    # weak-oos: waste==0 and baked>0 and no explicit oos_time
    weak_oos = (waste == 0) & (baked > 0) & oos_time_series.str.strip().eq("")
    weak_oos_rate = float(np.mean(weak_oos.astype(int)))

    # Decision rules
    if (early_oos_rate >= 0.25 or weak_oos_rate >= 0.35) and avg_waste_rate <= 0.08:
        morning_share = 0.75 if early_oos_rate >= 0.35 else 0.65
        return {"mode": "2x", "reason": "Oft zu früh leer & wenig Abschrift", "morning_share": morning_share}

    if avg_waste_rate >= 0.12 and early_oos_rate <= 0.10:
        return {"mode": "1x", "reason": "Abschrift hoch, selten früh leer", "morning_share": 1.0}

    return {"mode": "1x", "reason": "Kein klarer Vorteil für 2x (noch)", "morning_share": 1.0}

def recommend_today_qty(demand_est: float, waste_rate_est: float, oos_rate_est: float, waste_target: float):
    """
    Convert learned demand into bake recommendation (pieces).
    - Reduce slightly if waste above target
    - Increase slightly if OOS frequent
    """
    base = max(0.0, float(demand_est))
    waste_penalty = max(0.0, float(waste_rate_est) - float(waste_target))
    oos_boost = float(oos_rate_est)

    adj = 1.0 - 0.6 * waste_penalty + 0.12 * oos_boost
    adj = float(np.clip(adj, 0.75, 1.30))
    return int(np.round(base * adj))

# =========================
# App Start
# =========================
sh = open_spreadsheet()
ensure_tabs(sh)

cfg = read_tab(sh, "config")
close_hour = get_config_value(cfg, "close_hour", CLOSE_HOUR_DEFAULT)
alpha = get_config_value(cfg, "alpha", ALPHA_DEFAULT)
waste_target = get_config_value(cfg, "waste_target", WASTE_TARGET_DEFAULT)

articles = read_tab(sh, "articles")
daily_log = read_tab(sh, "daily_log")
model = read_tab(sh, "demand_model")

# Normalize article fields
if articles.empty:
    articles = pd.DataFrame(columns=["sku", "name", "active", "created_at"])
else:
    articles["sku"] = articles["sku"].astype(str)
    articles["name"] = articles["name"].astype(str)
    articles["active"] = articles["active"].astype(str)

# Normalize log fields
if daily_log.empty:
    daily_log = pd.DataFrame(columns=["date", "sku", "baked_total", "waste_qty", "oos_time", "notes", "created_at"])
else:
    daily_log["sku"] = daily_log["sku"].astype(str)

# Ensure model rows exist (in memory)
model = ensure_model_rows(model, articles)

# =========================
# UI
# =========================
st.title("🥐 Bake-Off Planer (lernend – ohne Vergangenheitsdaten)")

st.sidebar.header("Navigation")
page = st.sidebar.radio(
    "Seite",
    ["Planung (heute)", "Eingabe (gestern)", "Dashboard", "Artikel", "Einstellungen"],
    index=0,
)

# -------------------------
# Einstellungen
# -------------------------
if page == "Einstellungen":
    st.subheader("Einstellungen")

    c1, c2, c3 = st.columns(3)
    with c1:
        close_hour_new = st.number_input("Ladenschluss (Stunde)", 17, 23, int(close_hour), 1)
    with c2:
        alpha_new = st.slider("Lernrate (alpha)", 0.05, 0.30, float(alpha), 0.01)
    with c3:
        waste_target_new = st.slider("Ziel-Abschriftquote", 0.00, 0.20, float(waste_target), 0.01)

    if st.button("💾 Speichern", type="primary"):
        cfg_new = pd.DataFrame(
            [
                {"key": "close_hour", "value": int(close_hour_new)},
                {"key": "alpha", "value": float(alpha_new)},
                {"key": "waste_target", "value": float(waste_target_new)},
            ]
        )
        upsert_tab(sh, "config", cfg_new, key_cols=["key"])
        st.success("Gespeichert. Bitte App neu laden (Reboot oder Browser-Refresh).")

    st.stop()

# -------------------------
# Artikel
# -------------------------
if page == "Artikel":
    st.subheader("Artikel anlegen / verwalten")

    c1, c2 = st.columns([2, 1])
    with c1:
        sku_new = st.text_input("PLU / Artikelnummer (SKU)", value="").strip()
    with c2:
        active_new = st.checkbox("Aktiv", value=True)

    name_new = st.text_input("Artikelname", value="").strip()

    if st.button("➕ Artikel speichern", type="primary"):
        if not sku_new or not name_new:
            st.error("Bitte SKU und Artikelname ausfüllen.")
        else:
            row = pd.DataFrame(
                [
                    {
                        "sku": sku_new,
                        "name": name_new,
                        "active": "TRUE" if active_new else "FALSE",
                        "created_at": pd.Timestamp.utcnow().isoformat(),
                    }
                ]
            )
            upsert_tab(sh, "articles", row, key_cols=["sku"])

            # ensure model rows (write back so planning works immediately)
            articles_latest = read_tab(sh, "articles")
            model_latest = read_tab(sh, "demand_model")
            model_latest = ensure_model_rows(model_latest, articles_latest)
            write_tab(sh, "demand_model", model_latest[["sku", "weekday", "demand_est", "waste_rate_est", "oos_rate_est", "updated_at"]])

            st.success("Artikel gespeichert.")
            st.rerun()

    st.divider()
    st.write("### Aktive Artikel")
    articles_latest = read_tab(sh, "articles")
    if articles_latest.empty:
        st.info("Noch keine Artikel angelegt.")
        st.stop()

    art = articles_latest.copy()
    art["sku"] = art["sku"].astype(str)
    art["name"] = art["name"].astype(str)
    art["active"] = art["active"].astype(str).str.lower().isin(["true", "1", "yes", "ja"])

    edited = st.data_editor(
        art[["sku", "name", "active"]],
        use_container_width=True,
        num_rows="fixed",
    )

    if st.button("💾 Änderungen speichern"):
        out = edited.copy()
        out["active"] = out["active"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
        out["created_at"] = pd.Timestamp.utcnow().isoformat()
        upsert_tab(sh, "articles", out[["sku", "name", "active", "created_at"]], key_cols=["sku"])

        # also ensure demand_model contains rows for active articles
        articles_latest = read_tab(sh, "articles")
        model_latest = read_tab(sh, "demand_model")
        model_latest = ensure_model_rows(model_latest, articles_latest)
        write_tab(sh, "demand_model", model_latest[["sku", "weekday", "demand_est", "waste_rate_est", "oos_rate_est", "updated_at"]])

        st.success("Aktualisiert.")
        st.rerun()

    st.stop()

# -------------------------
# Eingabe (gestern)
# -------------------------
if page == "Eingabe (gestern)":
    st.subheader("Eingabe (gestern) – damit die App lernt")

    articles_latest = read_tab(sh, "articles")
    if articles_latest.empty or not articles_latest["active"].astype(str).str.lower().isin(["true", "1", "yes", "ja"]).any():
        st.info("Lege zuerst Artikel an (Seite: Artikel).")
        st.stop()

    entry_date = st.date_input("Tag der Eingabe (standard: gestern)", value=date.today() - timedelta(days=1))
    st.caption("Pro Artikel: **Gebacken gesamt**, **Abschrift**, optional **leer ab Uhrzeit** (wenn ausverkauft).")

    active_articles = articles_latest[articles_latest["active"].astype(str).str.lower().isin(["true", "1", "yes", "ja"])].copy()
    active_articles["sku"] = active_articles["sku"].astype(str)
    active_articles["name"] = active_articles["name"].astype(str)
    active_articles = active_articles.sort_values("name")

    base = active_articles[["sku", "name"]].copy()
    base["date"] = entry_date.isoformat()

    daily_log_latest = read_tab(sh, "daily_log")
    if not daily_log_latest.empty:
        daily_log_latest["date"] = daily_log_latest["date"].astype(str)
        daily_log_latest["sku"] = daily_log_latest["sku"].astype(str)
        existing = daily_log_latest[daily_log_latest["date"] == entry_date.isoformat()].copy()
    else:
        existing = pd.DataFrame(columns=["date", "sku", "baked_total", "waste_qty", "oos_time", "notes", "created_at"])

    edit = base.merge(existing, on=["date", "sku"], how="left")
    edit["baked_total"] = pd.to_numeric(edit.get("baked_total"), errors="coerce").fillna(0).astype(int)
    edit["waste_qty"] = pd.to_numeric(edit.get("waste_qty"), errors="coerce").fillna(0).astype(int)
    edit["oos_time"] = edit.get("oos_time", "").fillna("")
    edit["notes"] = edit.get("notes", "").fillna("")

    st.write("### Tageswerte")
    edited = st.data_editor(
        edit[["sku", "name", "baked_total", "waste_qty", "oos_time", "notes"]],
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "baked_total": st.column_config.NumberColumn("Gebacken gesamt", min_value=0, step=1),
            "waste_qty": st.column_config.NumberColumn("Abschrift", min_value=0, step=1),
            "oos_time": st.column_config.TextColumn("Leer ab (HH:MM, optional)"),
            "notes": st.column_config.TextColumn("Notiz"),
        },
    )

    if st.button("💾 Speichern & Lernen", type="primary"):
        to_save = edited.copy()
        to_save["date"] = entry_date.isoformat()
        to_save["sku"] = to_save["sku"].astype(str)
        to_save["created_at"] = pd.Timestamp.utcnow().isoformat()

        upsert_tab(
            sh,
            "daily_log",
            to_save[["date", "sku", "baked_total", "waste_qty", "oos_time", "notes", "created_at"]],
            key_cols=["date", "sku"],
        )

        # Learn/update demand model
        model_latest = read_tab(sh, "demand_model")
        model_latest["sku"] = model_latest["sku"].astype(str)
        model_latest["weekday"] = model_latest["weekday"].astype(str)
        model_latest = ensure_model_rows(model_latest, articles_latest)

        for _, r in to_save.iterrows():
            baked = clamp_int(r["baked_total"])
            waste = clamp_int(r["waste_qty"])
            oos_s = str(r.get("oos_time", "")).strip()
            if baked <= 0 and waste <= 0 and oos_s == "":
                continue
            model_latest = update_model_from_day(
                model_latest,
                {
                    "date": entry_date.isoformat(),
                    "sku": str(r["sku"]),
                    "baked_total": baked,
                    "waste_qty": waste,
                    "oos_time": oos_s,
                },
                alpha=float(alpha),
            )

        write_tab(sh, "demand_model", model_latest[["sku", "weekday", "demand_est", "waste_rate_est", "oos_rate_est", "updated_at"]])

        st.success("Gespeichert. Die App hat gelernt ✅")
        st.rerun()

    st.stop()

# -------------------------
# Dashboard
# -------------------------
if page == "Dashboard":
    st.subheader("📊 Dashboard (Markt-Übersicht)")

    daily_log_latest = read_tab(sh, "daily_log")
    articles_latest = read_tab(sh, "articles")
    model_latest = read_tab(sh, "demand_model")

    if daily_log_latest.empty:
        st.info("Noch keine Tagesdaten vorhanden. Erst ein paar Tage eintragen, dann zeigt das Dashboard Trends.")
        st.stop()

    # Normalize
    daily_log_latest["date"] = pd.to_datetime(daily_log_latest["date"], errors="coerce")
    daily_log_latest = daily_log_latest.dropna(subset=["date"])
    daily_log_latest["sku"] = daily_log_latest["sku"].astype(str)

    if articles_latest.empty:
        articles_latest = pd.DataFrame({"sku": daily_log_latest["sku"].unique(), "name": daily_log_latest["sku"].unique()})
    else:
        articles_latest["sku"] = articles_latest["sku"].astype(str)
        articles_latest["name"] = articles_latest.get("name", articles_latest["sku"]).astype(str)

    days = st.slider("Zeitraum (Tage)", 7, 56, 14, 7)
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=days)
    df = daily_log_latest[daily_log_latest["date"] >= cutoff].copy()

    df["baked_total"] = pd.to_numeric(df.get("baked_total", 0), errors="coerce").fillna(0)
    df["waste_qty"] = pd.to_numeric(df.get("waste_qty", 0), errors="coerce").fillna(0)
    df["oos_flag"] = df.get("oos_time", "").astype(str).str.strip().ne("")

    df = df.merge(articles_latest[["sku", "name"]], on="sku", how="left")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Tage im Zeitraum", int(df["date"].dt.date.nunique()))
    with c2:
        st.metric("Gesamt gebacken", int(df["baked_total"].sum()))
    with c3:
        st.metric("Gesamt Abschrift", int(df["waste_qty"].sum()))
    with c4:
        st.metric("OOS-Ereignisse", int(df["oos_flag"].sum()))

    st.divider()

    top_waste = (
        df.groupby(["sku", "name"], as_index=False)
        .agg(abschrift=("waste_qty", "sum"), gebacken=("baked_total", "sum"), tage=("date", "nunique"))
    )
    top_waste["abschrift_quote"] = np.where(
        top_waste["gebacken"] > 0, top_waste["abschrift"] / top_waste["gebacken"], 0.0
    )
    top_waste = top_waste.sort_values("abschrift", ascending=False).head(15)

    st.write("### 🔥 Top Abschriften")
    st.dataframe(top_waste, use_container_width=True)

    top_oos = (
        df.groupby(["sku", "name"], as_index=False)
        .agg(oos_rate=("oos_flag", "mean"), oos_events=("oos_flag", "sum"), tage=("date", "nunique"))
    )
    top_oos = top_oos.sort_values("oos_rate", ascending=False).head(15)

    st.write("### 🧯 Häufig leer / Out-of-Stock")
    st.dataframe(top_oos, use_container_width=True)

    if not model_latest.empty:
        model_latest["sku"] = model_latest["sku"].astype(str)
        model_latest["weekday"] = model_latest["weekday"].astype(str)
        model_latest["demand_est"] = pd.to_numeric(model_latest.get("demand_est", 0), errors="coerce").fillna(0.0)
        model_latest["waste_rate_est"] = pd.to_numeric(model_latest.get("waste_rate_est", 0), errors="coerce").fillna(0.0)
        model_latest["oos_rate_est"] = pd.to_numeric(model_latest.get("oos_rate_est", 0), errors="coerce").fillna(0.0)

        today_wd = pd.Timestamp.today().day_name()
        snap = model_latest[model_latest["weekday"] == today_wd].copy()
        snap = snap.merge(articles_latest[["sku", "name"]], on="sku", how="left")
        snap = snap.sort_values("demand_est", ascending=False).head(20)

        st.write(f"### 📈 Bedarfsschätzung (heute: {today_wd})")
        st.dataframe(snap[["sku", "name", "demand_est", "waste_rate_est", "oos_rate_est"]], use_container_width=True)

    st.stop()

# -------------------------
# Planung (heute)
# -------------------------
if page == "Planung (heute)":
    st.subheader("Planung (heute)")

    articles_latest = read_tab(sh, "articles")
    if articles_latest.empty or not articles_latest["active"].astype(str).str.lower().isin(["true", "1", "yes", "ja"]).any():
        st.info("Lege zuerst Artikel an (Seite: Artikel).")
        st.stop()

    plan_date = st.date_input("Datum", value=date.today())
    weekday = to_weekday_name(plan_date)
    st.caption(
        f"Ladenschluss: **{int(close_hour)}:00** | "
        f"Lernrate alpha: **{float(alpha):.2f}** | "
        f"Ziel-Abschrift: **{float(waste_target)*100:.0f}%** | "
        f"Wochentag: **{weekday}**"
    )

    active_articles = articles_latest[articles_latest["active"].astype(str).str.lower().isin(["true", "1", "yes", "ja"])].copy()
    active_articles["sku"] = active_articles["sku"].astype(str)
    active_articles["name"] = active_articles["name"].astype(str)
    active_articles = active_articles.sort_values("name")

    daily_log_latest = read_tab(sh, "daily_log")
    if not daily_log_latest.empty:
        daily_log_latest["sku"] = daily_log_latest["sku"].astype(str)

    model_latest = read_tab(sh, "demand_model")
    if model_latest.empty:
        model_latest = ensure_model_rows(model_latest, articles_latest)
        write_tab(sh, "demand_model", model_latest[["sku", "weekday", "demand_est", "waste_rate_est", "oos_rate_est", "updated_at"]])

    model_latest["sku"] = model_latest["sku"].astype(str)
    model_latest["weekday"] = model_latest["weekday"].astype(str)
    model_latest["demand_est"] = pd.to_numeric(model_latest.get("demand_est"), errors="coerce").fillna(float(START_DEMAND_DEFAULT))
    model_latest["waste_rate_est"] = pd.to_numeric(model_latest.get("waste_rate_est"), errors="coerce").fillna(0.10)
    model_latest["oos_rate_est"] = pd.to_numeric(model_latest.get("oos_rate_est"), errors="coerce").fillna(0.10)

    recs = []
    for _, a in active_articles.iterrows():
        sku = str(a["sku"])
        name = str(a["name"])

        row = model_latest[(model_latest["sku"] == sku) & (model_latest["weekday"] == weekday)]
        if row.empty:
            demand_est = float(START_DEMAND_DEFAULT)
            wr = 0.10
            orr = 0.10
        else:
            demand_est = float(row.iloc[0]["demand_est"])
            wr = float(row.iloc[0]["waste_rate_est"])
            orr = float(row.iloc[0]["oos_rate_est"])

        qty = recommend_today_qty(demand_est, wr, orr, float(waste_target))

        freq = decide_bake_frequency(sku, daily_log_latest)
        if freq["mode"] == "2x":
            morning = int(np.round(qty * freq["morning_share"]))
            afternoon = max(0, qty - morning)
        else:
            morning = qty
            afternoon = 0

        recs.append(
            {
                "sku": sku,
                "name": name,
                "empfehlung_total": qty,
                "empfehlung_morgens": morning,
                "empfehlung_nachmittag": afternoon,
                "backmodus": "2×" if freq["mode"] == "2x" else "1×",
                "grund": freq["reason"],
                "gelernt_bedarf": int(np.round(demand_est)),
                "abschrift_est": f"{wr*100:.0f}%",
                "oos_est": f"{orr*100:.0f}%",
            }
        )

    df = pd.DataFrame(recs)
    if df.empty:
        st.info("Keine aktiven Artikel.")
        st.stop()

    df = df.sort_values(["backmodus", "empfehlung_total"], ascending=[True, False])

    c1, c2 = st.columns([2, 1])
    with c1:
        q = st.text_input("Suche (Name/SKU)", value="")
    with c2:
        only_positive = st.checkbox("Nur Empfehlung > 0", value=True)

    view = df.copy()
    if q.strip():
        qq = q.strip().lower()
        view = view[(view["sku"].str.lower().str.contains(qq)) | (view["name"].str.lower().str.contains(qq))]
    if only_positive:
        view = view[view["empfehlung_total"] > 0]

    st.write("### Backempfehlung")
    st.dataframe(
        view[
            [
                "sku",
                "name",
                "empfehlung_total",
                "empfehlung_morgens",
                "empfehlung_nachmittag",
                "backmodus",
                "grund",
                "gelernt_bedarf",
                "abschrift_est",
                "oos_est",
            ]
        ],
        use_container_width=True,
    )

    st.info(
        "Lernlogik: Die App lernt **Tagesbedarf** aus (Gebacken − Abschrift). "
        "Wenn ihr oft **zu früh leer** seid und wenig Abschrift habt, empfiehlt sie **2× Backen** "
        "und teilt automatisch in morgens/nachmittags auf."
    )

    st.stop()
