import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta, time
import gspread
from google.oauth2.service_account import Credentials

# ----------------------------
# App Config
# ----------------------------
st.set_page_config(page_title="Bake-Off Planer (lernend)", layout="wide")

CLOSE_HOUR_DEFAULT = 21
START_DEMAND_DEFAULT = 20  # Startwert je Artikel/Wochentag, bis Daten da sind
ALPHA = 0.15               # Lernrate (0.10–0.25 gut)
WASTE_TARGET = 0.06        # Ziel-Abschriftquote ~6% (einstellbar)
MIN_DAYS_FOR_DECISION = 7  # Mindestdaten für 1x vs 2x
EARLY_OOS_HOUR = 19        # "zu früh leer" vor 19:00 -> 2x sinnvoller

# ----------------------------
# Google Sheets
# ----------------------------
@st.cache_resource
def get_gspread_client():
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
    sheet_id = st.secrets.get("SHEET_ID", "").strip()
    if not sheet_id:
        st.error("SHEET_ID fehlt in Streamlit Secrets.")
        st.stop()
    return get_gspread_client().open_by_key(sheet_id)

def ensure_tabs(sh):
    required = {
        "articles": ["sku","name","active","created_at"],
        "daily_log": ["date","sku","baked_total","waste_qty","oos_time","notes","created_at"],
        "demand_model": ["sku","weekday","demand_est","waste_rate_est","oos_rate_est","updated_at"],
        "config": ["key","value"],
    }
    existing = {w.title for w in sh.worksheets()}
    for tab, headers in required.items():
        if tab not in existing:
            sh.add_worksheet(title=tab, rows=4000, cols=max(10, len(headers) + 2))
        ws = sh.worksheet(tab)
        row1 = ws.row_values(1)
        if [x.strip() for x in row1[:len(headers)]] != headers:
            ws.clear()
            ws.update([headers])

def read_tab(sh, tab: str) -> pd.DataFrame:
    ws = sh.worksheet(tab)
    rows = ws.get_all_records()
    if not rows:
        headers = ws.row_values(1)
        return pd.DataFrame(columns=headers if headers else [])
    return pd.DataFrame(rows)

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

# ----------------------------
# Helpers
# ----------------------------
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
    except:
        return None
    return None

def clamp_int(x, lo=0, hi=10_000):
    try:
        v = int(float(x))
    except:
        v = 0
    return int(max(lo, min(hi, v)))

def clamp_float(x, lo=0.0, hi=1.0):
    try:
        v = float(x)
    except:
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
    except:
        return default
    return v

# ----------------------------
# Learning logic
# ----------------------------
def ensure_model_rows(model_df: pd.DataFrame, articles_df: pd.DataFrame) -> pd.DataFrame:
    """Ensure each (sku, weekday) exists with defaults."""
    weekdays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    articles_active = articles_df[articles_df["active"].astype(str).str.lower().isin(["true","1","yes","ja"])].copy()
    if articles_active.empty:
        return pd.DataFrame(columns=["sku","weekday","demand_est","waste_rate_est","oos_rate_est","updated_at"])

    base = pd.MultiIndex.from_product([articles_active["sku"].astype(str).tolist(), weekdays], names=["sku","weekday"]).to_frame(index=False)
    if model_df.empty:
        model_df = base.copy()
        model_df["demand_est"] = START_DEMAND_DEFAULT
        model_df["waste_rate_est"] = 0.10
        model_df["oos_rate_est"] = 0.10
        model_df["updated_at"] = pd.Timestamp.utcnow().isoformat()
        return model_df

    out = base.merge(model_df, on=["sku","weekday"], how="left")
    out["demand_est"] = pd.to_numeric(out["demand_est"], errors="coerce").fillna(START_DEMAND_DEFAULT)
    out["waste_rate_est"] = pd.to_numeric(out["waste_rate_est"], errors="coerce").fillna(0.10)
    out["oos_rate_est"] = pd.to_numeric(out["oos_rate_est"], errors="coerce").fillna(0.10)
    out["updated_at"] = out["updated_at"].fillna(pd.Timestamp.utcnow().isoformat())
    return out

def update_model_from_day(model_df: pd.DataFrame, day_row: dict, alpha: float, close_hour: int):
    """
    Update demand_est/waste_rate_est/oos_rate_est for (sku, weekday)
    using yesterday's baked_total, waste, oos_time (optional).
    """
    sku = str(day_row["sku"])
    d = pd.to_datetime(day_row["date"]).date()
    weekday = to_weekday_name(d)

    baked = clamp_int(day_row.get("baked_total", 0))
    waste = clamp_int(day_row.get("waste_qty", 0))
    oos_t = parse_oos_time(day_row.get("oos_time", ""))

    sold_est = max(0, baked - waste)  # wichtigste Schätzung

    # oos signal: if user entered time OR waste==0 and baked>0 and likely hit the ceiling
    # We use either explicit oos_time OR "waste==0 and baked>0" as a weak oos hint.
    explicit_oos = oos_t is not None
    weak_oos = (waste == 0 and baked > 0)
    oos_flag = explicit_oos or weak_oos

    # Update model row
    mask = (model_df["sku"].astype(str) == sku) & (model_df["weekday"].astype(str) == weekday)
    if not mask.any():
        return model_df

    i = model_df.index[mask][0]
    old_demand = float(model_df.at[i, "demand_est"])
    old_wr = float(model_df.at[i, "waste_rate_est"])
    old_or = float(model_df.at[i, "oos_rate_est"])

    # Demand update (EMA) - but if we likely stocked out, sold_est may be a lower bound.
    # In that case, nudge toward "baked" instead of sold_est.
    observed = sold_est
    if explicit_oos:
        observed = max(sold_est, baked)  # sold was at least what you baked
    new_demand = (1 - alpha) * old_demand + alpha * observed

    # Waste rate update
    waste_rate_obs = (waste / baked) if baked > 0 else 0.0
    new_wr = (1 - alpha) * old_wr + alpha * waste_rate_obs

    # OOS rate update (binary)
    oos_obs = 1.0 if oos_flag else 0.0
    new_or = (1 - alpha) * old_or + alpha * oos_obs

    model_df.at[i, "demand_est"] = max(0.0, new_demand)
    model_df.at[i, "waste_rate_est"] = clamp_float(new_wr, 0.0, 1.0)
    model_df.at[i, "oos_rate_est"] = clamp_float(new_or, 0.0, 1.0)
    model_df.at[i, "updated_at"] = pd.Timestamp.utcnow().isoformat()
    return model_df

def decide_bake_frequency(sku: str, log_df: pd.DataFrame, close_hour: int):
    """
    Decide 1x vs 2x for a SKU based on recent history.
    Rules (simple & market-friendly):
      - Need at least MIN_DAYS_FOR_DECISION logs for that SKU.
      - If "too early OOS" happens often and waste is low -> 2x.
      - If waste is high and OOS rare -> 1x.
    """
    dfx = log_df[log_df["sku"].astype(str) == str(sku)].copy()
    if dfx.empty:
        return {"mode":"1x", "reason":"Noch keine Daten", "morning_share":1.0}

    # last 28 days for stability
    dfx["date"] = pd.to_datetime(dfx["date"], errors="coerce")
    dfx = dfx.dropna(subset=["date"]).sort_values("date")
    dfx = dfx.tail(28)

    if len(dfx) < MIN_DAYS_FOR_DECISION:
        return {"mode":"1x", "reason":f"Zu wenig Daten ({len(dfx)}/{MIN_DAYS_FOR_DECISION})", "morning_share":1.0}

    baked = pd.to_numeric(dfx["baked_total"], errors="coerce").fillna(0).clip(lower=0)
    waste = pd.to_numeric(dfx["waste_qty"], errors="coerce").fillna(0).clip(lower=0)
    waste_rate = np.where(baked > 0, (waste / baked), 0.0)
    avg_waste_rate = float(np.mean(waste_rate))

    # early-oos count (explicit time)
    def is_early(t):
        tt = parse_oos_time(t)
        if tt is None:
            return False
        return tt < time(EARLY_OOS_HOUR, 0)

    early_oos = dfx["oos_time"].apply(is_early)
    early_oos_rate = float(np.mean(early_oos.astype(int)))

    # weak-oos: waste==0 and baked>0 (not reliable, but helps)
    weak_oos = (waste == 0) & (baked > 0) & dfx["oos_time"].astype(str).str.strip().eq("")
    weak_oos_rate = float(np.mean(weak_oos.astype(int)))

    # decision
    # 2x if early-oos shows up often OR weak-oos is frequent, while waste stays low.
    if (early_oos_rate >= 0.25 or weak_oos_rate >= 0.35) and avg_waste_rate <= 0.08:
        # choose morning share based on how early
        morning_share = 0.75 if early_oos_rate >= 0.35 else 0.65
        return {"mode":"2x", "reason":"Oft zu früh leer & wenig Abschrift", "morning_share":morning_share}

    # 1x if waste high and early oos rare
    if avg_waste_rate >= 0.12 and early_oos_rate <= 0.10:
        return {"mode":"1x", "reason":"Abschrift hoch, selten früh leer", "morning_share":1.0}

    # default conservative
    return {"mode":"1x", "reason":"Kein klarer Vorteil für 2x (noch)", "morning_share":1.0}

def recommend_today_qty(demand_est: float, waste_rate_est: float, oos_rate_est: float, waste_target: float):
    """
    Convert learned demand into bake recommendation.
    Intuition:
      - If waste is above target -> reduce slightly
      - If oos is frequent -> increase slightly
    """
    base = max(0.0, float(demand_est))

    # simple adjustment
    waste_penalty = (waste_rate_est - waste_target)  # >0 means too much waste
    oos_boost = oos_rate_est                          # 0..1

    adj = 1.0 - 0.6 * max(0.0, waste_penalty) + 0.12 * oos_boost
    adj = float(np.clip(adj, 0.75, 1.30))

    return int(np.round(base * adj))

# ----------------------------
# App Start
# ----------------------------
sh = open_spreadsheet()
ensure_tabs(sh)

cfg = read_tab(sh, "config")
close_hour = get_config_value(cfg, "close_hour", CLOSE_HOUR_DEFAULT)
alpha = get_config_value(cfg, "alpha", ALPHA)
waste_target = get_config_value(cfg, "waste_target", WASTE_TARGET)

articles = read_tab(sh, "articles")
daily_log = read_tab(sh, "daily_log")
model = read_tab(sh, "demand_model")

# Normalize booleans
if not articles.empty:
    articles["sku"] = articles["sku"].astype(str)
    articles["name"] = articles["name"].astype(str)
    articles["active"] = articles["active"].astype(str)
else:
    articles = pd.DataFrame(columns=["sku","name","active","created_at"])

if not daily_log.empty:
    daily_log["sku"] = daily_log["sku"].astype(str)
else:
    daily_log = pd.DataFrame(columns=["date","sku","baked_total","waste_qty","oos_time","notes","created_at"])

model = ensure_model_rows(model, articles)

st.title("🥐 Bake-Off Planer (lernend – ohne Vergangenheitsdaten)")

# ----------------------------
# Sidebar
# ----------------------------
st.sidebar.header("Navigation")
page = st.sidebar.radio("Seite", ["Planung (heute)", "Eingabe (gestern)", "Artikel", "Einstellungen"], index=0)

# ----------------------------
# Page: Einstellungen
# ----------------------------
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
        cfg_new = pd.DataFrame([
            {"key":"close_hour","value":int(close_hour_new)},
            {"key":"alpha","value":float(alpha_new)},
            {"key":"waste_target","value":float(waste_target_new)},
        ])
        upsert_tab(sh, "config", cfg_new, key_cols=["key"])
        st.success("Gespeichert. Bitte App neu laden (oder Reboot).")
    st.stop()

# ----------------------------
# Page: Artikel
# ----------------------------
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
            row = pd.DataFrame([{
                "sku": sku_new,
                "name": name_new,
                "active": "TRUE" if active_new else "FALSE",
                "created_at": pd.Timestamp.utcnow().isoformat(),
            }])
            upsert_tab(sh, "articles", row, key_cols=["sku"])
            st.success("Artikel gespeichert.")
            st.rerun()

    st.divider()
    st.write("### Aktive Artikel")
    if articles.empty:
        st.info("Noch keine Artikel angelegt.")
    else:
        # show editor for active toggle and name
        art = articles.copy()
        art["active"] = art["active"].astype(str).str.lower().isin(["true","1","yes","ja"])
        edited = st.data_editor(
            art[["sku","name","active"]],
            use_container_width=True,
            num_rows="fixed",
        )
        if st.button("💾 Änderungen speichern"):
            out = edited.copy()
            out["active"] = out["active"].apply(lambda x: "TRUE" if bool(x) else "FALSE")
            out["created_at"] = pd.Timestamp.utcnow().isoformat()
            upsert_tab(sh, "articles", out[["sku","name","active","created_at"]], key_cols=["sku"])
            st.success("Aktualisiert.")
            st.rerun()

    st.stop()

# ----------------------------
# Page: Eingabe (gestern)
# ----------------------------
if page == "Eingabe (gestern)":
    st.subheader("Eingabe (gestern) – damit die App lernt")

    if articles.empty or not (articles["active"].astype(str).str.lower().isin(["true","1","yes","ja"]).any()):
        st.info("Lege zuerst Artikel an (Seite: Artikel).")
        st.stop()

    entry_date = st.date_input("Tag der Eingabe (standard: gestern)", value=date.today() - timedelta(days=1))
    st.caption("Pro Artikel: **Gebacken gesamt**, **Abschrift**, optional **leer ab Uhrzeit** (wenn ausverkauft).")

    active_articles = articles[articles["active"].astype(str).str.lower().isin(["true","1","yes","ja"])].copy()
    active_articles = active_articles.sort_values("name")

    # build table
    base = active_articles[["sku","name"]].copy()
    base["date"] = entry_date.isoformat()

    # load existing rows for that date
    existing = daily_log.copy()
    if not existing.empty:
        existing = existing[existing["date"].astype(str) == entry_date.isoformat()].copy()
    else:
        existing = pd.DataFrame(columns=["date","sku","baked_total","waste_qty","oos_time","notes","created_at"])

    edit = base.merge(existing, on=["date","sku"], how="left")
    edit["baked_total"] = pd.to_numeric(edit.get("baked_total"), errors="coerce").fillna(0).astype(int)
    edit["waste_qty"] = pd.to_numeric(edit.get("waste_qty"), errors="coerce").fillna(0).astype(int)
    edit["oos_time"] = edit.get("oos_time", "").fillna("")
    edit["notes"] = edit.get("notes", "").fillna("")

    st.write("### Tageswerte")
    edited = st.data_editor(
        edit[["sku","name","baked_total","waste_qty","oos_time","notes"]],
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "baked_total": st.column_config.NumberColumn("Gebacken gesamt", min_value=0, step=1),
            "waste_qty": st.column_config.NumberColumn("Abschrift", min_value=0, step=1),
            "oos_time": st.column_config.TextColumn("Leer ab (HH:MM, optional)"),
            "notes": st.column_config.TextColumn("Notiz"),
        }
    )

    if st.button("💾 Speichern & Lernen", type="primary"):
        # write daily log
        to_save = edited.copy()
        to_save["date"] = entry_date.isoformat()
        to_save["sku"] = to_save["sku"].astype(str)
        to_save["created_at"] = pd.Timestamp.utcnow().isoformat()

        upsert_tab(sh, "daily_log", to_save[["date","sku","baked_total","waste_qty","oos_time","notes","created_at"]], key_cols=["date","sku"])

        # update model in memory then write
        model_local = model.copy()
        for _, r in to_save.iterrows():
            if clamp_int(r["baked_total"]) <= 0 and clamp_int(r["waste_qty"]) <= 0 and str(r.get("oos_time","")).strip()=="":
                # skip empty rows
                continue
            model_local = update_model_from_day(
                model_local,
                day_row={
                    "date": entry_date.isoformat(),
                    "sku": str(r["sku"]),
                    "baked_total": r["baked_total"],
                    "waste_qty": r["waste_qty"],
                    "oos_time": r.get("oos_time",""),
                },
                alpha=float(alpha),
                close_hour=int(close_hour),
            )

        # persist model
        write_tab(sh, "demand_model", model_local[["sku","weekday","demand_est","waste_rate_est","oos_rate_est","updated_at"]])
        st.success("Gespeichert. Die App hat gelernt ✅")
        st.rerun()

    st.stop()

# ----------------------------
# Page: Planung (heute)
# ----------------------------
if page == "Planung (heute)":
    st.subheader("Planung (heute)")

    if articles.empty or not (articles["active"].astype(str).str.lower().isin(["true","1","yes","ja"]).any()):
        st.info("Lege zuerst Artikel an (Seite: Artikel).")
        st.stop()

    plan_date = st.date_input("Datum", value=date.today())
    weekday = to_weekday_name(plan_date)
    st.caption(f"Ladenschluss: **{int(close_hour)}:00** | Lernrate alpha: **{float(alpha):.2f}** | Wochentag: **{weekday}**")

    active_articles = articles[articles["active"].astype(str).str.lower().isin(["true","1","yes","ja"])].copy()
    active_articles = active_articles.sort_values("name")

    # latest tables
    daily_log_latest = read_tab(sh, "daily_log")
    model_latest = read_tab(sh, "demand_model")
    model_latest["sku"] = model_latest["sku"].astype(str)
    model_latest["weekday"] = model_latest["weekday"].astype(str)
    model_latest["demand_est"] = pd.to_numeric(model_latest["demand_est"], errors="coerce").fillna(START_DEMAND_DEFAULT)
    model_latest["waste_rate_est"] = pd.to_numeric(model_latest["waste_rate_est"], errors="coerce").fillna(0.10)
    model_latest["oos_rate_est"] = pd.to_numeric(model_latest["oos_rate_est"], errors="coerce").fillna(0.10)

    # build recommendations
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

        freq = decide_bake_frequency(sku, daily_log_latest, close_hour=int(close_hour))
        if freq["mode"] == "2x":
            morning = int(np.round(qty * freq["morning_share"]))
            afternoon = max(0, qty - morning)
        else:
            morning = qty
            afternoon = 0

        recs.append({
            "sku": sku,
            "name": name,
            "empfehlung_total": qty,
            "empfehlung_morgens": morning,
            "empfehlung_nachmittag": afternoon,
            "backmodus": "2×" if freq["mode"] == "2x" else "1×",
            "grund": freq["reason"],
            "gelernt_bedarf": int(np.round(demand_est)),
            "abschrift_est": f"{wr*100:.0f}%",
            "oos_est": f"{orr*100:.0f}%"
        })

    df = pd.DataFrame(recs).sort_values(["backmodus","empfehlung_total"], ascending=[True, False])

    c1, c2 = st.columns([2, 1])
    with c1:
        q = st.text_input("Suche (Name/SKU)", value="")
    with c2:
        only_active = st.checkbox("Nur Empfehlung > 0", value=True)

    view = df.copy()
    if q.strip():
        qq = q.strip().lower()
        view = view[(view["sku"].str.lower().str.contains(qq)) | (view["name"].str.lower().str.contains(qq))]
    if only_active:
        view = view[view["empfehlung_total"] > 0]

    st.write("### Backempfehlung")
    st.dataframe(
        view[["sku","name","empfehlung_total","empfehlung_morgens","empfehlung_nachmittag","backmodus","grund","gelernt_bedarf","abschrift_est","oos_est"]],
        use_container_width=True
    )

    st.info(
        "Logik: Die App lernt **Tagesbedarf** aus (Gebacken − Abschrift). "
        "Wenn ihr oft **zu früh leer** seid und wenig Abschrift habt, empfiehlt sie **2× Backen**."
    )

    st.stop()
