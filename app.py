import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, timedelta
import gspread
from google.oauth2.service_account import Credentials

# =========================
# App Config
# =========================
st.set_page_config(page_title="Bake-Off Planer", layout="wide")

DEFAULT_CLOSE_HOUR = 20
DEFAULT_BATCH_SIZE = 6
ALPHA = 0.12  # Lernrate (0.05–0.2 sinnvoll)

BLOCK_A_HOURS = list(range(7, 14))  # 07–13
# BLOCK_B = 15–close_hour-1

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
    sheet_id = st.secrets.get("SHEET_ID", "").strip()
    if not sheet_id:
        st.error("Secrets fehlen: SHEET_ID ist nicht gesetzt.")
        st.stop()
    gc = get_gspread_client()
    return gc.open_by_key(sheet_id)

def ensure_tabs(spreadsheet):
    """Create required tabs + headers if missing."""
    required = {
        "daily_inputs": ["date","sku","baked_06","baked_14","rest_14","rest_close","waste_qty","note"],
        "sku_levels": ["sku","weekday","level_a","level_b","updated_at"],
        "config": ["key","value"],
    }
    existing = {w.title for w in spreadsheet.worksheets()}

    for tab, headers in required.items():
        if tab not in existing:
            spreadsheet.add_worksheet(title=tab, rows=2000, cols=max(12, len(headers) + 2))
        w = spreadsheet.worksheet(tab)
        row1 = w.row_values(1)
        if [x.strip() for x in row1[:len(headers)]] != headers:
            w.clear()
            w.update([headers])

def read_tab(spreadsheet, tab: str) -> pd.DataFrame:
    w = spreadsheet.worksheet(tab)
    records = w.get_all_records()
    if not records:
        headers = w.row_values(1)
        return pd.DataFrame(columns=headers if headers else [])
    return pd.DataFrame(records)

def write_tab(spreadsheet, tab: str, df: pd.DataFrame):
    w = spreadsheet.worksheet(tab)
    df2 = df.copy().replace({np.nan: ""})
    values = [df2.columns.tolist()] + df2.astype(object).values.tolist()
    w.clear()
    w.update(values)

def upsert_tab(spreadsheet, tab: str, df_new: pd.DataFrame, key_cols: list[str]):
    df_old = read_tab(spreadsheet, tab)
    if df_old.empty:
        df = df_new.copy()
    else:
        df = pd.concat([df_old, df_new], ignore_index=True)

    for c in key_cols:
        df[c] = df[c].astype(str)

    df = df.drop_duplicates(subset=key_cols, keep="last")
    write_tab(spreadsheet, tab, df)

# =========================
# Helpers / Forecast
# =========================
def parse_hour(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float)) and not np.isnan(x):
        h = int(x)
        return h if 0 <= h <= 23 else np.nan
    s = str(x).strip()
    if "-" in s:
        s = s.split("-")[0].strip()
    if ":" in s:
        try:
            h = int(s.split(":")[0])
            return h if 0 <= h <= 23 else np.nan
        except:
            return np.nan
    try:
        h = int(s)
        return h if 0 <= h <= 23 else np.nan
    except:
        return np.nan

def round_to_batch(x: float, batch: int) -> int:
    x = max(0.0, float(x))
    if batch <= 1:
        return int(np.ceil(x))
    return int(np.ceil(x / batch) * batch)

def normalize_freq(df: pd.DataFrame, sku_col, date_col, hour_col, qty_col, name_col=None):
    cols = [sku_col, date_col, hour_col, qty_col] + ([name_col] if name_col else [])
    out = df[cols].copy()

    rename = {sku_col:"sku", date_col:"date_raw", hour_col:"hour_raw", qty_col:"sales_qty"}
    if name_col:
        rename[name_col] = "name"
    out = out.rename(columns=rename)

    out["dt"] = pd.to_datetime(out["date_raw"], errors="coerce", dayfirst=True)
    out = out.dropna(subset=["dt"])
    out["date"] = out["dt"].dt.date.astype(str)
    out["weekday"] = out["dt"].dt.day_name()

    out["hour"] = out["hour_raw"].apply(parse_hour)
    out = out.dropna(subset=["hour"])
    out["hour"] = out["hour"].astype(int)

    out["sales_qty"] = pd.to_numeric(out["sales_qty"], errors="coerce").fillna(0.0)

    if "name" not in out.columns:
        out["name"] = out["sku"].astype(str)

    out["sku"] = out["sku"].astype(str)
    out["name"] = out["name"].astype(str)

    return out[["sku","name","date","weekday","hour","sales_qty"]]

def build_baseline(freq_norm: pd.DataFrame, close_hour: int):
    block_b_hours = [h for h in range(15, 24) if h < close_hour]

    df = freq_norm.copy()
    df["is_A"] = df["hour"].isin(BLOCK_A_HOURS)
    df["is_B"] = df["hour"].isin(block_b_hours)

    daily = (df.groupby(["sku","name","date","weekday"], as_index=False)
               .agg(
                   sales_A=("sales_qty", lambda s: float(s[df.loc[s.index,"is_A"]].sum())),
                   sales_B=("sales_qty", lambda s: float(s[df.loc[s.index,"is_B"]].sum())),
               ))

    baseline = (daily.groupby(["sku","name","weekday"], as_index=False)
                  .agg(
                      base_A=("sales_A","median"),
                      base_B=("sales_B","median"),
                      std_A=("sales_A", lambda x: float(np.std(x, ddof=0))),
                      std_B=("sales_B", lambda x: float(np.std(x, ddof=0))),
                      n_days=("sales_A","count"),
                  ))
    return daily, baseline

def get_cfg(config_df: pd.DataFrame, key: str, default):
    if config_df.empty or "key" not in config_df.columns or "value" not in config_df.columns:
        return default
    hit = config_df[config_df["key"].astype(str) == str(key)]
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

def get_daily_inputs(spreadsheet, d: date) -> pd.DataFrame:
    df = read_tab(spreadsheet, "daily_inputs")
    if df.empty:
        return pd.DataFrame(columns=["date","sku","baked_06","baked_14","rest_14","rest_close","waste_qty","note"])
    df["date"] = df["date"].astype(str)
    df["sku"] = df["sku"].astype(str)
    return df[df["date"] == d.isoformat()].copy()

def get_prev_close_stock(spreadsheet, d: date) -> pd.DataFrame:
    prev = d - timedelta(days=1)
    df = read_tab(spreadsheet, "daily_inputs")
    if df.empty:
        return pd.DataFrame(columns=["sku","stock_06"])
    df["date"] = df["date"].astype(str)
    df["sku"] = df["sku"].astype(str)
    prev_df = df[df["date"] == prev.isoformat()].copy()
    if prev_df.empty:
        return pd.DataFrame(columns=["sku","stock_06"])
    prev_df["stock_06"] = pd.to_numeric(prev_df.get("rest_close"), errors="coerce").fillna(0.0)
    return prev_df[["sku","stock_06"]].copy()

def compute_plan(levels: pd.DataFrame, stock_06: pd.DataFrame, weekday: str,
                 batch_size: int, bufferA: float, bufferB: float, close_hour: int):
    df = levels[levels["weekday"] == weekday].copy()
    df = df.merge(stock_06, on="sku", how="left")
    df["stock_06"] = pd.to_numeric(df["stock_06"], errors="coerce").fillna(0.0)

    # Block A (07–14)
    df["target_A"] = df["level_a"] * (1.0 + bufferA)
    df["bake_06_raw"] = (df["target_A"] - df["stock_06"]).clip(lower=0.0)
    df["bake_06"] = df["bake_06_raw"].apply(lambda x: round_to_batch(x, batch_size))

    # stock_14 estimate
    df["stock_14_est"] = (df["stock_06"] + df["bake_06"] - df["level_a"]).clip(lower=0.0)

    # Block B (15–close)
    df["target_B"] = df["level_b"] * (1.0 + bufferB)
    df["bake_14_raw"] = (df["target_B"] - df["stock_14_est"]).clip(lower=0.0)
    df["bake_14"] = df["bake_14_raw"].apply(lambda x: round_to_batch(x, batch_size))

    # Minimaler Schutz: um 15 Uhr nicht direkt leer -> mindestens 1 Batch verfügbar
    df["bake_14"] = np.maximum(df["bake_14"], (batch_size - df["stock_14_est"]).clip(lower=0.0))
    df["bake_14"] = df["bake_14"].apply(lambda x: round_to_batch(x, batch_size))

    return df

def plausibility_checks(freq_daily: pd.DataFrame, baseline: pd.DataFrame, inputs: pd.DataFrame, close_hour: int):
    if inputs.empty:
        return pd.DataFrame(columns=["sku","issue","detail"])

    d0 = inputs["date"].iloc[0]
    weekday = pd.to_datetime(d0).day_name()

    pos_day = freq_daily[freq_daily["date"] == d0][["sku","sales_A","sales_B"]].copy()
    base_wd = baseline[baseline["weekday"] == weekday][["sku","base_A","base_B"]].copy()

    merged = inputs.merge(pos_day, on="sku", how="left").merge(base_wd, on="sku", how="left")
    issues = []

    for _, r in merged.iterrows():
        sku = str(r["sku"])
        baked_06 = int(r.get("baked_06") or 0)
        baked_14 = int(r.get("baked_14") or 0)
        rest_close = r.get("rest_close")
        waste = int(r.get("waste_qty") or 0)

        sales_B = r.get("sales_B")
        base_B = r.get("base_B")

        # Abend ohne Umsatz, aber Rest/Abschrift
        if pd.notna(sales_B) and float(sales_B) == 0.0:
            if (waste >= 1) or (pd.notna(rest_close) and int(rest_close) >= 1):
                issues.append((sku, "Abend ohne Umsatz",
                               f"Sales 15–{close_hour}: 0, aber Rest/Abschrift vorhanden (waste={waste}, rest_close={rest_close})."))

        # OOS-Verdacht
        if pd.notna(sales_B) and pd.notna(base_B):
            if float(sales_B) < 0.4 * float(base_B) and (pd.isna(rest_close) or int(rest_close) == 0) and waste == 0:
                issues.append((sku, "OOS-Verdacht (Abend)",
                               f"Sales Abend deutlich unter üblich (heute {sales_B:.0f} vs. üblich ~{base_B:.0f}), Rest/Abschrift 0."))

        # Bilanz-Check (wenn POS da)
        sales_A = r.get("sales_A")
        if pd.notna(rest_close) and pd.notna(sales_A) and pd.notna(sales_B):
            balance = (baked_06 + baked_14) - (float(sales_A) + float(sales_B)) - waste - int(rest_close)
            if balance < -2:
                issues.append((sku, "Unplausible Bilanz",
                               f"(baked06+baked14) - sales - waste - rest_close = {balance:.1f}. Prüfe Eingabe/Zuordnung."))

    return pd.DataFrame(issues, columns=["sku","issue","detail"])

# =========================
# UI
# =========================
st.title("🥐 Bake-Off Planer (Google Sheets)")

spreadsheet = open_spreadsheet()
ensure_tabs(spreadsheet)

# Sidebar Navigation
st.sidebar.header("Navigation")
page = st.sidebar.radio("Seite", ["Planung (heute)", "Tagesabschluss (Eingabe)", "Debug"], index=0)

# Load config
config_df = read_tab(spreadsheet, "config")
cfg_close = get_cfg(config_df, "close_hour", DEFAULT_CLOSE_HOUR)
cfg_batch = get_cfg(config_df, "batch_size", DEFAULT_BATCH_SIZE)

# Sidebar parameters
st.sidebar.header("Parameter")
close_hour = st.sidebar.slider("Ladenschluss (Stunde)", 17, 23, int(cfg_close), 1)
batch_size = st.sidebar.number_input("Batch/Blechgröße (Rundung)", 1, 48, int(cfg_batch), 1)
bufferA = st.sidebar.slider("Puffer Block A (07–14)", 0.00, 0.50, 0.10, 0.01)
bufferB = st.sidebar.slider("Puffer Block B (15–Close)", 0.00, 0.70, 0.15, 0.01)

if st.sidebar.button("💾 Parameter speichern"):
    cfg_new = pd.DataFrame([{"key":"close_hour","value":int(close_hour)},
                            {"key":"batch_size","value":int(batch_size)}])
    upsert_tab(spreadsheet, "config", cfg_new, key_cols=["key"])
    st.sidebar.success("Gespeichert.")

# Upload Frequency file (needed for baseline)
st.sidebar.header("Historische Daten (Upload)")
freq_file = st.sidebar.file_uploader("FREQUENZ / Verkäufe (Excel)", type=["xlsx"])

if page != "Debug":
    if not freq_file:
        st.info("⬅️ Lade links die FREQUENZ/Verkäufe Excel hoch, damit die Baseline-Prognose funktioniert.")
        st.stop()

    freq_raw = pd.read_excel(freq_file)
    cols = list(freq_raw.columns)

    st.sidebar.header("Spalten-Mapping (FREQUENZ)")
    sku_col = st.sidebar.selectbox("SKU/Artikel-ID", cols, index=0)
    name_col = st.sidebar.selectbox("Artikelname (optional)", ["(keine)"] + cols, index=0)
    date_col = st.sidebar.selectbox("Datum/Datetime", cols, index=min(1, len(cols)-1))
    hour_col = st.sidebar.selectbox("Stunde/Zeitfenster", cols, index=min(2, len(cols)-1))
    qty_col = st.sidebar.selectbox("Menge (Stück)", cols, index=min(3, len(cols)-1))
    name_col_real = None if name_col == "(keine)" else name_col

    freq_norm = normalize_freq(freq_raw, sku_col, date_col, hour_col, qty_col, name_col=name_col_real)
    freq_daily, baseline = build_baseline(freq_norm, close_hour=int(close_hour))

    # Build / Load levels from sheet
    levels_sheet = read_tab(spreadsheet, "sku_levels")
    levels = baseline[["sku","name","weekday","base_A","base_B"]].copy()

    if not levels_sheet.empty:
        lv = levels_sheet.copy()
        lv["sku"] = lv["sku"].astype(str)
        lv["weekday"] = lv["weekday"].astype(str)
        lv["level_a"] = pd.to_numeric(lv["level_a"], errors="coerce")
        lv["level_b"] = pd.to_numeric(lv["level_b"], errors="coerce")

        levels = levels.merge(lv[["sku","weekday","level_a","level_b"]], on=["sku","weekday"], how="left")
        levels["level_a"] = levels["level_a"].fillna(levels["base_A"])
        levels["level_b"] = levels["level_b"].fillna(levels["base_B"])
    else:
        levels["level_a"] = levels["base_A"]
        levels["level_b"] = levels["base_B"]

    sku_master = baseline[["sku","name"]].drop_duplicates().sort_values("name")

# -------------------------
# Page: Debug
# -------------------------
if page == "Debug":
    st.subheader("Debug")
    try:
        gc = get_gspread_client()
        files = gc.list_spreadsheet_files()
        st.write("Sichtbare Spreadsheets:", len(files))
        st.dataframe(pd.DataFrame(files), use_container_width=True)
        st.success("Wenn dein Ziel-Sheet hier drin ist, setze SHEET_ID auf die passende `id` aus der Tabelle.")
    except Exception as e:
        st.error("Debug fehlgeschlagen")
        st.exception(e)
    st.stop()

# -------------------------
# Page: Planning
# -------------------------
if page == "Planung (heute)":
    st.subheader("Planung (heute)")
    plan_date = st.date_input("Datum", value=date.today())
    weekday = pd.to_datetime(plan_date.isoformat()).day_name()

    st.caption(f"Wochentag: **{weekday}** | Backfenster: **06–07** und **14–15** | Bedarf: **Block A 07–14**, **Block B 15–{int(close_hour)}:00**")

    stock_06 = get_prev_close_stock(spreadsheet, plan_date)
    plan = compute_plan(
        levels=levels,
        stock_06=stock_06,
        weekday=weekday,
        batch_size=int(batch_size),
        bufferA=float(bufferA),
        bufferB=float(bufferB),
        close_hour=int(close_hour),
    )

    c1, c2 = st.columns([2, 1])
    with c1:
        q = st.text_input("Suche (SKU oder Name)", value="")
    with c2:
        only_positive = st.checkbox("Nur Vorschläge > 0", value=True)

    view = plan.copy()
    if q.strip():
        qq = q.strip().lower()
        view = view[(view["sku"].astype(str).str.lower().str.contains(qq)) |
                    (view["name"].astype(str).str.lower().str.contains(qq))]
    if only_positive:
        view = view[(view["bake_06"] > 0) | (view["bake_14"] > 0)]

    view = view.sort_values(["bake_06","bake_14"], ascending=False)

    st.write("### Backvorschläge")
    st.dataframe(
        view[["sku","name","level_a","level_b","stock_06","bake_06","stock_14_est","bake_14"]],
        use_container_width=True
    )

    st.info("`stock_06` kommt aus **Rest Ladenschluss von gestern** (wenn eingetragen). `stock_14_est` ist geschätzt.")

# -------------------------
# Page: Daily Input
# -------------------------
else:
    st.subheader("Tagesabschluss (Eingabe)")
    entry_date = st.date_input("Datum (für Eingabe)", value=date.today())
    weekday = pd.to_datetime(entry_date.isoformat()).day_name()

    st.caption("Trage die **echten Zahlen** ein. Das schreibt ins Google Sheet und aktualisiert die Prognose (Levels) pro Wochentag.")

    existing = get_daily_inputs(spreadsheet, entry_date)

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        q = st.text_input("Suche Artikel (Name/SKU)", value="")
    with c2:
        max_rows = st.number_input("Max. Zeilen", min_value=20, max_value=500, value=80, step=10)
    with c3:
        show_only_changed = st.checkbox("Nur bereits erfasste zeigen", value=False)

    master = sku_master.copy()
    if q.strip():
        qq = q.strip().lower()
        master = master[(master["sku"].astype(str).str.lower().str.contains(qq)) |
                        (master["name"].astype(str).str.lower().str.contains(qq))]

    if show_only_changed and not existing.empty:
        master = master[master["sku"].astype(str).isin(existing["sku"].astype(str))]

    master = master.head(int(max_rows))

    edit = master.merge(existing, on="sku", how="left")
    edit["date"] = entry_date.isoformat()

    for col in ["baked_06","baked_14","waste_qty"]:
        edit[col] = pd.to_numeric(edit.get(col), errors="coerce").fillna(0).astype(int)

    for col in ["rest_14","rest_close"]:
        edit[col] = pd.to_numeric(edit.get(col), errors="coerce")

    edit["note"] = edit.get("note", "").fillna("")

    edited = st.data_editor(
        edit[["sku","name","baked_06","baked_14","rest_14","rest_close","waste_qty","note"]],
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "baked_06": st.column_config.NumberColumn("Gebacken 06:00", min_value=0, step=1),
            "baked_14": st.column_config.NumberColumn("Gebacken 14:00", min_value=0, step=1),
            "rest_14": st.column_config.NumberColumn("Rest 14:00 (optional)", min_value=0, step=1),
            "rest_close": st.column_config.NumberColumn("Rest Ladenschluss", min_value=0, step=1),
            "waste_qty": st.column_config.NumberColumn("Abschrift (Stück)", min_value=0, step=1),
        }
    )

    if st.button("💾 Speichern", type="primary"):
        to_save = edited.copy()
        to_save["date"] = entry_date.isoformat()
        to_save["sku"] = to_save["sku"].astype(str)

        df_new = pd.DataFrame({
            "date": to_save["date"].astype(str),
            "sku": to_save["sku"].astype(str),
            "baked_06": to_save["baked_06"].fillna(0).astype(int),
            "baked_14": to_save["baked_14"].fillna(0).astype(int),
            "rest_14": to_save["rest_14"].replace({np.nan: ""}),
            "rest_close": to_save["rest_close"].replace({np.nan: ""}),
            "waste_qty": to_save["waste_qty"].fillna(0).astype(int),
            "note": to_save["note"].fillna("").astype(str),
        })

        upsert_tab(spreadsheet, "daily_inputs", df_new, key_cols=["date","sku"])

        # ---------- Learning update ----------
        base_wd = baseline[baseline["weekday"] == weekday][["sku","base_A","base_B"]].copy()
        pos_day = freq_daily[freq_daily["date"] == entry_date.isoformat()][["sku","sales_A","sales_B"]].copy()

        merged = base_wd.merge(pos_day, on="sku", how="left")
        merged["sales_A"] = merged["sales_A"].fillna(merged["base_A"])
        merged["sales_B"] = merged["sales_B"].fillna(merged["base_B"])

        rest = df_new[["sku","rest_close","waste_qty"]].copy()
        rest["rest_close_num"] = pd.to_numeric(rest["rest_close"], errors="coerce").fillna(0.0)
        merged = merged.merge(rest[["sku","rest_close_num"]], on="sku", how="left")
        merged["rest_close_num"] = merged["rest_close_num"].fillna(0.0)

        current_lv = read_tab(spreadsheet, "sku_levels")
        if current_lv.empty:
            current_lv = pd.DataFrame(columns=["sku","weekday","level_a","level_b","updated_at"])
        current_lv["sku"] = current_lv.get("sku", "").astype(str)
        current_lv["weekday"] = current_lv.get("weekday", "").astype(str)

        merged = merged.merge(
            current_lv[current_lv["weekday"] == weekday][["sku","level_a","level_b"]],
            on="sku",
            how="left"
        )
        merged["level_a"] = pd.to_numeric(merged["level_a"], errors="coerce").fillna(merged["base_A"])
        merged["level_b"] = pd.to_numeric(merged["level_b"], errors="coerce").fillna(merged["base_B"])

        merged["obs_A"] = merged["sales_A"]
        merged["obs_B"] = merged["sales_B"]

        # OOS-Verdacht: Rest=0 und Sales_B deutlich unter baseline -> baseline als "wahre" Nachfrage nehmen
        merged["oos_suspect_B"] = (merged["rest_close_num"] <= 0) & (merged["sales_B"] < 0.5 * merged["base_B"])
        merged.loc[merged["oos_suspect_B"], "obs_B"] = merged.loc[merged["oos_suspect_B"], "base_B"]

        merged["new_level_a"] = (1 - ALPHA) * merged["level_a"] + ALPHA * merged["obs_A"]
        merged["new_level_b"] = (1 - ALPHA) * merged["level_b"] + ALPHA * merged["obs_B"]

        lv_new = pd.DataFrame({
            "sku": merged["sku"].astype(str),
            "weekday": weekday,
            "level_a": merged["new_level_a"].astype(float),
            "level_b": merged["new_level_b"].astype(float),
            "updated_at": pd.Timestamp.utcnow().isoformat()
        })

        upsert_tab(spreadsheet, "sku_levels", lv_new, key_cols=["sku","weekday"])

        st.success("Gespeichert. Prognose-Levels wurden aktualisiert.")

    st.divider()
    st.write("### Plausibilitätschecks (für dieses Datum)")
    inputs = get_daily_inputs(spreadsheet, entry_date)
    if inputs.empty:
        st.info("Noch keine Eingaben gespeichert.")
    else:
        checks = plausibility_checks(freq_daily, baseline, inputs, close_hour=int(close_hour))
        if checks.empty:
            st.success("Keine Auffälligkeiten gefunden (oder keine POS-Daten für diesen Tag verfügbar).")
        else:
            st.dataframe(checks, use_container_width=True)
            st.caption("Hinweise sind heuristisch (MVP) und sollen Überproduktion, OOS-Verdacht oder Daten-/Zuordnungsfehler sichtbar machen.")
