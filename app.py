import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import date, timedelta

# ----------------------------
# Config
# ----------------------------
st.set_page_config(page_title="Bake-Off Planer", layout="wide")

DEFAULT_CLOSE_HOUR = 20  # kannst du in der UI ändern
BLOCK_A_HOURS = list(range(7, 14))   # 07-13 (nach Backfenster 06-07)
BLOCK_B_HOURS = list(range(15, 24))  # 15-23, wird später bis close gekürzt
ALPHA = 0.12  # Lernrate fürs Glätten (0.05-0.2 sinnvoll)

# ----------------------------
# Helpers
# ----------------------------
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

def get_db():
    # SQLite Datei im Arbeitsverzeichnis
    conn = sqlite3.connect("bakeoff.db", check_same_thread=False)
    return conn

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()

    # tägliche Eingaben
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_inputs (
        d TEXT NOT NULL,
        sku TEXT NOT NULL,
        baked_06 INTEGER DEFAULT 0,
        baked_14 INTEGER DEFAULT 0,
        rest_14 INTEGER,
        rest_close INTEGER,
        waste_qty INTEGER DEFAULT 0,
        note TEXT,
        PRIMARY KEY (d, sku)
    );
    """)

    # laufende "Levels" pro Wochentag (Demand-Estimate Block A/B)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sku_levels (
        sku TEXT NOT NULL,
        weekday TEXT NOT NULL,
        level_a REAL NOT NULL,
        level_b REAL NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (sku, weekday)
    );
    """)

    conn.commit()

@st.cache_data
def read_excel(file) -> pd.DataFrame:
    return pd.read_excel(file)

def normalize_freq(df: pd.DataFrame, sku_col, date_col, hour_col, qty_col, name_col=None):
    out = df.copy()
    cols = [sku_col, date_col, hour_col, qty_col] + ([name_col] if name_col else [])
    out = out[cols].copy()

    rename = {sku_col: "sku", date_col: "date_raw", hour_col: "hour_raw", qty_col: "sales_qty"}
    if name_col:
        rename[name_col] = "name"
    out = out.rename(columns=rename)

    out["dt"] = pd.to_datetime(out["date_raw"], errors="coerce", dayfirst=True)
    out = out.dropna(subset=["dt"])
    out["d"] = out["dt"].dt.date
    out["weekday"] = out["dt"].dt.day_name()
    out["hour"] = out["hour_raw"].apply(parse_hour).astype("Int64")
    out["sales_qty"] = pd.to_numeric(out["sales_qty"], errors="coerce").fillna(0.0)

    # falls Name fehlt: später mappen
    if "name" not in out.columns:
        out["name"] = out["sku"].astype(str)

    return out[["sku", "name", "d", "weekday", "hour", "sales_qty"]]

def build_baseline_profiles(freq_norm: pd.DataFrame, close_hour: int):
    # Daily block sums per SKU
    block_b_hours = [h for h in range(15, 24) if h < close_hour]

    df = freq_norm.copy()
    df["is_A"] = df["hour"].isin(BLOCK_A_HOURS)
    df["is_B"] = df["hour"].isin(block_b_hours)

    daily = (df.groupby(["sku", "name", "d", "weekday"], as_index=False)
               .agg(sales_A=("sales_qty", lambda s: float(s[df.loc[s.index, "is_A"]].sum())),
                    sales_B=("sales_qty", lambda s: float(s[df.loc[s.index, "is_B"]].sum()))))

    # robust baseline: Median pro (SKU, weekday)
    baseline = (daily.groupby(["sku", "name", "weekday"], as_index=False)
                    .agg(base_A=("sales_A", "median"),
                         base_B=("sales_B", "median"),
                         mean_A=("sales_A", "mean"),
                         mean_B=("sales_B", "mean"),
                         std_A=("sales_A", lambda x: float(np.std(x, ddof=0))),
                         std_B=("sales_B", lambda x: float(np.std(x, ddof=0))),
                         n_days=("sales_A", "count")))
    return daily, baseline

def load_levels(conn, baseline: pd.DataFrame):
    # initial levels = baseline, wenn noch nichts in DB
    cur = conn.cursor()
    rows = cur.execute("SELECT sku, weekday, level_a, level_b FROM sku_levels").fetchall()
    if rows:
        lvl = pd.DataFrame(rows, columns=["sku", "weekday", "level_a", "level_b"])
        out = baseline.merge(lvl, on=["sku", "weekday"], how="left")
        out["level_a"] = out["level_a"].fillna(out["base_A"])
        out["level_b"] = out["level_b"].fillna(out["base_B"])
        return out
    else:
        out = baseline.copy()
        out["level_a"] = out["base_A"]
        out["level_b"] = out["base_B"]
        return out

def upsert_levels(conn, sku: str, weekday: str, level_a: float, level_b: float):
    cur = conn.cursor()
    cur.execute("""
    INSERT INTO sku_levels (sku, weekday, level_a, level_b, updated_at)
    VALUES (?, ?, ?, ?, datetime('now'))
    ON CONFLICT(sku, weekday) DO UPDATE SET
        level_a=excluded.level_a,
        level_b=excluded.level_b,
        updated_at=datetime('now');
    """, (sku, weekday, float(level_a), float(level_b)))
    conn.commit()

def fetch_inputs(conn, d: date):
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT d, sku, baked_06, baked_14, rest_14, rest_close, waste_qty, note
        FROM daily_inputs WHERE d = ?
    """, (d.isoformat(),)).fetchall()
    if not rows:
        return pd.DataFrame(columns=["d","sku","baked_06","baked_14","rest_14","rest_close","waste_qty","note"])
    return pd.DataFrame(rows, columns=["d","sku","baked_06","baked_14","rest_14","rest_close","waste_qty","note"])

def fetch_inputs_day(conn, d: date):
    return fetch_inputs(conn, d)

def get_prev_close_stock(conn, d: date):
    prev = d - timedelta(days=1)
    inp = fetch_inputs(conn, prev)
    if inp.empty:
        return pd.DataFrame(columns=["sku", "stock_06"])
    # stock_06 für heute = rest_close von gestern
    out = inp[["sku","rest_close"]].copy()
    out = out.rename(columns={"rest_close":"stock_06"})
    return out

def save_inputs(conn, d: date, df_inputs: pd.DataFrame):
    cur = conn.cursor()
    for _, r in df_inputs.iterrows():
        cur.execute("""
        INSERT INTO daily_inputs (d, sku, baked_06, baked_14, rest_14, rest_close, waste_qty, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(d, sku) DO UPDATE SET
            baked_06=excluded.baked_06,
            baked_14=excluded.baked_14,
            rest_14=excluded.rest_14,
            rest_close=excluded.rest_close,
            waste_qty=excluded.waste_qty,
            note=excluded.note;
        """, (
            d.isoformat(),
            str(r["sku"]),
            int(r.get("baked_06", 0) or 0),
            int(r.get("baked_14", 0) or 0),
            None if pd.isna(r.get("rest_14")) else int(r.get("rest_14")),
            None if pd.isna(r.get("rest_close")) else int(r.get("rest_close")),
            int(r.get("waste_qty", 0) or 0),
            str(r.get("note", "") or "")
        ))
    conn.commit()

def compute_plan(levels: pd.DataFrame, stock_06: pd.DataFrame, weekday: str, batch_size: int,
                 bufferA: float, bufferB: float):
    df = levels[levels["weekday"] == weekday].copy()

    df = df.merge(stock_06, on="sku", how="left")
    df["stock_06"] = pd.to_numeric(df["stock_06"], errors="coerce").fillna(0.0)

    # Vorschlag: level * (1+buffer) - vorhandener Bestand, dann auf Batch runden
    df["target_A"] = df["level_a"] * (1.0 + bufferA)
    df["bake_06_raw"] = (df["target_A"] - df["stock_06"]).clip(lower=0.0)
    df["bake_06"] = df["bake_06_raw"].apply(lambda x: round_to_batch(x, batch_size))

    # stock_14 (geschätzt): stock_06 + bake_06 - level_a (erwarteter Verbrauch Block A)
    df["stock_14_est"] = (df["stock_06"] + df["bake_06"] - df["level_a"]).clip(lower=0.0)

    df["target_B"] = df["level_b"] * (1.0 + bufferB)
    df["bake_14_raw"] = (df["target_B"] - df["stock_14_est"]).clip(lower=0.0)
    df["bake_14"] = df["bake_14_raw"].apply(lambda x: round_to_batch(x, batch_size))

    # Minimaler “nicht leer um 15 Uhr”-Schutz: mind. 1 Batch vorhanden/produziert
    df["min_15"] = batch_size
    df["bake_14"] = np.maximum(df["bake_14"], df["min_15"] - df["stock_14_est"])
    df["bake_14"] = df["bake_14"].apply(lambda x: round_to_batch(x, batch_size))

    return df

def plausibility_checks(freq_daily: pd.DataFrame, baseline: pd.DataFrame, inputs: pd.DataFrame, close_hour: int):
    """
    Checks für Tage, die im POS-Datensatz existieren (freq_daily enthält sales_A/B je Tag).
    """
    if inputs.empty:
        return pd.DataFrame(columns=["sku","issue","detail"])

    # POS daily sales for that day (if available)
    merged = inputs.merge(freq_daily[["sku","d","sales_A","sales_B"]], left_on=["sku","d"], right_on=["sku","d"], how="left")

    # baseline expectation for same weekday (needs weekday)
    d0 = pd.to_datetime(inputs["d"].iloc[0]).date()
    weekday = pd.to_datetime(str(d0)).day_name()
    base_wd = baseline[baseline["weekday"] == weekday][["sku","base_A","base_B"]]
    merged = merged.merge(base_wd, on="sku", how="left")

    issues = []

    for _, r in merged.iterrows():
        sku = r["sku"]
        baked_06 = int(r.get("baked_06") or 0)
        baked_14 = int(r.get("baked_14") or 0)
        rest_14 = r.get("rest_14")
        rest_close = r.get("rest_close")
        waste = int(r.get("waste_qty") or 0)

        sales_A = r.get("sales_A")
        sales_B = r.get("sales_B")
        base_A = r.get("base_A")
        base_B = r.get("base_B")

        # Check 1: “keine Umsätze Abend” aber Ware + Abschrift deuten auf Überproduktion
        if pd.notna(sales_B) and float(sales_B) == 0.0:
            # wenn trotzdem nennenswert waste oder Rest -> Hinweis
            if (waste >= 1) or (pd.notna(rest_close) and int(rest_close) >= 1):
                issues.append((sku, "Abend ohne Umsatz", f"Sales 15–{close_hour}: 0, aber Abschrift/Rest vorhanden (waste={waste}, rest_close={rest_close}). Wahrscheinlich zu viel gebacken / falsches Timing / Platzierung."))

        # Check 2: “Umsatz auffällig niedrig + Rest=0 + waste=0” -> OOS-Verdacht
        if pd.notna(sales_B) and pd.notna(base_B):
            if float(sales_B) < 0.4 * float(base_B) and (pd.isna(rest_close) or int(rest_close) == 0) and waste == 0:
                issues.append((sku, "OOS-Verdacht (Abend)", f"Sales Abend deutlich unter üblich (heute {sales_B:.0f} vs. üblich ~{base_B:.0f}), Rest/Abschrift 0. Ware evtl. ausverkauft oder nie nachgelegt."))

        # Check 3: Massenbilanz (nur wenn Rest_close eingetragen & POS-Sales vorhanden)
        if pd.notna(rest_close) and pd.notna(sales_A) and pd.notna(sales_B):
            # stock_06 kennt die App ohne Vortag hier nicht -> wir prüfen nur 'verfügbar' plausibel:
            # baked_06+baked_14 - sales - waste - rest_close sollte ~>=0 sein
            balance = (baked_06 + baked_14) - (float(sales_A) + float(sales_B)) - waste - int(rest_close)
            if balance < -2:  # Toleranz
                issues.append((sku, "Unplausible Bilanz", f"Eingaben passen nicht zu POS: (baked06+baked14) - sales - waste - rest_close = {balance:.1f}. Prüfe Eingabe/Artikelzuordnung."))

    return pd.DataFrame(issues, columns=["sku","issue","detail"])

# ----------------------------
# UI
# ----------------------------
st.title("🥐 Bake-Off Planer: 06:00 & 14:00 Backvorschläge + Tagesabschluss")

conn = get_db()
init_db(conn)

st.sidebar.header("Navigation")
page = st.sidebar.radio("Seite", ["Planung (heute)", "Tagesabschluss (Eingabe)"], index=0)

st.sidebar.header("Historische Daten (Upload)")
st.sidebar.caption("Für Baseline-Prognose & Checks. Einmal hochladen pro Session.")

freq_file = st.sidebar.file_uploader("FREQUENZ / Verkäufe (Excel)", type=["xlsx"])

if not freq_file:
    st.info("⬅️ Lade links die FREQUENZ/Verkäufe Excel hoch, damit die Baseline-Prognose funktioniert.")
    st.stop()

freq_raw = read_excel(freq_file)

st.sidebar.header("Spalten-Mapping (FREQUENZ)")
cols = list(freq_raw.columns)

sku_col = st.sidebar.selectbox("SKU/Artikel-ID", cols, index=0)
name_guess = 1 if len(cols) > 1 else 0
name_col = st.sidebar.selectbox("Artikelname (optional)", ["(keine)"] + cols, index=0 if "(keine)" else 0)
date_col = st.sidebar.selectbox("Datum/Datetime", cols, index=min(1, len(cols)-1))
hour_col = st.sidebar.selectbox("Stunde/Zeitfenster", cols, index=min(2, len(cols)-1))
qty_col = st.sidebar.selectbox("Menge (Stück)", cols, index=min(3, len(cols)-1))

name_col_real = None if name_col == "(keine)" else name_col

st.sidebar.header("Parameter")
close_hour = st.sidebar.slider("Ladenschluss (Stunde)", 17, 23, DEFAULT_CLOSE_HOUR, 1)
batch_size = st.sidebar.number_input("Batch/Blechgröße (Rundung)", min_value=1, max_value=48, value=6, step=1)
bufferA = st.sidebar.slider("Puffer Block A (07–14)", 0.00, 0.50, 0.10, 0.01)
bufferB = st.sidebar.slider("Puffer Block B (15–Close)", 0.00, 0.70, 0.15, 0.01)

freq_norm = normalize_freq(freq_raw, sku_col, date_col, hour_col, qty_col, name_col=name_col_real)
freq_daily, baseline = build_baseline_profiles(freq_norm, close_hour=close_hour)
levels = load_levels(conn, baseline)

# SKU master for dropdowns
sku_master = (baseline[["sku","name"]]
              .drop_duplicates()
              .sort_values("name"))

# ----------------------------
# Page: Planning
# ----------------------------
if page == "Planung (heute)":
    st.subheader("Planung (heute)")

    plan_date = st.date_input("Datum", value=date.today())
    weekday = pd.to_datetime(str(plan_date)).day_name()
    st.caption(f"Wochentag: **{weekday}** | Backfenster: **06–07** und **14–15** | Bedarf: **Block A 07–14**, **Block B 15–{close_hour}:00**")

    # stock_06 from yesterday rest_close
    stock_06 = get_prev_close_stock(conn, plan_date)

    plan = compute_plan(levels, stock_06, weekday=weekday, batch_size=batch_size, bufferA=bufferA, bufferB=bufferB)
    plan = plan.sort_values(["bake_06","bake_14"], ascending=False)

    # filter/search
    c1, c2 = st.columns([2, 1])
    with c1:
        q = st.text_input("Suche (SKU oder Name)", value="")
    with c2:
        only_positive = st.checkbox("Nur Vorschläge > 0", value=True)

    view = plan.copy()
    if q.strip():
        qq = q.strip().lower()
        view = view[(view["sku"].astype(str).str.lower().str.contains(qq)) | (view["name"].astype(str).str.lower().str.contains(qq))]
    if only_positive:
        view = view[(view["bake_06"] > 0) | (view["bake_14"] > 0)]

    st.write("### Backvorschläge")
    st.dataframe(
        view[["sku","name","level_a","level_b","stock_06","bake_06","stock_14_est","bake_14"]],
        use_container_width=True
    )

    st.info("Hinweis: `stock_06` kommt aus **Restbestand von gestern (Ladenschluss)**, wenn eingetragen. Sonst 0. `stock_14_est` ist geschätzt.")

# ----------------------------
# Page: Daily input
# ----------------------------
else:
    st.subheader("Tagesabschluss (Eingabe)")

    entry_date = st.date_input("Datum (für Eingabe)", value=date.today())
    weekday = pd.to_datetime(str(entry_date)).day_name()

    st.caption("Trage hier die **echten Zahlen** ein. Die App nutzt das als Feedback für die Prognose und für Plausibilitätschecks.")

    # Choose subset of SKUs for editing (avoid giant tables)
    c1, c2, c3 = st.columns([2,1,1])
    with c1:
        q = st.text_input("Suche Artikel (Name/SKU)", value="")
    with c2:
        max_rows = st.number_input("Max. Zeilen", min_value=20, max_value=500, value=80, step=10)
    with c3:
        show_only_changed = st.checkbox("Nur bereits erfasste zeigen", value=False)

    existing = fetch_inputs_day(conn, entry_date)
    existing["d"] = existing.get("d", entry_date.isoformat())

    # base list to show
    master = sku_master.copy()
    if q.strip():
        qq = q.strip().lower()
        master = master[(master["sku"].astype(str).str.lower().str.contains(qq)) | (master["name"].astype(str).str.lower().str.contains(qq))]

    if show_only_changed and not existing.empty:
        master = master[master["sku"].isin(existing["sku"].astype(str))]

    master = master.head(int(max_rows))

    # Merge existing inputs into editable frame
    edit = master.merge(existing, on="sku", how="left")
    edit["d"] = entry_date.isoformat()
    edit["baked_06"] = pd.to_numeric(edit["baked_06"], errors="coerce").fillna(0).astype(int)
    edit["baked_14"] = pd.to_numeric(edit["baked_14"], errors="coerce").fillna(0).astype(int)
    edit["waste_qty"] = pd.to_numeric(edit["waste_qty"], errors="coerce").fillna(0).astype(int)

    # rest fields can be empty
    for col in ["rest_14", "rest_close"]:
        edit[col] = pd.to_numeric(edit[col], errors="coerce")

    edit["note"] = edit["note"].fillna("")

    st.write("### Eingabe-Tabelle")
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

    save_col1, save_col2 = st.columns([1,2])
    with save_col1:
        do_save = st.button("💾 Speichern", type="primary")

    if do_save:
        to_save = edited.copy()
        to_save["sku"] = to_save["sku"].astype(str)

        save_inputs(conn, entry_date, pd.DataFrame({
            "sku": to_save["sku"],
            "baked_06": to_save["baked_06"],
            "baked_14": to_save["baked_14"],
            "rest_14": to_save["rest_14"],
            "rest_close": to_save["rest_close"],
            "waste_qty": to_save["waste_qty"],
            "note": to_save["note"],
        }))

        # ---- Update levels (learning) ----
        # observed demand proxy:
        # We use POS daily sales if available; otherwise use baseline as proxy.
        pos_day = freq_daily[freq_daily["d"] == entry_date].copy()
        base_wd = baseline[baseline["weekday"] == weekday][["sku","base_A","base_B"]].copy()

        merged = base_wd.merge(pos_day[["sku","sales_A","sales_B"]], on="sku", how="left")
        merged = merged.merge(to_save[["sku","rest_close","waste_qty"]], on="sku", how="left")
        merged = merged.merge(levels[levels["weekday"] == weekday][["sku","level_a","level_b"]], on="sku", how="left")

        merged["sales_A"] = merged["sales_A"].fillna(merged["base_A"])
        merged["sales_B"] = merged["sales_B"].fillna(merged["base_B"])
        merged["rest_close"] = pd.to_numeric(merged["rest_close"], errors="coerce").fillna(0.0)
        merged["waste_qty"] = pd.to_numeric(merged["waste_qty"], errors="coerce").fillna(0.0)

        # OOS-Verdacht Heuristik: wenn rest_close=0 und Verkäufe deutlich unter baseline -> treat demand as baseline
        merged["obs_A"] = merged["sales_A"]
        merged["obs_B"] = merged["sales_B"]

        # only apply when POS exists; if not, already using baseline
        merged["oos_suspect_B"] = (merged["rest_close"] <= 0) & (merged["sales_B"] < 0.5 * merged["base_B"])
        merged.loc[merged["oos_suspect_B"], "obs_B"] = merged.loc[merged["oos_suspect_B"], "base_B"]

        # smooth update
        merged["new_level_a"] = (1-ALPHA) * merged["level_a"].fillna(merged["base_A"]) + ALPHA * merged["obs_A"]
        merged["new_level_b"] = (1-ALPHA) * merged["level_b"].fillna(merged["base_B"]) + ALPHA * merged["obs_B"]

        # write back
        for _, r in merged.iterrows():
            upsert_levels(conn, str(r["sku"]), weekday, float(r["new_level_a"]), float(r["new_level_b"]))

        st.success("Gespeichert. Levels (Prognose) wurden aktualisiert.")

    st.divider()
    st.write("### Plausibilitätschecks (für dieses Datum)")
    # We need inputs for this date
    inputs = fetch_inputs_day(conn, entry_date)
    if inputs.empty:
        st.info("Noch keine Eingaben gespeichert.")
    else:
        # attach d
        inputs = inputs.copy()
        inputs["d"] = inputs["d"].astype(str)
        checks = plausibility_checks(freq_daily, baseline, inputs, close_hour=close_hour)
        if checks.empty:
            st.success("Keine Auffälligkeiten gefunden (oder keine POS-Daten für diesen Tag verfügbar).")
        else:
            st.dataframe(checks, use_container_width=True)
            st.caption("Diese Hinweise sind heuristisch (MVP). Sie helfen, Überproduktion, OOS-Verdacht oder Daten-/Zuordnungsfehler zu finden.")
