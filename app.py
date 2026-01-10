# =========================================================
# Bake-Off Planer – Version 1.0 (final, eine Seite)
# =========================================================
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date
import time as pytime

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import APIError

# ---------------------------------------------------------
# App Setup
# ---------------------------------------------------------
st.set_page_config(page_title="Bake-Off Planer", layout="wide")

START_DEMAND = 20
START_MORNING_SHARE = 0.75
ALPHA = 0.15

WIN_MORNING = (5, 11)
WIN_AFTERNOON = (12, 17)
WIN_CLOSE = (18, 23)

# ---------------------------------------------------------
# Google Sheets (stabil)
# ---------------------------------------------------------
@st.cache_resource
def gs_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

@st.cache_resource
def sheet():
    return gs_client().open_by_key(st.secrets["SHEET_ID"])

def retry(fn):
    for i in range(5):
        try:
            return fn()
        except APIError:
            pytime.sleep(min(2**i, 8))
    raise

def ensure_tabs():
    sh = sheet()
    tabs = {
        "articles": ["sku","name","active","created_at"],
        "daily_log": ["date","sku","baked_morning","baked_afternoon","waste_qty","early_empty","closed","created_at"],
        "demand_model": ["sku","weekday","demand","morning_share","updated_at"],
    }
    existing = {w.title for w in sh.worksheets()}
    for t, cols in tabs.items():
        if t not in existing:
            sh.add_worksheet(t, rows=2000, cols=10)
        ws = sh.worksheet(t)
        if ws.row_values(1)[:len(cols)] != cols:
            ws.clear()
            ws.update([cols])

def read_tab(name):
    ws = sheet().worksheet(name)
    v = retry(ws.get_all_values)
    if not v:
        return pd.DataFrame()
    return pd.DataFrame(v[1:], columns=v[0])

def write_tab(name, df):
    ws = sheet().worksheet(name)
    ws.clear()
    ws.update([df.columns.tolist()] + df.fillna("").values.tolist())

ensure_tabs()

# ---------------------------------------------------------
# Daten laden
# ---------------------------------------------------------
articles = read_tab("articles")
daily = read_tab("daily_log")
model = read_tab("demand_model")

today = date.today()
wd = today.strftime("%A")
now = datetime.now().hour

# ---------------------------------------------------------
# ARTIKELVERWALTUNG
# ---------------------------------------------------------
st.header("🧺 Artikel")

with st.expander("➕ Artikel anlegen", expanded=False):
    sku = st.text_input("PLU / Artikelnummer")
    name = st.text_input("Artikelname")
    if st.button("Artikel speichern"):
        if sku and name:
            articles = pd.concat([articles, pd.DataFrame([{
                "sku": sku,
                "name": name,
                "active": "TRUE",
                "created_at": datetime.utcnow().isoformat()
            }])]).drop_duplicates("sku", keep="last")
            write_tab("articles", articles)

            weekdays = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            for d in weekdays:
                if not ((model["sku"] == sku) & (model["weekday"] == d)).any():
                    model = pd.concat([model, pd.DataFrame([{
                        "sku": sku,
                        "weekday": d,
                        "demand": START_DEMAND,
                        "morning_share": START_MORNING_SHARE,
                        "updated_at": ""
                    }])])
            write_tab("demand_model", model)
            st.success("Artikel angelegt")
            st.rerun()

articles["active"] = articles["active"].astype(str) == "TRUE"
edited_articles = st.data_editor(
    articles[["sku","name","active"]],
    use_container_width=True,
    num_rows="fixed"
)
if st.button("Artikelstatus speichern"):
    edited_articles["active"] = edited_articles["active"].apply(lambda x: "TRUE" if x else "FALSE")
    edited_articles["created_at"] = datetime.utcnow().isoformat()
    write_tab("articles", edited_articles)
    st.success("Gespeichert")
    st.rerun()

st.divider()

# ---------------------------------------------------------
# AKTIVE ARTIKEL
# ---------------------------------------------------------
active = edited_articles[edited_articles["active"] == True]
if active.empty:
    st.warning("Keine aktiven Artikel.")
    st.stop()

# ---------------------------------------------------------
# MODELL INITIALISIEREN
# ---------------------------------------------------------
for sku in active["sku"]:
    for d in ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]:
        if not ((model["sku"] == sku) & (model["weekday"] == d)).any():
            model = pd.concat([model, pd.DataFrame([{
                "sku": sku,
                "weekday": d,
                "demand": START_DEMAND,
                "morning_share": START_MORNING_SHARE,
                "updated_at": ""
            }])])
write_tab("demand_model", model)

# ---------------------------------------------------------
# HEUTE – PLANUNG
# ---------------------------------------------------------
st.header("🥐 Heute backen wir so")

plan = model[model["weekday"] == wd].merge(active, on="sku")
plan["demand"] = plan["demand"].astype(float)
plan["morning_share"] = plan["morning_share"].astype(float)

plan["morgens"] = (plan["demand"] * plan["morning_share"]).round().astype(int)
plan["nachmittags"] = plan["demand"] - plan["morgens"]
plan["modus"] = np.where(plan["nachmittags"] > 2, "2× backen", "1× backen")

st.dataframe(plan[["name","morgens","nachmittags","modus"]], use_container_width=True)

st.divider()

# ---------------------------------------------------------
# HEUTE – BACKEN EINTRAGEN
# ---------------------------------------------------------
st.header("📝 Heute – beim Backen eintragen")

if not daily.empty:
    today_log = daily[daily["date"] == str(today)]
else:
    today_log = pd.DataFrame()

rows = []
for _, r in plan.iterrows():
    st.subheader(r["name"])
    m = st.number_input("Morgens gebacken", min_value=0, step=1, key=f"m{r['sku']}")
    a = st.number_input("Nachmittags gebacken", min_value=0, step=1, key=f"a{r['sku']}")
    rows.append({
        "date": str(today),
        "sku": r["sku"],
        "baked_morning": m,
        "baked_afternoon": a,
        "waste_qty": 0,
        "early_empty": "FALSE",
        "closed": "FALSE",
        "created_at": datetime.utcnow().isoformat()
    })

if st.button("💾 Backen speichern"):
    df = pd.DataFrame(rows)
    daily = pd.concat([daily, df]).drop_duplicates(["date","sku"], keep="last")
    write_tab("daily_log", daily)
    st.success("Gespeichert")
    st.rerun()

st.divider()

# ---------------------------------------------------------
# TAGESABSCHLUSS
# ---------------------------------------------------------
st.header("🗑️ Tagesabschluss")

rows_close = []
for _, r in plan.iterrows():
    st.subheader(r["name"])
    w = st.number_input("Abschrift", min_value=0, step=1, key=f"w{r['sku']}")
    e = st.checkbox("Vor 14 Uhr leer", key=f"e{r['sku']}")
    rows_close.append((r["sku"], w, e))

if st.button("✅ Fertig für heute"):
    for sku, w, e in rows_close:
        mask = (daily["date"] == str(today)) & (daily["sku"] == sku)
        daily.loc[mask, "waste_qty"] = w
        daily.loc[mask, "early_empty"] = "TRUE" if e else "FALSE"
        daily.loc[mask, "closed"] = "TRUE"

        # Lernen
        sold = daily.loc[mask, "baked_morning"].iloc[0] + daily.loc[mask, "baked_afternoon"].iloc[0] - w
        m_mask = (model["sku"] == sku) & (model["weekday"] == wd)
        model.loc[m_mask, "demand"] = (1-ALPHA)*model.loc[m_mask,"demand"].astype(float) + ALPHA*sold
        if e:
            model.loc[m_mask, "morning_share"] = min(0.95, model.loc[m_mask,"morning_share"].astype(float)+0.05)

    write_tab("daily_log", daily)
    write_tab("demand_model", model)
    st.success("Tag abgeschlossen – App hat gelernt ✅")
    st.rerun()
