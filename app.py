import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.title("Sheet Access Test")

creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/drive.readonly"],
)
gc = gspread.authorize(creds)

files = gc.list_spreadsheet_files()
st.write("Anzahl sichtbarer Sheets:", len(files))
st.dataframe(files)

st.stop()
