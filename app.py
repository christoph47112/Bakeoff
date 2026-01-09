import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

st.title("Auth Debug Test")

creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

st.write("Credentials geladen")

# ERZWINGE Token-Refresh
creds.refresh(None)

st.success("TOKEN REFRESH OK")
st.stop()
