import streamlit as st
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request

st.title("Auth Debug Test")

creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
)

st.write("Credentials geladen")

try:
    creds.refresh(Request())
    st.success("TOKEN REFRESH OK")
    st.write("Token läuft ab um:", creds.expiry)
except Exception as e:
    st.error("TOKEN REFRESH FAILED")
    st.exception(e)

st.stop()
