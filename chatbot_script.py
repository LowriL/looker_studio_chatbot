import streamlit as st
import json
import requests
import pandas as pd
import altair as alt
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account

# --- Configuration ---
st.set_page_config(
    page_title="Measurelab Data Assistant",
    page_icon="📈",
    layout="wide"
)

# --- Theme ---
def add_custom_css():
    """Injects custom CSS to apply a Measurelab-style theme."""
    st.markdown(
        """
        <style>
            /* --- Measurelab Theme --- */

            /* Main app background */
            .stApp {
                background-color: #FFFFFF; /* Clean white */
            }

            /* Sidebar */
            [data-testid="stSidebar"] {
                background-color: #F0F2F6; /* Light grey sidebar */
                border-right: 1px solid #D0D0D0;
            }
            [data-testid="stSidebar"] .stHeader {
                 color: #0A2B4C; /* Dark blue 'Configuration' */
            }
            [data-testid="stSidebar"] .stMarkdown {
                 color: #333;
            }

            /* Main Title */
            h1 {
                color: #0A2B4C; /* Dark blue */
            }
            
            /* Main caption */
            .stApp > .main .block-container div[data-testid="stMarkdown"] p {
                color: #555555;
            }

            /* Chat Messages */
            [data-testid="stChatMessage"] {
                background-color: #F0F2F6; /* Light grey for assistant */
                border-radius: 8px;
                border: 1px solid #D0D0D0;
                padding: 12px;
            }
            [data-testid="stChatMessage"][data-user-message="true"] {
                background-color: #E0E7FF; /* Lighter blue for user */
                border: 1px solid #C0C7E0;
