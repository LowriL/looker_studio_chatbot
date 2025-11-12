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
    """
    Injects custom CSS to apply the new theme based on the provided
    Tailwind/shadcn variable definitions.
    """
    st.markdown(
        """
        <style>
            /* 1. Import Poppins Font */
            @import url('https://fonts.googleapis.com/css2?family=Poppins:ital,wght@0,100;0,200;0,300;0,400;0,500;0,600;0,700;0,800;0,900;1,100;1,200;1,300;1,400;1,500;1,600;1,700;1,800;1,900&display=swap');

            /* 2. Define the core variables from your file */
            :root {
                --background: oklch(1.0000 0 0);
                --foreground: oklch(0.1408 0.0044 285.8229);
                --primary: oklch(0.7879 0.1991 139.5227);
                --primary-foreground: oklch(1.0000 0 0);
                --secondary: oklch(0.9579 0.0013 106.4242);
                --secondary-foreground: oklch(0.2163 0.0435 230.7774);
                --muted: oklch(0.9674 0.0013 286.3752);
                --muted-foreground: oklch(0.3730 0.0340 259.7330);
                --border: oklch(0.9197 0.0040 286.3202);
                --radius: 0.625rem;
                --sidebar: oklch(0.9851 0 0);
                --sidebar-foreground: oklch(0.1408 0.0044 285.8229);
                --font-sans: 'Poppins', sans-serif;
            }
            
            /* Hide sidebar */
            [data-testid="stSidebar"] {
                display: none;
            }
            
            /* Adjust main app margin now that sidebar is gone */
            .main .block-container {
                max-width: 100%;
                padding-left: 2rem;
                padding-right: 2rem;
            }

            /* 3. Apply the theme to Streamlit components */
            
            body {
                font-family: var(--font-sans);
            }

            /* Main app background */
            .stApp {
                background-color: var(--background);
                color: var(--foreground);
                font-family: var(--font-sans);
            }

            /* Main Title */
            h1 {
                color: var(--foreground);
                font-family: var(--font-sans);
            }
            
            /* Main caption */
            .stApp > .main .block-container div[data-testid="stMarkdown"] p {
                color: var(--muted-foreground);
            }
            
            /* Clear chat button */
            .stButton>button {
                background-color: var(--primary);
                color: var(--primary-foreground);
                border: none;
                border-radius: var(--radius);
                padding: 0.25rem 0.75rem;
                font-size: 0.875rem;
            }
            .stButton>button:hover {
                background-color: var(--primary);
                opacity: 0.85;
                color: var(--primary-foreground);
                border: none;
            }

            /* Chat Messages */
            [data-testid="stChatMessage"] {
                background-color: var(--secondary); /* Assistant message */
                border-radius: var(--radius);
                border: 1px solid var(--border);
                color: var(--secondary-foreground);
            }
            [data-testid="stChatMessage"][data-user-message="true"] {
                background-color: var(--muted); /* User message */
                border: 1px solid var(--border);
                color: var(--muted-foreground);
            }
            
            /* Auth warning */
            [data-testid="stWarning"] {
                background-color: oklch(0.98 0.09 90); /* Light yellow */
                border-color: oklch(0.8 0.1 90); /* Darker yellow border */
                border-radius: var(--radius);
                padding: 1rem;
            }
            [data-testid="stWarning"] code {
                background-color: oklch(0.9 0.05 90);
                padding: 2px 5px;
                border-radius: 4px;
            }

        </style>
        """,
        unsafe_allow_html=True,
    )

add_custom_css()


# --- Helper Functions from Notebook ---

def is_json(myjson):
    """Checks if a string is valid JSON."""
    try:
        json.loads(myjson)
    except ValueError:
        return False
    return True

def get_property(data, field_name, default=""):
    """Safely gets a property from a dictionary."""
    return data.get(field_name, default)

def format_bq_table_ref(table_ref):
    """Formats a BigQuery table reference."""
    return "{}.{}.{}".format(
        table_ref.get("projectId", "unknown-project"),
        table_ref.get("datasetId", "unknown-dataset"),
        table_ref.get("tableId", "unknown-table")
    )

def format_looker_table_ref(table_ref):
    """Formats a Looker table reference."""
    return "lookmlModel: {}, explore: {}".format(
        table_ref.get("lookmlModel", "unknown-model"),
        table_ref.get("explore", "unknown-explore")
    )

def parse_schema_to_dataframe(datasources):
    """Parses schema information into DataFrames for display."""
    dfs = []
    for datasource in datasources:
        source_name = ""
        if "studioDatasourceId" in datasource:
            source_name = f"Looker Studio: {datasource['studioDatasourceId']}"
        elif "lookerExploreReference" in datasource:
            source_name = f"Looker: {format_looker_table_ref(datasource['lookerExploreReference'])}"
        else:
            source_name = f"BigQuery: {format_bq_table_ref(datasource['bigqueryTableReference'])}"
        
        fields = datasource.get("schema", {}).get("fields", [])
        df = pd.DataFrame(
            {
                "Column": [get_property(f, "name") for f in fields],
                "Type": [get_property(f, "type") for f in fields],
                "Description": [get_property(f, "description", "-") for f in fields],
                "Mode": [get_property(f, "mode") for f in fields],
            }
        )
        dfs.append((source_name, df))
    return dfs

def parse_data_to_dataframe(result):
    """Parses data result into a DataFrame."""
    fields = [get_property(f, "name") for f in result.get("schema", {}).get("fields", [])]
    data = result.get("data", [])
    
    # Create a dictionary of lists for the DataFrame
    data_dict = {}
    for field in fields:
        data_dict[field] = [get_property(el, field) for el in data]
        
    return pd.DataFrame(data_dict)

# --- Authentication ---

@st.cache_data(ttl=3000) # Cache token for 50 minutes
def get_access_token():
    """
    Gets the Google Cloud access token using service account keys
    stored as individual Streamlit Secrets.
    """
    try:
        # Check for the minimal required keys
        required_keys = ["type", "project_id", "private_key", "client_email"]
        if not all(key in st.secrets for key in required_keys):
            st.error("Missing one or more required GCP service account keys in Streamlit secrets.")
            st.info("Please open your service account JSON file and add each key-value pair as a separate secret in your Streamlit app settings.")
            st.stop()

        # Build the credentials dictionary from individual secrets
        # The .replace() for private_key is a common fix for
        # newlines being stored as text.
        creds_dict = {
            "type": st.secrets["type"],
            "project_id": st.secrets["project_id"],
            "private_key_id": st.secrets.get("private_key_id"),
            "private_key": st.secrets["private_key"].replace('\\n', '\n'),
            "client_email": st.secrets["client_email"],
            "client_id": st.secrets.get("client_id"),
            "auth_uri": st.secrets.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": st.secrets.get("token_uri", "https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": st.secrets.get("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
            "client_x509_cert_url": st.secrets.get("client_x509_cert_url")
        }
        
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict, scopes=scopes
        )
        
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        
        return credentials.token
    
    except Exception as e:
        st.error(f"Error getting auth token from service account secrets: {e}")
        st.stop()

# --- Streaming Chat Function ---

def stream_chat_response(chat_url, payload, headers):
    """
    A generator function to stream and parse the chat response.
    This adapts the `get_stream_multi_turn` logic from the notebook.
    It yields structured dictionaries for easy rendering in Streamlit.
    """
    s = requests.Session()
    acc = ""  # JSON accumulator
    latest_data_rows = None

    try:
        with s.post(chat_url, json=payload, headers=headers, stream=True, timeout=600) as resp:
            if resp.status_code != 200:
                yield {"type": "error", "content": f"API Error {resp.status_code}: {resp.text}"}
                return

            for line in resp.iter_lines():
                if not line:
                    continue

                decoded_line = str(line, encoding="utf-8")

                if decoded_line == "[{":
                    acc = "{"
                elif decoded_line == "}]":
                    acc += "}"
                elif decoded_line == ",":
                    continue
                else:
                    acc += decoded_line

                if not is_json(acc):
                    continue

                # --- Valid JSON object received ---
                data_json = json.loads(acc)
                
                # Yield the raw API message for context
                yield {"type": "api_message", "content": data_json}

                if "error" in data_json:
                    err = data_json["error"]
                    yield {"type": "error", "content": f"Code: {err.get('code')}\nMessage: {err.get('message')}"}
                    continue

                if "systemMessage" not in data_json:
                    continue

                msg = data_json["systemMessage"]

                # Handle different message types
                if "text" in msg:
                    yield {"type": "text", "content": "".join(msg["text"]["parts"])}
                
                elif "schema" in msg:
                    if "query" in msg["schema"]:
                        yield {"type": "text", "content": f"**Resolving schema for:** *{msg['schema']['query']['question']}*"}
                    elif "result" in msg["schema"]:
                        yield {"type": "text", "content": "**Schema resolved. Data sources:**"}
                        dfs = parse_schema_to_dataframe(msg["schema"]["result"]["datasources"])
                        for source_name, df in dfs:
                            yield {"type": "text", "content": f"**{source_name}**"}
                            yield {"type": "dataframe", "content": df}
                
                elif "data" in msg:
                    if "query" in msg["data"]:
                        query = msg["data"]["query"]
                        yield {"type": "text", "content": f"**Retrieval Query:** *{query['question']}*"}
                    elif "generatedSql" in msg["data"]:
                        yield {"type": "text", "content": "**Generated SQL:**"}
                        yield {"type": "sql", "content": msg["data"]["generatedSql"]}
                    elif "result" in msg["data"]:
                        yield {"type": "text", "content": "**Data retrieved:**"}
                        df = parse_data_to_dataframe(msg["data"]["result"])
                        latest_data_rows = msg["data"]["result"].get("data", [])
                        yield {"type": "dataframe", "content": df}
                    elif "result" in msg["chart"]:
                        yield {"type": "text", "content": "**Chart generated:**"}
                        spec = msg["chart"]["result"]["vegaConfig"]
                        if latest_data_rows is not None:
                            spec["data"] = {"values": latest_data_rows}
                            latest_data_rows = None
                        yield {"type": "chart", "content": spec}
                
                acc = ""  # Reset accumulator

    except requests.exceptions.RequestException as e:
        yield {"type": "error", "content": f"Request failed: {e}"}
    except Exception as e:
        yield {"type": "error", "content": f"An unexpected error occurred: {e}"}


# --- Streamlit UI ---

# --- Hardcoded Configuration ---
BILLING_PROJECT = "ml-aihub-d-efbcu"
LOCATION = "global"
API_VERSION = "v1beta"
ENVIRONMENT = "prod" # "prod", "autopush", "staging"
DATA_AGENT_ID = "demo_data_agent_webinar"

# --- Base URL Logic ---
if ENVIRONMENT == "autopush":
    base_url = "https://autopush-geminidataanalytics.sandbox.googleapis.com"
elif ENVIRONMENT == "staging":
    base_url = "https://staging-geminidataanalytics.sandbox.googleapis.com"
else:
    base_url = "https://geminidataanalytics.googleapis.com"


st.title("📈 Measurelab Data Assistant")

# --- Clear Chat Button ---
col1, col2 = st.columns([0.8, 0.2])
with col1:
    st.caption("Powered by the Gemini Conversational Analytics API")
with col2:
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.session_state.conversation_messages = []
        st.rerun()

st.markdown("---") # Visual separator

# --- Initialize session state ---
if "messages" not in st.session_state:
    st.session_state.messages = []  # For displaying in Streamlit UI
if "conversation_messages" not in st.session_state:
    st.session_state.conversation_messages = []  # For sending to API

# --- Display chat history ---
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        # The content is a list of chunks (text, df, chart)
        content_list = message["content"]
        
        current_text = ""
        for item in content_list:
            if item["type"] == "text":
                current_text += item["content"]
            else:
                # If we have pending text, write it
                if current_text:
                    st.markdown(current_text)
                    current_text = ""
                
                # Write the non-text block
                if item["type"] == "sql":
                    st.code(item["content"], language="sql")
                elif item["type"] == "dataframe":
                    st.dataframe(item["content"])
                elif item["type"] == "chart":
                    try:
                        st.altair_chart(alt.Chart.from_dict(chunk["content"]), use_container_width=True)
                    except Exception as e:
                        st.error(f"Failed to render chart: {e}")
                elif item["type"] == "error":
                    st.error(item["content"])
        
        # Write any remaining text
        if current_text:
            st.markdown(current_text)

# --- Chat Input ---
if prompt := st.chat_input("Ask your data agent..."):
    # Get token *only* when user sends a message
    access_token = get_access_token()
    
    # Check for auth
    if not access_token:
        st.error("Authentication failed. Please check the terminal and auth warning.")
    else:
        # Add user message to UI
        st.session_state.messages.append({"role": "user", "content": [{"type": "text", "content": prompt}]})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Add user message to API context
        st.session_state.conversation_messages.append({"userMessage": {"text": prompt}})

        # Prepare API request
        chat_url = f"{base_url}/{API_VERSION}/projects/{BILLING_PROJECT}/locations/{LOCATION}:chat"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        
        chat_payload = {
            "parent": f"projects/{BILLING_PROJECT}/locations/global",
            "messages": st.session_state.conversation_messages,
            "data_agent_context": {
                "data_agent": f"projects/{BILLING_PROJECT}/locations/{LOCATION}/dataAgents/{DATA_AGENT_ID}",
            },
        }

        # Stream the response
        with st.chat_message("assistant"):
            placeholder = st.empty()
            full_display_list = []  # To store chunks for session state
            api_context_list = []   # To store API messages for context
            
            generator = stream_chat_response(chat_url, chat_payload, headers)
            
            current_text = "" # Accumulator for text chunks

            for chunk in generator:
                if chunk["type"] == "api_message":
                    api_context_list.append(chunk["content"])
                    continue

                # Add to display list
                full_display_list.append(chunk)
                
                # Render all content for this turn so far
                if chunk["type"] == "text":
                    current_text += chunk["content"]
                    with placeholder.container():
                        st.markdown(current_text)
                
                else:
                    # A non-text chunk is about to be rendered.
                    # First, render any text that was accumulated *before* it.
                    with placeholder.container():
                        if current_text:
                            st.markdown(current_text) # Write pending text
                        
                        # Now render the non-text chunk
                        if chunk["type"] == "sql":
                            st.code(chunk["content"], language="sql")
                        elif chunk["type"] == "dataframe":
                            st.dataframe(chunk["content"])
                        elif chunk["type"] == "chart":
                            try:
                                st.altair_chart(alt.Chart.from_dict(chunk["content"]), use_container_width=True)
                            except Exception as e:
                                st.error(f"Failed to render chart: {e}")
                        elif chunk["type"] == "error":
                            st.error(chunk["content"])
                    
                    # Reset the text accumulator and the placeholder
                    current_text = ""
                    placeholder = st.empty()

            # After the loop, if there's any remaining text,
            # ensure it's rendered.
            if current_text:
                with placeholder.container():
                    st.markdown(current_text)

        # Add the final accumulated message to session state
        st.session_state.messages.append({"role": "assistant", "content": full_display_list})
        st.session_state.conversation_messages.extend(api_context_list)
