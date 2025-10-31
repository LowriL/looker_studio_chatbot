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
    page_title="Gemini Data Analytics Chatbot",
    page_icon="🤖",
    layout="wide"
)

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
    Gets the Google Cloud access token using Application Default Credentials (ADC).
    Relies on user being authenticated via `gcloud auth application-default login`.
    """
    try:
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        credentials, project = google.auth.default(scopes=scopes)
        
        auth_req = google.auth.transport.requests.Request()
        credentials.refresh(auth_req)
        
        st.session_state.billing_project = project
        return credentials.token
    except Exception as e:
        st.error(f"Error getting auth token: {e}")
        st.warning("Please ensure you are authenticated locally to Google Cloud. Run `gcloud auth application-default login` in your terminal.", icon="⚠️")
        return None

# --- Streaming Chat Function ---

def stream_chat_response(chat_url, payload, headers):
    """
    A generator function to stream and parse the chat response.
    This adapts the `get_stream_multi_turn` logic from the notebook.
    It yields structured dictionaries for easy rendering in Streamlit.
    """
    s = requests.Session()
    acc = ""  # JSON accumulator

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
                        yield {"type": "dataframe", "content": df}
                
                elif "chart" in msg:
                    if "query" in msg["chart"]:
                        yield {"type": "text", "content": f"**Generating chart for:** *{msg['chart']['query']['instructions']}*"}
                    elif "result" in msg["chart"]:
                        yield {"type": "text", "content": "**Chart generated:**"}
                        yield {"type": "chart", "content": msg["chart"]["result"]["vegaConfig"]}
                
                acc = ""  # Reset accumulator

    except requests.exceptions.RequestException as e:
        yield {"type": "error", "content": f"Request failed: {e}"}
    except Exception as e:
        yield {"type": "error", "content": f"An unexpected error occurred: {e}"}


# --- Streamlit UI ---

st.title("🤖 Gemini Data Analytics Chatbot")
st.caption("A Streamlit interface for the Conversational Analytics API")

# --- Sidebar for Configuration ---
with st.sidebar:
    st.header("Configuration")
    
    # Get token and pre-fill project if available
    access_token = get_access_token()
    
    billing_project = st.text_input(
        "Google Cloud Project ID", 
        value=st.session_state.get("billing_project", ""),
        help="Your GCP Billing Project ID. This is often detected automatically after auth."
    )
    
    location = st.text_input("Location", "global")
    api_version = st.text_input("API Version", "v1beta")
    
    environment = st.selectbox(
        "Environment", 
        ("prod", "autopush", "staging"),
        help="Select the API environment."
    )
    
    data_agent_id = st.text_input(
        "Data Agent ID", 
        "data_agent_1",
        help="The ID of the Data Agent to use."
    )

    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.session_state.conversation_messages = []
        st.rerun()

    st.markdown("---")
    st.markdown(
        "**Note:** This app uses Application Default Credentials (ADC). "
        "Please authenticate in your terminal before running:\n"
        "`gcloud auth application-default login`"
    )

# --- Base URL Logic ---
if environment == "autopush":
    base_url = "https://autopush-geminidataanalytics.sandbox.googleapis.com"
elif environment == "staging":
    base_url = "https://staging-geminidataanalytics.sandbox.googleapis.com"
else:
    base_url = "https://geminidataanalytics.googleapis.com"

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
                current_text += item["content"] + "\n\n"
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
                        st.altair_chart(alt.Chart.from_json(item["content"]), use_container_width=True)
                    except Exception as e:
                        st.error(f"Failed to render chart: {e}")
                elif item["type"] == "error":
                    st.error(item["content"])
        
        # Write any remaining text
        if current_text:
            st.markdown(current_text)

# --- Chat Input ---
if prompt := st.chat_input("Ask your data agent..."):
    # Check for config
    if not all([billing_project, location, data_agent_id, access_token]):
        st.error("Configuration missing. Please fill in all fields in the sidebar and ensure you are authenticated.")
    else:
        # Add user message to UI
        st.session_state.messages.append({"role": "user", "content": [{"type": "text", "content": prompt}]})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Add user message to API context
        st.session_state.conversation_messages.append({"userMessage": {"text": prompt}})

        # Prepare API request
        chat_url = f"{base_url}/{api_version}/projects/{billing_project}/locations/{location}:chat"
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        
        chat_payload = {
            "parent": f"projects/{billing_project}/locations/global",
            "messages": st.session_state.conversation_messages,
            "data_agent_context": {
                "data_agent": f"projects/{billing_project}/locations/{location}/dataAgents/{data_agent_id}",
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
                with placeholder.container():
                    if chunk["type"] == "text":
                        current_text += chunk["content"] + "\n\n"
                        st.markdown(current_text)
                    elif chunk["type"] == "sql":
                        if current_text: st.markdown(current_text) # Write pending text
                        st.code(chunk["content"], language="sql")
                        current_text = "" # Reset text
                    elif chunk["type"] == "dataframe":
                        if current_text: st.markdown(current_text)
                        st.dataframe(chunk["content"])
                        current_text = ""
                    elif chunk["type"] == "chart":
                        if current_text: st.markdown(current_text)
                        try:
                            st.altair_chart(alt.Chart.from_json(chunk["content"]), use_container_width=True)
                        except Exception as e:
                            st.error(f"Failed to render chart: {e}")
                        current_text = ""
                    elif chunk["type"] == "error":
                        if current_text: st.markdown(current_text)
                        st.error(chunk["content"])
                        current_text = ""

            # Add the final accumulated message to session state
            st.session_state.messages.append({"role": "assistant", "content": full_display_list})
            st.session_state.conversation_messages.extend(api_context_list)
