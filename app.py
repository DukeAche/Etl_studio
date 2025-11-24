import streamlit as st
import pandas as pd
import duckdb
import numpy as np
from datetime import datetime
import io
import os
import re
from typing import Dict, List, Any, Optional
import plotly.express as px
import plotly.graph_objects as go
from streamlit_ace import st_ace
import database

# Page configuration
st.set_page_config(
    page_title="No-Code ETL Studio",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database
database.init_db()

# Initialize session state for transaction logging and data management
if 'transaction_log' not in st.session_state:
    st.session_state.transaction_log = []

if 'current_df' not in st.session_state:
    st.session_state.current_df = None

if 'original_df' not in st.session_state:
    st.session_state.original_df = None

if 'dataframes' not in st.session_state:
    st.session_state.dataframes = {}

if 'query_history' not in st.session_state:
    st.session_state.query_history = []

# Authentication session state
if 'is_authenticated' not in st.session_state:
    st.session_state.is_authenticated = False

if 'current_user' not in st.session_state:
    st.session_state.current_user = None

if 'is_admin' not in st.session_state:
    st.session_state.is_admin = False

# Utility functions
def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def log_transaction(operation: str, details: Dict[str, Any]):
    """Log data transformations for audit trail"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    transaction = {
        "timestamp": timestamp,
        "operation": operation,
        "details": details
    }
    st.session_state.transaction_log.append(transaction)

def add_dataframe(name: str, df: pd.DataFrame):
    """Add dataframe to session state with proper tracking"""
    st.session_state.dataframes[name] = df
    if st.session_state.current_df is None:
        st.session_state.current_df = name

def get_current_df() -> Optional[pd.DataFrame]:
    """Get current working dataframe"""
    if st.session_state.current_df and st.session_state.current_df in st.session_state.dataframes:
        return st.session_state.dataframes[st.session_state.current_df]
    return None

def update_current_df(df: pd.DataFrame):
    """Update current working dataframe"""
    if st.session_state.current_df:
        st.session_state.dataframes[st.session_state.current_df] = df.copy()

# Data ingestion functions
@st.cache_data
def load_csv_file(uploaded_file):
    """Load CSV file with caching"""
    return pd.read_csv(uploaded_file)

@st.cache_data
def load_excel_file(uploaded_file, sheet_name=0):
    """Load Excel file with caching"""
    return pd.read_excel(uploaded_file, sheet_name=sheet_name)

@st.cache_data
def load_json_file(uploaded_file):
    """Load JSON file with caching"""
    return pd.read_json(uploaded_file)

@st.cache_data
def load_parquet_file(uploaded_file):
    """Load Parquet file with caching"""
    return pd.read_parquet(uploaded_file)

@st.cache_resource
def get_db_engine(connection_string: str):
    """Create and cache database engine"""
    from sqlalchemy import create_engine
    return create_engine(connection_string)

def load_from_database(connection_string: str, query: str):
    """Load data from database using SQLAlchemy"""
    try:
        engine = get_db_engine(connection_string)
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return None

@st.cache_data
def generate_profile_report(df: pd.DataFrame) -> pd.DataFrame:
    """Generate profile report with caching"""
    col_info = []
    for col in df.columns:
        col_info.append({
            'Column': col,
            'Data_Type': str(df[col].dtype),
            'Non_Null_Count': int(df[col].count()),
            'Null_Count': int(df[col].isnull().sum()),
            'Unique_Values': int(df[col].nunique()),
            'Sample_Values': str(df[col].dropna().unique()[:3])
        })
    return pd.DataFrame(col_info)

# Authentication Pages
def show_login_page():
    """Display login page"""
    st.title("🔐 Login to ETL Studio")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            st.subheader("Login")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Login", type="primary", use_container_width=True)
            
            if submit:
                if not username or not password:
                    st.error("Please enter both username and password")
                else:
                    success, user_data, message = database.authenticate_user(username, password)
                    if success:
                        st.session_state.is_authenticated = True
                        st.session_state.current_user = user_data
                        st.session_state.is_admin = user_data.get('is_admin', False)
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
        
        st.markdown("---")
        st.markdown("Don't have an account?")
        if st.button("Sign Up", use_container_width=True):
            st.session_state.show_signup = True
            st.rerun()


def show_signup_page():
    """Display signup page"""
    st.title("📝 Create Account")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("signup_form"):
            st.subheader("Sign Up")
            username = st.text_input("Username*")
            email = st.text_input("Email*")
            password = st.text_input("Password*", type="password")
            confirm_password = st.text_input("Confirm Password*", type="password")
            
            col_a, col_b = st.columns(2)
            with col_a:
                submit = st.form_submit_button("Create Account", type="primary", use_container_width=True)
            with col_b:
                back = st.form_submit_button("Back to Login", use_container_width=True)
            
            if back:
                st.session_state.show_signup = False
                st.rerun()
            
            if submit:
                # Validation
                errors = []
                if not username or not username.strip():
                    errors.append("Username is required")
                if not email:
                    errors.append("Email is required")
                elif not validate_email(email):
                    errors.append("Please enter a valid email address")
                if not password:
                    errors.append("Password is required")
                elif len(password) < 6:
                    errors.append("Password must be at least 6 characters")
                if password != confirm_password:
                    errors.append("Passwords do not match")
                
                if errors:
                    for error in errors:
                        st.error(error)
                else:
                    success, message = database.create_user(
                        username.strip(),
                        email,
                        password
                    )
                    if success:
                        st.success(message)
                        st.info("Please login with your new account")
                        st.session_state.show_signup = False
                        st.rerun()
                    else:
                        st.error(message)


# Main application
def main():
    st.title("🔄 No-Code ETL Studio")
    st.markdown("**Transform raw data into gold standard datasets**")
    
    # Sidebar for transaction log and data management
    with st.sidebar:
        # User info and logout
        st.header("👤 User Info")
        if st.session_state.current_user:
            st.write(f"**{st.session_state.current_user['username']}**")
            if st.session_state.is_admin:
                st.badge("Admin", icon="⭐")
            
            if st.button("🚪 Logout", use_container_width=True):
                # Log logout
                database.log_authentication(
                    st.session_state.current_user['id'],
                    st.session_state.current_user['username'],
                    'logout'
                )
                # Clear session
                st.session_state.is_authenticated = False
                st.session_state.current_user = None
                st.session_state.is_admin = False
                st.rerun()
            
            # Change password section
            with st.expander("🔑 Change Password"):
                with st.form("change_password_form"):
                    old_pwd = st.text_input("Current Password", type="password", key="old_pwd")
                    new_pwd = st.text_input("New Password", type="password", key="new_pwd")
                    confirm_pwd = st.text_input("Confirm New Password", type="password", key="confirm_pwd")
                    
                    submit_pwd = st.form_submit_button("Change Password", use_container_width=True)
                    
                    if submit_pwd:
                        if not old_pwd or not new_pwd or not confirm_pwd:
                            st.error("All fields are required")
                        elif new_pwd != confirm_pwd:
                            st.error("New passwords do not match")
                        elif len(new_pwd) < 6:
                            st.error("New password must be at least 6 characters")
                        else:
                            success, message = database.change_password(
                                st.session_state.current_user['id'],
                                old_pwd,
                                new_pwd
                            )
                            if success:
                                st.success(message)
                            else:
                                st.error(message)
        
        st.markdown("---")
        st.header("📊 Session Management")
        
        # DataFrame selector
        if st.session_state.dataframes:
            selected_df = st.selectbox(
                "Active DataFrames",
                options=list(st.session_state.dataframes.keys()),
                key="dataframe_selector"
            )
            st.session_state.current_df = selected_df
            
            df = get_current_df()
            if df is not None:
                st.metric("Rows", len(df))
                st.metric("Columns", len(df.columns))
        
        # Transaction log
        st.header("📝 Transaction Log")
        if st.session_state.transaction_log:
            for transaction in reversed(st.session_state.transaction_log[-10:]):
                with st.expander(f"{transaction['timestamp']}"):
                    st.write(f"**Operation:** {transaction['operation']}")
                    st.write("**Details:**")
                    st.json(transaction['details'])
        else:
            st.info("No transactions logged yet")
        
        # Newsletter Signup
        st.header("📧 Stay Updated")
        with st.form("newsletter_form", clear_on_submit=True):
            email_input = st.text_input("Email Address", placeholder="your@email.com")
            submit_newsletter = st.form_submit_button("Subscribe to Newsletter")
            
            if submit_newsletter:
                if not email_input:
                    st.error("Please enter an email address")
                elif not validate_email(email_input):
                    st.error("Please enter a valid email address")
                else:
                    success, message = database.create_signup(email_input)
                    if success:
                        st.success(message)
                    else:
                        st.warning(message)
    
    # Main tabs for ETL pipeline
    if st.session_state.is_admin:
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "📥 Source (Ingest)", 
            "🔍 Profile (Quality)", 
            "🧮 SQL Workbench", 
            "🔧 Transform", 
            "📤 Sink (Export)",
            "💬 Contact & Support",
            "🔐 Admin Dashboard"
        ])
    else:
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📥 Source (Ingest)", 
            "🔍 Profile (Quality)", 
            "🧮 SQL Workbench", 
            "🔧 Transform", 
            "📤 Sink (Export)",
            "💬 Contact & Support"
        ])
    
    # Phase 1: Source (Ingestion)
    with tab1:
        st.header("📥 Data Ingestion Phase")
        st.markdown("Upload your raw data or connect to a database")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📁 File Upload")
            uploaded_file = st.file_uploader(
                "Choose a file",
                type=['csv', 'xlsx', 'json', 'parquet'],
                key="file_uploader"
            )
            
            if uploaded_file is not None:
                file_details = {
                    "Filename": uploaded_file.name,
                    "File size": f"{uploaded_file.size / 1024:.2f} KB",
                    "File type": uploaded_file.type
                }
                st.json(file_details)
                
                try:
                    df = None
                    if uploaded_file.name.endswith('.csv'):
                        df = load_csv_file(uploaded_file)
                    elif uploaded_file.name.endswith('.xlsx'):
                        # Excel file handling with sheet selection
                        excel_file = pd.ExcelFile(uploaded_file)
                        if len(excel_file.sheet_names) > 1:
                            sheet_name = st.selectbox("Select Sheet", excel_file.sheet_names)
                            df = load_excel_file(uploaded_file, sheet_name)
                        else:
                            df = load_excel_file(uploaded_file)
                    elif uploaded_file.name.endswith('.json'):
                        df = load_json_file(uploaded_file)
                    elif uploaded_file.name.endswith('.parquet'):
                        df = load_parquet_file(uploaded_file)
                    
                    if df is not None:
                        dataframe_name = st.text_input("DataFrame Name", value=f"df_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                        if st.button("Load Data"):
                            add_dataframe(dataframe_name, df)
                            st.session_state.original_df = dataframe_name
                            log_transaction("Data Ingestion", {
                                "source": "file_upload",
                                "filename": uploaded_file.name,
                                "rows": len(df),
                                "columns": list(df.columns)
                            })
                            st.success(f"✅ Data loaded successfully as '{dataframe_name}'")
                            
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
        
        with col2:
            st.subheader("🗄️ Database Connection")
            with st.expander("Database Connection (SQLAlchemy)"):
                connection_string = st.text_input(
                    "Connection String",
                    placeholder="postgresql://user:password@host:port/database",
                    help="SQLAlchemy compatible connection string"
                )
                
                query = st.text_area(
                    "SQL Query",
                    placeholder="SELECT * FROM your_table LIMIT 1000",
                    height=100
                )
                
                if st.button("Fetch Data from Database"):
                    if connection_string and query:
                        with st.spinner("Connecting to database..."):
                            df = load_from_database(connection_string, query)
                            if df is not None:
                                dataframe_name = st.text_input("DataFrame Name", value=f"db_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                                if st.button("Load Database Data"):
                                    add_dataframe(dataframe_name, df)
                                    log_transaction("Database Ingestion", {
                                        "source": "database",
                                        "query": query[:100] + "..." if len(query) > 100 else query,
                                        "rows": len(df),
                                        "columns": list(df.columns)
                                    })
                                    st.success(f"✅ Database data loaded successfully as '{dataframe_name}'")
                    else:
                        st.warning("Please provide both connection string and query")
        
        # Display bronze data preview
        if get_current_df() is not None:
            st.subheader("🥉 Bronze Data Preview")
            df = get_current_df()
            st.dataframe(df.head(100), use_container_width=True)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Phase 2: Data Profiling (Quality Check)
    with tab2:
        st.header("🔍 Data Profiling & Quality Assessment")
        
        df = get_current_df()
        if df is not None:
            # Data Health Score Calculation
            total_cells = len(df) * len(df.columns)
            missing_cells = df.isnull().sum().sum()
            duplicate_rows = df.duplicated().sum()
            
            # Health score calculation
            completeness = (total_cells - missing_cells) / total_cells * 100
            uniqueness = (len(df) - duplicate_rows) / len(df) * 100
            health_score = (completeness + uniqueness) / 2
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Data Health Score", f"{health_score:.1f}%")
            with col2:
                st.metric("Missing Values", missing_cells)
            with col3:
                st.metric("Duplicate Rows", duplicate_rows)
            with col4:
                st.metric("Total Cells", total_cells)
            
            # Missing values visualization
            st.subheader("📊 Missing Values Analysis")
            missing_data = df.isnull().sum()
            missing_pct = (missing_data / len(df) * 100).round(2)
            
            missing_df = pd.DataFrame({
                'Column': missing_data.index,
                'Missing_Count': missing_data.values,
                'Missing_Percentage': missing_pct.values
            }).sort_values('Missing_Percentage', ascending=False)
            
            fig = px.bar(missing_df, x='Column', y='Missing_Percentage', 
                        title='Missing Values by Column (%)')
            st.plotly_chart(fig, use_container_width=True)
            
            # Data types overview
            st.subheader("🔧 Data Types Overview")
            dtype_counts = df.dtypes.value_counts()
            st.bar_chart(dtype_counts)
            
            # Column information
            st.subheader("📋 Column Information")
            col_info_df = generate_profile_report(df)
            st.dataframe(col_info_df, use_container_width=True)
        else:
            st.warning("⚠️ No data loaded. Please upload data in the Source tab first.")
    
    # Phase 3: SQL Workbench (Killer Feature)
    with tab3:
        st.header("🧮 SQL Workbench - Query Your Data")
        st.markdown("Write SQL queries directly against your DataFrames using DuckDB")
        
        df = get_current_df()
        if df is not None:
            # SQL Query Editor
            st.subheader("📝 SQL Editor")
            
            # Available tables info
            with st.expander("📚 Available Tables & Schema"):
                st.info(f"Current DataFrame: '{st.session_state.current_df}'")
                st.write("**Schema:**")
                schema_df = pd.DataFrame({
                    'Column': df.columns,
                    'Data_Type': [str(dtype) for dtype in df.dtypes]
                })
                st.dataframe(schema_df, use_container_width=True)
            
            # SQL examples
            sql_examples = {
                "Basic SELECT": f"SELECT * FROM {st.session_state.current_df} LIMIT 10",
                "Filter Data": f"SELECT * FROM {st.session_state.current_df} WHERE column_name > 100",
                "Aggregation": f"SELECT category_column, COUNT(*) as count, AVG(numeric_column) as avg_value FROM {st.session_state.current_df} GROUP BY category_column",
                "Join Example": f"SELECT a.*, b.other_column FROM {st.session_state.current_df} a JOIN other_df b ON a.id = b.id",
                "Data Cleaning": f"SELECT * FROM {st.session_state.current_df} WHERE column_name IS NOT NULL ORDER BY date_column DESC"
            }
            
            selected_example = st.selectbox("Load SQL Example", ["Custom Query"] + list(sql_examples.keys()))
            
            if selected_example != "Custom Query":
                default_query = sql_examples[selected_example]
            else:
                default_query = f"SELECT * FROM {st.session_state.current_df} LIMIT 10"
            
            # SQL Editor using streamlit-ace
            sql_query = st_ace(
                value=default_query,
                language='sql',
                theme='monokai',
                keybinding='vscode',
                font_size=14,
                tab_size=4,
                wrap=True,
                auto_update=True,
                height=200,
                key="sql_editor"
            )
            
            # Query execution
            col1, col2, col3 = st.columns(3)
            with col1:
                execute_query = st.button("▶️ Execute Query", type="primary")
            with col2:
                save_query = st.button("💾 Save Query Result")
            with col3:
                clear_query = st.button("🗑️ Clear Query")
            
            if execute_query and sql_query.strip():
                try:
                    # Register all dataframes with DuckDB
                    conn = duckdb.connect()
                    
                    # Register all available dataframes
                    for df_name, df_data in st.session_state.dataframes.items():
                        conn.register(df_name, df_data)
                    
                    with st.spinner("Executing SQL query..."):
                        result_df = conn.execute(sql_query).fetchdf()
                        
                        # Store query result temporarily
                        st.session_state.temp_query_result = result_df
                        
                        # Display results
                        st.subheader("📊 Query Results")
                        st.metric("Rows Returned", len(result_df))
                        st.dataframe(result_df, use_container_width=True)
                        
                        # Add to query history
                        query_info = {
                            "query": sql_query,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "rows_returned": len(result_df)
                        }
                        st.session_state.query_history.append(query_info)
                        
                        conn.close()
                        
                except Exception as e:
                    st.error(f"SQL Error: {str(e)}")
                    st.info("💡 Tip: Make sure your table names match the DataFrame names and SQL syntax is correct")
            
            if save_query and 'temp_query_result' in st.session_state:
                result_df = st.session_state.temp_query_result
                query_name = st.text_input("Query Result Name", 
                                         value=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
                
                if st.button("Save as New DataFrame"):
                    add_dataframe(query_name, result_df)
                    log_transaction("SQL Query", {
                        "query": sql_query[:100] + "..." if len(sql_query) > 100 else sql_query,
                        "result_name": query_name,
                        "rows_returned": len(result_df)
                    })
                    st.success(f"✅ Query result saved as '{query_name}'")
                    
                    # Switch to the new dataframe
                    st.session_state.current_df = query_name
            
            # Query history
            if st.session_state.query_history:
                st.subheader("📜 Query History")
                for i, query_info in enumerate(reversed(st.session_state.query_history[-5:])):
                    with st.expander(f"Query {len(st.session_state.query_history) - i} ({query_info['timestamp']})"):
                        st.code(query_info['query'], language='sql')
                        st.metric("Rows Returned", query_info['rows_returned'])
        else:
            st.warning("⚠️ No data loaded. Please upload data in the Source tab first.")
    
    # Phase 4: Transformation
    with tab4:
        st.header("🔧 Data Transformation & Cleaning")
        st.markdown("Clean and transform your data with no-code operations")
        
        df = get_current_df()
        if df is not None:
            st.subheader("🎯 Quick Transformations")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("🗑️ Drop Duplicates"):
                    original_rows = len(df)
                    df_cleaned = df.drop_duplicates()
                    rows_dropped = original_rows - len(df_cleaned)
                    update_current_df(df_cleaned)
                    log_transaction("Drop Duplicates", {
                        "rows_dropped": rows_dropped,
                        "remaining_rows": len(df_cleaned)
                    })
                    st.success(f"✅ Dropped {rows_dropped} duplicate rows")
            
            with col2:
                fill_method = st.selectbox("Fill Missing Values", ["None", "Forward Fill", "Backward Fill", "Zero", "Mean", "Median"])
                if fill_method != "None" and st.button("🔧 Apply Fill"):
                    original_missing = df.isnull().sum().sum()
                    df_filled = df.copy()
                    
                    if fill_method == "Forward Fill":
                        df_filled = df_filled.fillna(method='ffill')
                    elif fill_method == "Backward Fill":
                        df_filled = df_filled.fillna(method='bfill')
                    elif fill_method == "Zero":
                        df_filled = df_filled.fillna(0)
                    elif fill_method == "Mean":
                        numeric_cols = df_filled.select_dtypes(include=[np.number]).columns
                        df_filled[numeric_cols] = df_filled[numeric_cols].fillna(df_filled[numeric_cols].mean())
                    elif fill_method == "Median":
                        numeric_cols = df_filled.select_dtypes(include=[np.number]).columns
                        df_filled[numeric_cols] = df_filled[numeric_cols].fillna(df_filled[numeric_cols].median())
                    
                    new_missing = df_filled.isnull().sum().sum()
                    filled_count = original_missing - new_missing
                    update_current_df(df_filled)
                    log_transaction("Fill Missing Values", {
                        "method": fill_method,
                        "values_filled": filled_count,
                        "remaining_missing": new_missing
                    })
                    st.success(f"✅ Filled {filled_count} missing values")
            
            with col3:
                if st.button("✂️ Trim Whitespace"):
                    df_trimmed = df.copy()
                    string_cols = df_trimmed.select_dtypes(include=['object']).columns
                    trim_count = 0
                    
                    for col in string_cols:
                        original_values = df_trimmed[col].astype(str).str.contains(r'^\s+|\s+$', regex=True).sum()
                        df_trimmed[col] = df_trimmed[col].astype(str).str.strip()
                        trim_count += original_values
                    
                    update_current_df(df_trimmed)
                    log_transaction("Trim Whitespace", {
                        "columns_affected": len(string_cols),
                        "values_trimmed": trim_count
                    })
                    st.success(f"✅ Trimmed whitespace from {trim_count} values")
            
            # Column operations
            st.subheader("📝 Column Operations")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Column renaming
                st.markdown("**Rename Columns**")
                rename_mapping = {}
                
                for col in df.columns:
                    new_name = st.text_input(f"Rename '{col}'", value=col, key=f"rename_{col}")
                    if new_name != col:
                        rename_mapping[col] = new_name
                
                if rename_mapping and st.button("Apply Renaming"):
                    df_renamed = df.rename(columns=rename_mapping)
                    update_current_df(df_renamed)
                    log_transaction("Rename Columns", {
                        "renamed_columns": rename_mapping
                    })
                    st.success(f"✅ Renamed {len(rename_mapping)} columns")
                    st.rerun()
            
            with col2:
                # Type casting
                st.markdown("**Change Data Types**")
                type_mapping = {}
                
                for col in df.columns:
                    current_type = str(df[col].dtype)
                    new_type = st.selectbox(
                        f"Convert '{col}' ({current_type})",
                        ["Keep Current", "string", "int64", "float64", "datetime64", "boolean"],
                        key=f"type_{col}"
                    )
                    if new_type != "Keep Current":
                        type_mapping[col] = new_type
                
                if type_mapping and st.button("Apply Type Conversion"):
                    df_converted = df.copy()
                    converted_count = 0
                    
                    for col, new_type in type_mapping.items():
                        try:
                            if new_type == "datetime64":
                                df_converted[col] = pd.to_datetime(df_converted[col])
                            else:
                                df_converted[col] = df_converted[col].astype(new_type)
                            converted_count += 1
                        except Exception as e:
                            st.warning(f"Could not convert {col} to {new_type}: {str(e)}")
                    
                    update_current_df(df_converted)
                    log_transaction("Type Conversion", {
                        "conversions": type_mapping,
                        "successful_conversions": converted_count
                    })
                    st.success(f"✅ Converted {converted_count} columns")
                    st.rerun()
        else:
            st.warning("⚠️ No data loaded. Please upload data in the Source tab first.")
    
    # Phase 5: Sink (Export)
    with tab5:
        st.header("📤 Data Export")
        st.markdown('Export your cleaned "Gold Standard" dataset')
        
        df = get_current_df()
        if df is not None:
            st.subheader("📊 Export Options")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Export Format**")
                export_format = st.selectbox(
                    "Format",
                    ["CSV", "Parquet", "JSON", "Excel"],
                    help="Parquet is optimized for performance and storage"
                )
                
                filename = st.text_input(
                    "Filename",
                    value=f"gold_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )
                
                compression = st.selectbox(
                    "Compression (where applicable)",
                    ["none", "gzip", "zip", "bz2", "xz"]
                )
            
            with col2:
                st.markdown("**Data Summary**")
                st.metric("Final Rows", len(df))
                st.metric("Final Columns", len(df.columns))
                st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
                
                # Data quality metrics
                missing_pct = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
                duplicate_pct = (df.duplicated().sum() / len(df)) * 100
                
                st.metric("Missing Data", f"{missing_pct:.2f}%")
                st.metric("Duplicate Rows", f"{duplicate_pct:.2f}%")
            
            # Export functionality
            st.subheader("🚀 Export Data")
            
            if st.button("📥 Export Dataset", type="primary"):
                try:
                    if export_format == "CSV":
                        if compression != "none":
                            filename_ext = f"{filename}.csv.{compression}"
                        else:
                            filename_ext = f"{filename}.csv"
                        
                        csv_buffer = io.StringIO()
                        df.to_csv(csv_buffer, index=False, compression=compression if compression != "none" else None)
                        csv_data = csv_buffer.getvalue()
                        
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv_data,
                            file_name=filename_ext,
                            mime="text/csv"
                        )
                    
                    elif export_format == "Parquet":
                        parquet_buffer = io.BytesIO()
                        df.to_parquet(parquet_buffer, index=False, compression='snappy')
                        parquet_data = parquet_buffer.getvalue()
                        
                        st.download_button(
                            label="📥 Download Parquet",
                            data=parquet_data,
                            file_name=f"{filename}.parquet",
                            mime="application/octet-stream"
                        )
                    
                    elif export_format == "JSON":
                        json_buffer = io.StringIO()
                        df.to_json(json_buffer, orient='records', indent=2)
                        json_data = json_buffer.getvalue()
                        
                        st.download_button(
                            label="📥 Download JSON",
                            data=json_data,
                            file_name=f"{filename}.json",
                            mime="application/json"
                        )
                    
                    elif export_format == "Excel":
                        excel_buffer = io.BytesIO()
                        df.to_excel(excel_buffer, index=False, engine='openpyxl')
                        excel_data = excel_buffer.getvalue()
                        
                        st.download_button(
                            label="📥 Download Excel",
                            data=excel_data,
                            file_name=f"{filename}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
                    log_transaction("Data Export", {
                        "format": export_format,
                        "filename": filename,
                        "compression": compression,
                        "rows": len(df),
                        "columns": len(df.columns)
                    })
                    
                    st.success(f"✅ Export prepared successfully!")
                    
                except Exception as e:
                    st.error(f"Export error: {str(e)}")
            
            # Transaction log export
            st.subheader("📋 Export Transaction Log")
            if st.session_state.transaction_log:
                log_df = pd.DataFrame(st.session_state.transaction_log)
                log_csv = log_df.to_csv(index=False)
                
                st.download_button(
                    label="📥 Download Transaction Log",
                    data=log_csv,
                    file_name=f"etl_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        else:
            st.warning("⚠️ No data loaded. Please upload and process data first.")
    
    # Phase 6: Contact & Support
    with tab6:
        st.header("💬 Contact & Support")
        st.markdown("Have questions? Need help? We're here for you!")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("📝 Send us a message")
            with st.form("contact_form", clear_on_submit=True):
                name_input = st.text_input("Name*", placeholder="Your name")
                email_input = st.text_input("Email*", placeholder="your@email.com")
                message_input = st.text_area(
                    "Message*", 
                    placeholder="How can we help you?",
                    height=150
                )
                
                submit_contact = st.form_submit_button("Send Message", type="primary")
                
                if submit_contact:
                    # Validation
                    errors = []
                    if not name_input or not name_input.strip():
                        errors.append("Name is required")
                    if not email_input:
                        errors.append("Email is required")
                    elif not validate_email(email_input):
                        errors.append("Please enter a valid email address")
                    if not message_input or not message_input.strip():
                        errors.append("Message is required")
                    
                    if errors:
                        for error in errors:
                            st.error(error)
                    else:
                        success, message = database.create_contact_submission(
                            name_input.strip(),
                            email_input,
                            message_input.strip()
                        )
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
        
        with col2:
            st.subheader("📊 Quick Stats")
            try:
                newsletter_count = database.get_signup_count()
                contact_count = database.get_contact_count()
                
                st.metric("Newsletter Subscribers", newsletter_count)
                st.metric("Contact Messages", contact_count)
            except:
                st.info("Stats unavailable")
            
            st.subheader("💡 Quick Tips")
            st.markdown("""
            - Use the **SQL Workbench** to query your data
            - Enable **Data Profiling** for quality checks
            - Export to **Parquet** for best performance
            - Check the **Transaction Log** for audit trails
            """)
    
    # Phase 7: Admin Dashboard (only visible to admins)
    if st.session_state.is_admin:
        with tab7:
            st.header("🔐 Admin Dashboard")
            st.markdown("Monitor user authentication and system activity")
            
            # Stats section
            st.subheader("📊 System Statistics")
            try:
                stats = database.get_auth_stats()
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Users", stats['total_users'])
                with col2:
                    st.metric("Total Logins", stats['total_logins'])
                with col3:
                    st.metric("Failed Logins", stats['failed_logins'])
                with col4:
                    newsletter_count = database.get_signup_count()
                    st.metric("Newsletter Subs", newsletter_count)
            except Exception as e:
                st.error(f"Error loading stats: {str(e)}")
            
            st.markdown("---")
            
            # Authentication logs
            st.subheader("🔍 Authentication Logs")
            
            col_filter1, col_filter2, col_filter3 = st.columns(3)
            with col_filter1:
                log_limit = st.selectbox("Show last", [50, 100, 200, 500], index=1)
            with col_filter2:
                action_filter = st.selectbox(
                    "Filter by action",
                    ["All", "login", "logout", "failed_login", "signup"]
                )
            with col_filter3:
                if st.button("🔄 Refresh Logs"):
                    st.rerun()
            
            try:
                # Get logs
                logs = database.get_authentication_logs(limit=log_limit)
                
                # Filter if needed
                if action_filter != "All":
                    logs = [log for log in logs if log['action'] == action_filter]
                
                if logs:
                    # Convert to DataFrame for better display
                    logs_df = pd.DataFrame(logs)
                    
                    # Format timestamp
                    logs_df['timestamp'] = pd.to_datetime(logs_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Reorder columns
                    column_order = ['timestamp', 'username', 'action', 'user_id', 'ip_address']
                    logs_df = logs_df[[col for col in column_order if col in logs_df.columns]]
                    
                    # Display with conditional formatting
                    st.dataframe(
                        logs_df,
                        use_container_width=True,
                        column_config={
                            "action": st.column_config.TextColumn(
                                "Action",
                                help="Authentication action type"
                            ),
                            "timestamp": st.column_config.TextColumn(
                                "Time",
                                help="When the action occurred"
                            )
                        }
                    )
                    
                    # Download logs as CSV
                    csv = logs_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Logs as CSV",
                        data=csv,
                        file_name=f"auth_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                    
                    # Action breakdown chart
                    st.subheader("📈 Activity Breakdown")
                    action_counts = logs_df['action'].value_counts()
                    fig = px.pie(
                        values=action_counts.values,
                        names=action_counts.index,
                        title="Authentication Actions Distribution"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No authentication logs found")
            except Exception as e:
                st.error(f"Error loading logs: {str(e)}")


if __name__ == "__main__":
    # Initialize show_signup flag
    if 'show_signup' not in st.session_state:
        st.session_state.show_signup = False
    
    # Route based on authentication status
    if not st.session_state.is_authenticated:
        if st.session_state.show_signup:
            show_signup_page()
        else:
            show_login_page()
    else:
        main()