from snowflake.snowpark.context import get_active_session
import streamlit as st
import pandas as pd
import numpy as np
import xlsxwriter
import io
from io import StringIO
from io import BytesIO

def get_current_db(session):
    try:
        current_account = session.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]  # Fetch account name 
        return 'OPI_IMPL' if "ONEPINP" in current_account.upper() else 'OPI_PRD'
    except Exception as e:
        st.error(f"Error determining database: {e}")
        return None


# Function to clear session state

def clear_session_state(df_key):
    if df_key in st.session_state:
        del st.session_state[df_key]
    st.write("Session state cleared")


#load CSV Files
@st.cache_data
def load_csv(file):
    return pd.read_csv(file)
        

# Function to run SQL queries and store results in session state
@st.cache_data

def run_sql_queries(query1, query2,session):
    if query1 and query2:
        df1 = session.sql(query1).to_pandas()
        df2 = session.sql(query2).to_pandas()
        return df1, df2
    else:
        st.warning("Please enter both SQL queries to proceed.")
        return None, None
    


#Load Excel Files
@st.cache_data
def load_excel(file, sheet_name):
    return pd.read_excel(file, sheet_name=sheet_name)


# Save XLSX of comparison results 
def create_xlsx(df1, df2):
    # Check the size of df1
    if df1.shape[0] > 1048576:
        # Create the Excel file with only df2
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter', engine_kwargs={"options": {"use_zip64": False, "in_memory": True}}) as writer:
            df2.to_excel(writer, sheet_name="Macro Analysis")
        buffer.seek(0)
        return buffer
    else:
        # Create the Excel file with both df1 and df2
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter', engine_kwargs={"options": {"use_zip64": False, "in_memory": True}}) as writer:
            df1.to_excel(writer, sheet_name="Comparison Results")
            df2.to_excel(writer, sheet_name="Macro Analysis")
        buffer.seek(0)
    return buffer



# build comparison DataFrames
def identify_criteria(df1, df2):
    
    criteria_met = True
    dtype_mismatch = {}

    # Standardize column names to uppercase
    df1.columns = [col.upper() for col in df1.columns]
    df2.columns = [col.upper() for col in df2.columns]
    
    try:
        # Length
        if len(df1) != len(df2):
            st.warning(f"Row Error: False")
            st.write(f"Number of rows in df1: {len(df1):,}")
            st.write(f"Number of rows in df2: {len(df2):,}")
            criteria_met = False
        else:
            st.success(f"Rows match: {len(df1) == len(df2)}")
            st.write(f"Number of rows in df1: {len(df1):,}")
            st.write(f"Number of rows in df2: {len(df2):,}")
        
        # Columns
        if list(df1.columns) != list(df2.columns):
            st.warning("Column (#) Error (case insensitive).")
            column_list = pd.DataFrame({'df1_columns': list(df1.columns), 'df2_columns': list(df2.columns)})
            st.write(column_list)
            criteria_met = False
        else:
            st.success(f"Columns match: {list(df1.columns) == list(df2.columns)}")
            st.write(f"Number of columns in df1: {len(df1.columns):,}")
            st.write(f"Number of columns in df2: {len(df2.columns):,}")
        
        # Dtypes
        for col in df1.columns:
            if col in df2.columns:
                if df1[col].dtype != df2[col].dtype:
                    dtype_mismatch[col] = (df1[col].dtype, df2[col].dtype)
            else:
                st.warning(f"Column '{col}' not found in df2")
                criteria_met = False
        
        if dtype_mismatch:
            st.warning("Dtype match: False")
            st.write(pd.DataFrame(dtype_mismatch, index=["df1_dtype", "df2_dtype"]).T)
            criteria_met = False
        else:
            st.success("Dtype match: True")
    except Exception as e:
        st.warning(f"An error occurred: {e}")

    
    return criteria_met, dtype_mismatch



# Fix dfs before processing 
def convert_dtypes(df1, df2, dtype_mismatch, convert_to_float=False, specific_columns=None):
    if convert_to_float:
        if specific_columns:
            columns_to_convert = set(specific_columns)
        else:
            columns_to_convert = set(dtype_mismatch.keys())
        for col in columns_to_convert:
            if col in df1.columns:
                if df1[col].dtype == 'object' and pd.api.types.is_numeric_dtype(df2[col]):
                    df1[col] = pd.to_numeric(df1[col], errors='coerce').astype(float)
                elif pd.api.types.is_numeric_dtype(df1[col]):
                    df1[col] = df1[col].astype(float)
            if col in df2.columns:
                if df2[col].dtype == 'object' and pd.api.types.is_numeric_dtype(df1[col]):
                    df2[col] = pd.to_numeric(df2[col], errors='coerce').astype(float)
                elif pd.api.types.is_numeric_dtype(df2[col]):
                    df2[col] = df2[col].astype(float)
    return df1, df2


def round_and_sort(df1, df2, sort_columns, decimal_places=2):
    # Opted not to care to choose to round. its imperrative 
    # Round the numerical values
    df1 = df1.applymap(lambda x: round(x, decimal_places) if isinstance(x, (int, float)) else x)
    df2 = df2.applymap(lambda x: round(x, decimal_places) if isinstance(x, (int, float)) else x)

    # Sort DataFrames based on the specified columns
    df1 = df1.sort_values(by=sort_columns, ignore_index=True)
    df2 = df2.sort_values(by=sort_columns, ignore_index=True)
    
    return df1, df2



# Compare DataFrames
def test_compare_dataframes(df1, df2, drop_na=False, result_names=("df1", "df2"), **compare_kwargs):
    """
    Compares two dataframes and returns the differences, keeping the index for reference.
    Also prints the percentage of rows that are different.
    Will allow further analysis on the 2 dfs to be viewed and diagnosed. 

    Parameters:
    df1 (pd.DataFrame): First dataframe.
    df2 (pd.DataFrame): Second dataframe.
    drop_na (bool): Whether to drop rows with NaN values.
    result_names (tuple): Names to use for the comparison columns.
    compare_kwargs (dict): Additional keyword arguments for the compare method.

    Returns:
    pd.DataFrame: Dataframe showing the differences, keeping the index for reference.
    """
    try:
        if not df1.equals(df2):
            st.warning("Dataframe values are not equal:")
            comparison = df1.compare(df2, keep_shape=True, keep_equal=True, result_names=result_names, **compare_kwargs)
            
            # Calculate the difference for numerical columns only
            numerical_cols = df1.select_dtypes(include='number').columns
            for col in numerical_cols:
                comparison[(col, 'delta')] = comparison[(col, result_names[0])] - comparison[(col, result_names[1])]
                # Make delta that is 0 null
                comparison.loc[comparison[(col, 'delta')] == 0, (col, 'delta')] = None
            
            # Add boolean column for non-numeric columns
            non_numerical_cols = df1.select_dtypes(exclude='number').columns
            for col in non_numerical_cols:
                # Replace NaN with a placeholder value
                comparison[(col, result_names[0])] = comparison[(col, result_names[0])].fillna('placeholder')
                comparison[(col, result_names[1])] = comparison[(col, result_names[1])].fillna('placeholder')
                
                # Perform the comparison and set 1 for differences
                comparison[(col, 'delta')] = np.where(comparison[(col, result_names[0])] != comparison[(col, result_names[1])], 1, np.nan)
    
            # Reorder the MultiIndex to place 'delta' after the result_names columns
            new_columns = []
            for col in comparison.columns.levels[0]:
                new_columns.append((col, result_names[0]))
                new_columns.append((col, result_names[1]))
                if (col, 'delta') in comparison.columns:
                    new_columns.append((col, 'delta'))
            
            comparison = comparison.reindex(columns=pd.MultiIndex.from_tuples(new_columns))
            
            if drop_na:
                # Drop rows where all columns are NaN (i.e., no deltas)
                comparison = comparison.dropna(how='all')
            
            # Print the number of rows that have at least one non-null 'delta' value 
            rows_to_attend = comparison.loc[:, (slice(None), 'delta')].notnull().any(axis=1).sum()
            st.warning(f"Number of rows that have at least one non-null **delta** value and need to be addressed: {rows_to_attend:,}")
            
            # Calculate the percentage of cells out of total that have a non-null 'delta' value
            total_cells = comparison.loc[:, (slice(None), 'delta')].size
            non_null_cells = comparison.loc[:, (slice(None), 'delta')].notnull().sum().sum()
            percentage_non_null_cells = (non_null_cells / total_cells) * 100
            st.write(f"Percentage of cells out of total that have a non-null 'delta' value: {percentage_non_null_cells:.2f}%")
    
        else:
            st.success(f"Dataframe values are equal")
            comparison = pd.DataFrame()  # Return an empty dataframe if they are equal
        
        return comparison
    
    except ValueError as e:
        st.error("DataFrames are not similar still and cannot be compared.")
        
        # labels_df = pd.DataFrame({
        #             "DataFrame 1 Labels": df1.columns.tolist(),
        #             "DataFrame 2 Labels": df2.columns.tolist()})
        # st.write(labels_df)
        return None
