# new app

import streamlit as st
import pandas as pd
import numpy as np
import xlsxwriter
import io
st.set_page_config(layout="wide")
from io import StringIO
from io import BytesIO
from snowflake.snowpark.context import get_active_session
session = get_active_session()
# Fetch User's EUA ID
run_by_user = session.sql("select current_user()").collect()[0][0]  

###########################################################
# Functions 
###########################################################


#load CSV Files
def load_csv(file):
    return pd.read_csv(file)

#Load Excel Files
# def load_excel(file):
#     """Load an Excel file and return a dictionary of DataFrames for each sheet."""
#     xls = pd.ExcelFile(file)
#     sheets = {sheet_name: xls.parse(sheet_name) for sheet_name in xls.sheet_names}
#     return sheets

    
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
def build_comparison_dfs(df1, df2, sort_columns,convert_to_float=False, float_columns=None,
                        round_values=True,decimal_places=2):
 # Standardize column names to lowercase
    df1.columns = [col.upper() for col in df1.columns]
    df2.columns = [col.upper() for col in df2.columns]
        
    # Validate if DataFrames have the same columns and length
    if list(df1.columns) != list(df2.columns):
        st.warning("DataFrames do not have the same columns (case insensitive).")
        column_list = pd.DataFrame({'df1_columns': list(df1.columns),'df2_columns': list(df2.columns)})
    if len(df1) != len(df2):
        st.warning("DataFrames do not have the same length.")
        
    # Validate if DataFrames have the same data types for each column
    dtype_mismatch = {col: (df1[col].dtype, df2[col].dtype) for col in df1.columns if df1[col].dtype != df2[col].dtype}
    if dtype_mismatch:
        st.warning("DataFrames do not have the same data types for the following columns:")
        st.write(pd.DataFrame(dtype_mismatch, index=["df1_dtype", "df2_dtype"]).T)
        
        float_columns = st.multiselect("Select columns to convert to float", list(dtype_mismatch.keys()))
        convert_to_float = True
    else:
        st.success(f"DataFrames have the same # of columns: {len(df1.columns)}, length: {len(df1):,} and dtypes.")
    if convert_to_float and float_columns:
         for col in float_columns:
             if col in df1.columns and col in df2.columns:
                 try:
                     df1[col] = pd.to_numeric(df1[col], errors='coerce').astype(np.float64)
                     df2[col] = pd.to_numeric(df2[col], errors='coerce').astype(np.float64)
                 except ValueError as e:
                     st.error(f"Error converting column {col} to float: {e}")


    # Round the numerical values
    if round_values:
        df1 = df1.applymap(lambda x: round(x, decimal_places) if isinstance(x, (int, float)) else x)
        df2 = df2.applymap(lambda x: round(x, decimal_places) if isinstance(x, (int, float)) else x)

    
    # Sort DataFrames based on the same specified columns
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



###############################################################################
# Streamlit app
###############################################################################
def main():
    st.title("Upload and Compare Two Separate Files (CSV-Excel-SQL)")
    
    # Option to select data source
    data_source = st.selectbox("Select data source", ["CSV", "Excel", "SQL"])

###########################################################################################################  
                    # CSV input
###########################################################################################################
    if data_source == "CSV":
        st.header(" Upload CSV Files")
        # File uploader for the first CSV file
        uploaded_file1 = st.file_uploader("Choose the first CSV file", type=["csv"])
        if uploaded_file1 is not None:
            df1 = load_csv(uploaded_file1)
            st.write("First CSV DataFrame:")
            st.dataframe(df1.head(5))

        # File uploader for the second CSV file
        uploaded_file2 = st.file_uploader("Choose the second CSV file", type=["csv"])
        if uploaded_file2 is not None:
            df2 = load_csv(uploaded_file2)
            st.write("Second CSV DataFrame:")
            st.dataframe(df2.head(5))
          
    
###########################################################################################################  
             # EXCEL input
###########################################################################################################    
    if data_source == "Excel":
        # --- File Upload ---
        st.header(" Upload Excel Files")
        
        uploaded_file_1 = st.file_uploader("Upload First Excel File (.xlsx)", type="xlsx", key="file1")
        uploaded_file_2 = st.file_uploader("Upload Second Excel File (.xlsx)", type="xlsx", key="file2")

        # --- Global variables for dataframes and sheet names ---
        df1 = pd.DataFrame()
        df2 = pd.DataFrame()
        processed_df1 = pd.DataFrame()
        processed_df2 = pd.DataFrame()
        sheet_name_1 = None
        sheet_name_2 = None
        common_columns = []

        if uploaded_file_1 and uploaded_file_2:
            st.header("Select Sheets to Compare")
            

            try:
                # Get sheet names without loading full data yet
                xls1 = pd.ExcelFile(uploaded_file_1)
                sheet_names_1 = xls1.sheet_names
        
                xls2 = pd.ExcelFile(uploaded_file_2)
                sheet_names_2 = xls2.sheet_names
        
                col1_sheet, col2_sheet = st.columns(2)
                with col1_sheet:
                    sheet_name_1 = st.selectbox(f"'{uploaded_file_1.name}'", sheet_names_1, key="sheet1")
                with col2_sheet:
                    sheet_name_2 = st.selectbox(f"'{uploaded_file_2.name}'", sheet_names_2, key="sheet2")
        
            except Exception as e:
                st.error(f"Error reading Excel file structure: {e}")
                st.stop() # Stop execution if reading sheets fails
                # --- Data Processing ---
        if sheet_name_1 and sheet_name_2:
            try:
                # Load selected sheets into dataframes
                df1 = pd.read_excel(uploaded_file_1, sheet_name=sheet_name_1)
                df2 = pd.read_excel(uploaded_file_2, sheet_name=sheet_name_2)
                
                # st.write(f"Selected Sheet: {selected_sheet}")
                st.dataframe(df1.head(5))
                st.dataframe(df2.head(5))
                    
            except Exception as e:
                st.error(f"Error processing dataframes: {e}")
                st.stop()
   
        
###########################################################################################################  
    # SQL input
###########################################################################################################
    elif data_source == "SQL":
        st.header(f"PLease enter 2 SQL statements with db and schema")
        # Input for the first SQL query
        sql_query_1 = st.text_area("Enter the first SQL query")
        # Input for the second SQL query
        sql_query_2 = st.text_area("Enter the second SQL query")
        if st.button("Run SQL Queries"):
            if sql_query_1:
                df1 = session.sql(sql_query_1).to_pandas()
                st.write("First SQL DataFrame:")
                st.dataframe(df1.head(5))
            if sql_query_2:
                df2 = session.sql(sql_query_2).to_pandas()
                st.write("Second SQL DataFrame:")
                st.dataframe(df2.head(5))
            else:
                st.warning("Please enter both SQL queries to proceed.")



###########################################################################################################  
    # Full Processing
###########################################################################################################
 # Sort option for both DataFrames
    
# Check if df1 and df2 are defined and are DataFrames
    if 'df1' in locals() and isinstance(df1, pd.DataFrame) and 'df2' in locals() and isinstance(df2, pd.DataFrame):
        if not df1.empty and not df2.empty:
            st.header("Processing Data")
            # Convert column names to uppercase for sorting
            df1.columns = [col.upper() for col in df1.columns]
            df2.columns = [col.upper() for col in df2.columns]
            #Sort for analysis 
            sort_columns = st.multiselect("Select columns to sort both DataFrames by", df1.columns.intersection(df2.columns))
            if sort_columns:       
                decimal_places = st.number_input("Choose number of decimal places", min_value=0, max_value=10, value=2)
                
                df1_processed, df2_processed = build_comparison_dfs(df1, df2, sort_columns=sort_columns, decimal_places=decimal_places)
                st.header("Compare Data")
                # Button to trigger comparison
                if st.button("Compare DataFrames"):
                    comparison = test_compare_dataframes(df1_processed, df2_processed)
                    if not comparison.empty:
                        comparison_deltas = comparison[comparison.loc[:, (slice(None), 'delta')].notnull().any(axis=1)]
                        result_analysis = comparison.loc[:, (slice(None), 'delta')].describe().T
                       
                        st.write("Result Analysis:")
                        st.dataframe(result_analysis, width=1200, height=400)
                        
                        st.write("Comparison Results:")
                        st.dataframe(comparison_deltas.head(10))
                        st.success("Comparison complete!")
    
                        # Save information
                        # generate the excel for downloading before output
                        buffer = create_xlsx(comparison_deltas, result_analysis)
        
                        # Write and Save to Excel 
                        excel_file_path = st.text_input("Enter the filename (xlsx):")
                        
                        # Download button
                        st.download_button(
                            label="📥 Download Excel"
                            ,data=buffer
                            ,file_name=excel_file_path
                            ,mime="application/vnd.ms-excel"
                        )


if __name__ == "__main__":
    main()
