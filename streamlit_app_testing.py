# new app

import streamlit as st
import pandas as pd
import numpy as np
import xlsxwriter
import io

###########################################################
# Functions 
###########################################################

# Function to load CSV
def load_csv(file):
    return pd.read_csv(file)

# Function to load Excel
def load_excel(file):
    return pd.read_excel(file, sheet_name=None)

# Function to sort DataFrame
def sort_dataframe(df, columns):
    return df.sort_values(by=columns)
    
# Function to Save XLSX of comparison results 
def create_xlsx(df1, df2):
    buffer = BytesIO() 
    with pd.ExcelWriter(buffer, engine='xlsxwriter', engine_kwargs={"options": {"use_zip64": False, "in_memory": True}}) as writer: 
        df1.to_excel(writer, sheet_name="Comparison Results", index=False)
        df2.to_excel(writer, sheet_name="Analysis", index=False)
    buffer.seek(0) 
    return buffer

# Function to build comparison DataFrames
def build_comparison_dfs(df1, df2, sort_columns, convert_to_float=False, round_values=True, decimal_places=2):
    # Round the numerical values
    if round_values:
        df1 = df1.applymap(lambda x: round(x, decimal_places) if isinstance(x, (int, float)) else x)
        df2 = df2.applymap(lambda x: round(x, decimal_places) if isinstance(x, (int, float)) else x)

    # Optionally convert numeric columns to float64
    if convert_to_float:
        for col in df1.select_dtypes(include=[np.number]).columns:
            df1[col] = df1[col].astype(np.float64)
        for col in df2.select_dtypes(include=[np.number]).columns:
            df2[col] = df2[col].astype(np.float64)

    # Sort DataFrames based on the same specified columns
    df1 = df1.sort_values(by=sort_columns, ignore_index=True)
    df2 = df2.sort_values(by=sort_columns, ignore_index=True)

    # Validate if DataFrames have the same columns and length
    if list(df1.columns) != list(df2.columns):
        raise ValueError("DataFrames do not have the same columns.")
    if len(df1) != len(df2):
        raise ValueError("DataFrames do not have the same length.")
    else:
        st.write(f"DataFrames have the same columns: {len(df1.columns)} and length: {len(df1)}.")

    
    # Capture and display DataFrame information
    buffer = io.StringIO()
    df1.info(buf=buffer)
    df1_info = buffer.getvalue()
    buffer = io.StringIO()
    df2.info(buf=buffer)
    df2_info = buffer.getvalue()
    
    st.write("First DataFrame Info:")
    st.text(df1_info)
    st.write("Second DataFrame Info:")
    st.text(df2_info)
    
    return df1, df2


# Function to compare DataFrames
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
        st.write("Dataframes are not equal")
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
        
        # Print the number of rows that have at least one non-null 'delta' value and need to be attended to
        rows_to_attend = comparison.loc[:, (slice(None), 'delta')].notnull().any(axis=1).sum()
        st.write(f"Number of rows that have at least one non-null **delta** value and need to be attended to: {rows_to_attend}")
        
        # Calculate the percentage of cells out of total that have a non-null 'delta' value
        total_cells = comparison.loc[:, (slice(None), 'delta')].size
        non_null_cells = comparison.loc[:, (slice(None), 'delta')].notnull().sum().sum()
        percentage_non_null_cells = (non_null_cells / total_cells) * 100
        st.write(f"Percentage of cells out of total that have a non-null 'delta' value: {percentage_non_null_cells:.2f}%")

    else:
        st.write(f"Dataframes are equal and have {len(df1)} rows")
        comparison = pd.DataFrame()  # Return an empty dataframe if they are equal
    
    return comparison




# Streamlit app
def main():
    st.title("Upload and Sort Two Separate Files (CSV/Excel or SQL)")
    
    # Option to select data source
    data_source = st.selectbox("Select data source", ["CSV/Excel", "SQL"])

    if data_source == "CSV/Excel":
        # File uploader for the first file
        uploaded_file1 = st.file_uploader("Choose the first file", type=["csv", "xlsx"])

        # File uploader for the first file
        uploaded_file1 = st.file_uploader("Choose the first file", type=["csv", "xlsx"])
        if uploaded_file1 is not None:
            if uploaded_file1.name.endswith('.csv'):
                df1 = load_csv(uploaded_file1)
                st.write("First CSV DataFrame:")
                st.dataframe(df1.head(5))
            #excel will upload each tab 
            elif uploaded_file1.name.endswith('.xlsx'):
                df1_sheets = load_excel(uploaded_file1)
                st.write("First Excel File Sheets:")
                for sheet_name, df in df1_sheets.items():
                    st.write(f"Sheet: {sheet_name}")
                    st.dataframe(df.head(5))

        # File uploader for the second file
        uploaded_file2 = st.file_uploader("Choose the second file", type=["csv", "xlsx"])
        if uploaded_file2 is not None:
            if uploaded_file2.name.endswith('.csv'):
                df2 = load_csv(uploaded_file2)
                st.write("Second CSV DataFrame:")
                st.dataframe(df2.head(5))
            elif uploaded_file2.name.endswith('.xlsx'):
                df2_sheets = load_excel(uploaded_file2)
                st.write("Second Excel File Sheets:")
                for sheet_name, df in df2_sheets.items():
                    st.write(f"Sheet: {sheet_name}")
                    st.dataframe(df.head(5))
    # SQL input
    elif data_source == "SQL":
        # Input for SQL connection string
        connection_string = st.text_input("Enter SQL connection string")
        if connection_string:
            connection = sqlite3.connect(connection_string)
            
            # Input for the first SQL query
            sql_query1 = st.text_area("Enter the first SQL query")
            if sql_query1:
                df1 = load_sql(sql_query1, connection)
                st.write("First SQL DataFrame:")
                st.dataframe(df1.head(5))
            
            # Input for the second SQL query
            sql_query2 = st.text_area("Enter the second SQL query")
            if sql_query2:
                df2 = load_sql(sql_query2, connection)
                st.write("Second SQL DataFrame:")
                st.dataframe(df2.head(5))

    # Sort option for both DataFrames
    if uploaded_file1 is not None and uploaded_file2 is not None:
        if uploaded_file1.name.endswith('.xlsx') and uploaded_file2.name.endswith('.xlsx'):
            common_sheets = set(df1_sheets.keys()).intersection(set(df2_sheets.keys()))
            if common_sheets:
                st.write("Common Sheets:")
                st.write(common_sheets)
                selected_sheet = st.selectbox("Select sheet to compare", list(common_sheets))
                df1 = df1_sheets[selected_sheet]
                df2 = df2_sheets[selected_sheet]
            else:
                st.write("No common sheets found.")
                return
        else:
            sort_columns = st.multiselect("Select columns to sort both DataFrames by", df1.columns.intersection(df2.columns))
            if sort_columns:
                decimal_places = st.number_input("Choose number of decimal places", min_value=0, max_value=10, value=2)
                df1, df2 = build_comparison_dfs(df1, df2, sort_columns=sort_columns, decimal_places=decimal_places)

                # Button to trigger comparison
                if st.button("Compare DataFrames"):
                    comparison = test_compare_dataframes(df1, df2)
                    result_analysis = comparison.loc[:, (slice(None), 'delta')].describe().T
                    #issue Analysis
                    st.write("Comparison DataFrame Analysis:")
                    st.dataframe(result_analysis)
                    
                    st.write("Comparison DataFrame Sample:")
                    st.dataframe(comparison.head(5))
                    # Input for output file name
                    output_file_name = st.text_input("Enter the name of the output file (without extension):")
                    # Download Excel Button: Will be the only one after testing
                    if 'header_data' and 'summary_result'and 'bene_dgns_blg_sum_result' in st.session_state:
                        # Get user inputs
                        excel_file_path = st.text_input("Enter the filename (xlsx):", placeholder=f"Will be saved in {run_by_user} stage")
                        if not excel_file_path.endswith('.xlsx'):
                            excel_file_path += '.xlsx'
                        buffer = create_xlsx(comparison, result_analysis)
                        session.file.put_stream(buffer, f"@~/" + excel_file_path, overwrite=True, auto_compress=False)

                        # Download button
                        st.download_button(
                            label="📥 Download Excel",
                            data=buffer,
                            file_name=excel_file_path,
                            mime="application/vnd.ms-excel"
                        )



if __name__ == "__main__":
    main()
