import pandas as pd
import streamlit as st
import logging
from threading import Lock
import os

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

lock = Lock()

def safe_get_value(row, col):
    return row[col] if col in row and pd.notna(row[col]) else 0

def check_mismatch(row, index, column_name, expected_value, mismatched_data):
    actual_value = safe_get_value(row, column_name)
    if actual_value != expected_value:
        mismatched_data.append({
            'Row': index + 3,
            'Date': row['date'],
            'Column': column_name,
            'Expected': expected_value,
            'Actual': actual_value
        })

def pivot_and_average_prices(df):
    # Replace blank values with 'N/A'
    df = df.fillna('N/A')
    
    # Create the pivot table
    pivot_df = df.pivot_table(
        index=['site name', 'vendor', 'session', 'meal type', 'order type', 'buying price ai', 'selling price','remarks'],
        aggfunc='size'
    ).reset_index(name='days')
    
    return pivot_df


def find_mismatches(df):
    mismatched_data = []
    for index, row in df.iterrows():
        try:
            
            calculated_buying_amt = safe_get_value(row, 'selling amount') - safe_get_value(row, 'commission')              
            check_mismatch(row, index, 'buying amt ai', calculated_buying_amt, mismatched_data)
            
            if row['session'] in ['alacarte']:
                calculated_selling_amount = safe_get_value(row, 'selling amount')
                check_mismatch(row, index, 'selling amount', calculated_selling_amount, mismatched_data)
            else:
                calculated_selling_amount = (safe_get_value(row, 'pax sold') * safe_get_value(row, 'rate'))
                check_mismatch(row, index, 'selling amount', calculated_selling_amount, mismatched_data)
                
            calculated_commission = safe_get_value(row, 'selling amount') * safe_get_value(row, 'vendor commission %') 
                                     
            check_mismatch(row, index, 'commission', calculated_commission, mismatched_data)
        except Exception as e:
            logging.error(f"Error processing row {index + 3}: {e}")

    return mismatched_data

def find_karbon_expenses(df):
    karbon_expenses_data = []
    columns_to_check = ['date(karbon)','expense item', 'reason for expense', 'expense type', 'price', 'pax', 'amount', 'mode of payment','bill to','requested by','approved by']
    for index, row in df.iterrows():
        if any(pd.notna(row[col]) and row[col] != 0 for col in columns_to_check):
            karbon_expenses_data.append({
                'Row': index + 3,
                'Buying Amount': row['buying amt ai'],
                'Date': row['date(karbon)'],
                'Expense Item': row['expense item'],
                'Reason for Expense': row['reason for expense'],
                'Expense Type': row['expense type'],
                'Price': row['price'],
                'Pax': row['pax'],
                'Amount': row['amount'],
                'Mode Of Payment': row['mode of payment'],
                'Bill to': row['bill to'],
                'Requested By': row['requested by'],
                'Approved By': row['approved by']
            })

    return karbon_expenses_data

def calculate_aggregated_values(df):

    regular_and_adhoc_orders = df[df['order type'].isin(['regular', 'smartq-pop-up', 'food trial', 'regular-pop-up', 'tuckshop'])]
    sum_buying_amt_ai_regular= regular_and_adhoc_orders['buying amt ai'].sum()
    sum_selling_amt_regular = regular_and_adhoc_orders['selling amount'].sum()

    event_and_popup_orders = df[df['order type'].isin(['event', 'event-pop-up', 'adhoc'])]
    sum_buying_amt_ai_event= event_and_popup_orders['buying amt ai'].sum()
    sum_selling_amt_event = event_and_popup_orders['selling amount'].sum()

    sum_commission = df['commission'].sum()
    sum_amount = df['amount'].sum()

    valid_dates_df = df[(df['pax sold'] > 0)]
    number_of_days = valid_dates_df['date'].nunique()

    aggregated_data = {
        'Number of Days': number_of_days,
        'Buying Amt AI (Regular)': sum_buying_amt_ai_regular,
        'Selling Amt (Regular)': sum_selling_amt_regular,
        'Buying Amt AI (Event)': sum_buying_amt_ai_event,
        'Selling Amt (Event)': sum_selling_amt_event,
        'Commission': sum_commission,
        'Karbon Amount': sum_amount
    }

    return aggregated_data


def format_dataframe(df):
    # Format numerical columns to one decimal place
    for column in df.select_dtypes(include=['float', 'int']).columns:
        df[column] = df[column].map(lambda x: f"{x:.1f}")
    return df

def display_dataframes(pivot_df, mismatched_data, karbon_expenses_data, aggregated_data):
    st.write("Microchip JUICE JUNCTION")
    st.write("Alacarte Price Is Entired Directly")
    st.subheader("Average Buying Price and Selling Price")
    st.table(format_dataframe(pivot_df))
    st.markdown("---")

    if mismatched_data:
        mismatched_df = pd.DataFrame(mismatched_data)
        st.write("<span style='color:red'>Mismatched Data:heavy_exclamation_mark:</span>", unsafe_allow_html=True)
        st.table(format_dataframe(mismatched_df))
        st.markdown("---")
    else:
        st.write("<span style='color:green'>No mismatch found.</span> :white_check_mark:", unsafe_allow_html=True)
        st.markdown("---")


    if karbon_expenses_data:
        karbon_expenses_df = pd.DataFrame(karbon_expenses_data)
        st.subheader("Karbon Expenses")
        st.table(format_dataframe(karbon_expenses_df))
        st.markdown("---")
    else:
        st.write("No Karbon expenses found.")
        st.markdown("---")

    aggregated_df = pd.DataFrame(list(aggregated_data.items()), columns=['Parameter', 'Value'])
    st.subheader("Aggregated Values")
    st.table(format_dataframe(aggregated_df))
    

def business_logic_20(df):
    pivot_df = pivot_and_average_prices(df)
    mismatched_data = find_mismatches(df)
    aggregated_data = calculate_aggregated_values(df)
    karbon_expenses_data = find_karbon_expenses(df)
    display_dataframes(pivot_df, mismatched_data, karbon_expenses_data, aggregated_data)


#---------------------------------Auto P&L Punch-----------------------------------------------------------------------

def load_business_logic(df, selected_month):
    try:
        # Clean up the data by stripping spaces and converting strings to lowercase
        df = df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
        df = df[df['month'] == selected_month]

        if df.empty:
            raise ValueError("No data available for the selected month.")

        # Create the 'identifier' column
        df['identifier'] = df['review id'].combine_first(df['cost centre']) if 'review id' in df.columns else df['cost centre']

        # Filter the data based on 'order type'

        
        regular_amt = df[df['order type'].isin(['regular', 'smartq-pop-up', 'food trial', 'regular-pop-up', 'tuckshop', 'live'])]
        event_amt = df[df['order type'].isin(['event', 'event-pop-up', 'adhoc'])]
        no_of_days = df[(df['buying pax'] > 0) | (df['selling pax'] > 0)]

        # Group the data by 'identifier' and 'month'
        grouped_data = df.groupby(['identifier', 'month'])
        regular_amt_grouped = regular_amt.groupby(['identifier', 'month'])
        event_amt_grouped = event_amt.groupby(['identifier', 'month'])
        no_of_days_grouped = no_of_days.groupby(['identifier', 'month'])

        # Create the P&L DataFrame
        pnl_data = pd.DataFrame({
            'days': no_of_days_grouped['date'].nunique(),
            'regular buying amount': regular_amt_grouped['buying amt ai'].sum(),
            'regular selling amount': regular_amt_grouped['selling amount'].sum(),
            'event buying amount': event_amt_grouped['buying amt ai'].sum(),
            'event selling amount': event_amt_grouped['selling amount'].sum(),
            'sams': grouped_data['amount'].sum()
        }).reset_index()

        return format_dataframe(pnl_data)

    except Exception as e:
        st.error(f"Error loading business logic data: {e}")
        return None

def format_dataframe(df):
    for column in df.select_dtypes(include=['float', 'int']).columns:
        df[column] = df[column].map(lambda x: f"{x:.1f}")
    return df

def load_pnl_data(p_and_l_file_path):
    try:
        pnl_df = pd.read_excel(p_and_l_file_path, header=0)
        pnl_df = pnl_df.applymap(lambda x: x.strip().lower() if isinstance(x, str) else x)
        return pnl_df
    except FileNotFoundError:
        st.error("Output file not found. Please check the file path.")
        return None

def save_updated_data(df, p_and_l_file_path):
    try:
        with lock:
            df.to_excel(p_and_l_file_path, index=False)
            if os.path.exists(p_and_l_file_path):
                os.chmod(p_and_l_file_path, 0o666)
            else:
                st.error(f"File not found: {p_and_l_file_path}")
    except PermissionError:
        st.error("Permission denied: You don't have the necessary permissions to change the permissions of this file.")

def process_data(pnl_df, pnl_data):
    try:
        pnl_mapping = {
            'days': 'days',
            'regular buying amount': 'regular buying',
            'regular selling amount': 'selling -gmv',
            'event buying amount': 'event buying',
            'event selling amount': 'event -gmv',
            'sams': 'sams'
        }

        # Rename columns according to the mapping
        pnl_data = pnl_data.rename(columns=pnl_mapping)

        # Merge data
        pnl_merged_df = pd.merge(pnl_df, pnl_data, left_on=['cost centre', 'month'], right_on=['identifier', 'month'], how='left', suffixes=('', '_new'))

        # Check for unmatched data
        unmatched = pnl_data[~pnl_data.set_index(['identifier', 'month']).index.isin(pnl_merged_df.set_index(['cost centre', 'month']).index)]
        if not unmatched.empty:
            st.error("Could not find a match for cost centre & month.")
            return None, None

        # Update the original data with the new values
        for col in pnl_mapping.values():
            pnl_merged_df[col] = pnl_merged_df[f"{col}_new"].combine_first(pnl_merged_df[col])
        pnl_merged_df.drop(columns=[f"{col}_new" for col in pnl_mapping.values()], inplace=True)

        # Identify rows with updated data
        updated_rows = pnl_merged_df[(pnl_merged_df[list(pnl_mapping.values())] != pnl_df[list(pnl_mapping.values())]).any(axis=1)].copy()

        # Prepare the updated rows DataFrame
        updated_rows = updated_rows[['cost centre', 'month', 'site name'] + list(pnl_mapping.values())]
        updated_rows.columns = ['Cost Centre', 'Month', 'Site Name'] + [f"Updated {col.title().replace('_', ' ')}" for col in pnl_mapping.values()]
        updated_rows.dropna(subset=[f"Updated {col.title().replace('_', ' ')}" for col in pnl_mapping.values()], how='all', inplace=True)

        return pnl_merged_df, updated_rows
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return None, None

def update_p_and_l(df, selected_month, p_and_l_file_path):
    pnl_data = load_business_logic(df, selected_month)
    if pnl_data is None:
        return
    pnl_df = load_pnl_data(p_and_l_file_path)
    if pnl_df is None:
        return
    pnl_merged_df, updated_rows = process_data(pnl_df, pnl_data)
    if pnl_merged_df is not None:
        save_updated_data(pnl_merged_df, p_and_l_file_path)
        st.success("Successfully Punched P&L")
        st.dataframe(format_dataframe(updated_rows))

def clear_p_and_l_data(df, selected_month, p_and_l_file_path):
    pnl_data = load_business_logic(df, selected_month)
    if pnl_data is None:
        return
    pnl_df = load_pnl_data(p_and_l_file_path)
    if pnl_df is None:
        return

    try:
        pnl_mapping = {
            'days': 'days',
            'regular buying amount': 'regular buying',
            'regular selling amount': 'selling -gmv',
            'event buying amount': 'event buying',
            'event selling amount': 'event -gmv',
            'sams': 'sams'
        }

        # Clear the data by setting the relevant columns to None
        for identifier_val, month in zip(pnl_data['identifier'], pnl_data['month']):
            if not ((pnl_df['cost centre'] == identifier_val) & (pnl_df['month'] == month)).any():
                st.error(f"Could not find a match for cost centre {identifier_val} & month {month}.")
                return
            pnl_df.loc[(pnl_df['cost centre'] == identifier_val) & (pnl_df['month'] == month), pnl_mapping.values()] = None

        save_updated_data(pnl_df, p_and_l_file_path)
        st.write("Data cleared successfully!")
    except Exception as e:
        st.error(f"Error clearing data: {e}")


#-------------------------------------------------------Auto Dump--------------------------------------------------

def dump_data(df_filtered, month, dump_file_path):
    dump_mapping = {
        'date': 'date',
        'month': 'month',
        'day': 'day',
        'cost centre' : 'cost centre', 
        'site name': 'site name',
        'vendor code': 'vendor code',
        'vendor': 'vendor',
        'session': 'session',
        'meal type': 'meal type',
        'order type': 'order type',
        'buying amt ai': 'buying amt ai',
        'selling amount': 'selling amount',
        'commission': 'commission',
        'amount': 'amount'
    }
    
    try:
        dump_df = load_dump_data(dump_file_path)
        if dump_df is None:
            return

        if not dump_df.empty:
            last_row = dump_df.iloc[-1]
            last_row_df = last_row.to_frame().T
            last_row_df.insert(0, 'row number', len(dump_df) + 1)
            st.write("Last updated row before current dump:")
            st.dataframe(last_row_df)

        mapped_df = pd.DataFrame()
        for dump_col, df_col in dump_mapping.items():
            if df_col in df_filtered.columns and dump_col in dump_df.columns:
                mapped_df[dump_col] = df_filtered[df_col]

        if 'selling management' in df_filtered.columns:
            selling_sum = df_filtered['selling management'].sum()
            new_row = pd.DataFrame({
                'month': [month],
                'site name': [df_filtered['site name'].iloc[0] if 'site name' in df_filtered.columns else None],
                'order type': ['management fee'],
                'selling amount': [selling_sum]
            })
            dump_df = pd.concat([dump_df, new_row], ignore_index=True)

        updated_df = pd.concat([dump_df, mapped_df], ignore_index=True)
        save_updated_dump_data(updated_df, dump_file_path)
        logging.info("Filtered data appended to the dump file successfully.")
        st.success("Filtered data appended to the dump file successfully.")

    except Exception as e:
        st.error(f"Error dumping data: {e}")
        logging.error(f"Error dumping data: {e}")

def load_dump_data(dump_file_path):
    try:
        dump_df = pd.read_excel(dump_file_path, header=0)
        dump_df.columns = dump_df.columns.str.lower().str.strip()
        return dump_df
    except FileNotFoundError:
        st.write("Output file not found. Please check the file path.")
        return None

def save_updated_dump_data(df, dump_file_path):
    try:
        df.to_excel(dump_file_path, index=False)
        if os.path.exists(dump_file_path):
            os.chmod(dump_file_path, 0o666)
        else:
            st.write("File not found:", dump_file_path)
    except PermissionError:
        st.write("Permission denied: You don't have the necessary permissions to change the permissions of this file.")
