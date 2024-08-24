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
        index=['site name', 'vendor','meal type (only lunch)','buying price ai', 'selling price'],
        aggfunc='size'
    ).reset_index(name='days')
    
    return pivot_df



def find_mismatches(df):
    mismatched_data = []
    for index, row in df.iterrows():
        try:
            # for buying price ai
            meal_type = safe_get_value(row, 'meal type (only lunch)')
            buying_mg_pax = safe_get_value(row, 'buying mg/pax')

            if meal_type == "veg":
                if buying_mg_pax <= 500:
                    calculated_buying_price = 49
                elif buying_mg_pax <= 900:
                    calculated_buying_price = 48
                else:
                    calculated_buying_price = 47
            elif meal_type == "non-veg":
                if buying_mg_pax <= 500:
                    calculated_buying_price = 55
                elif buying_mg_pax <= 900:
                    calculated_buying_price = 52.5
                else:
                    calculated_buying_price = 50
            else:
                calculated_buying_price = None
            check_mismatch(row, index, 'buying price ai', calculated_buying_price, mismatched_data)

            # for delta pax
            calculated_delta_pax = max(
                safe_get_value(row, 'buying mg/pax') - (
                    safe_get_value(row, 'actual consumption/employee') +
                    safe_get_value(row, 'partners(direct cash sales)') +
                    safe_get_value(row, 'manual entry') +
                    safe_get_value(row, 'training new joining  staff')) ,safe_get_value(row, 'training new joining  staff'),0)
            check_mismatch(row, index, 'delta pax(gap between mg and consumption)', calculated_delta_pax, mismatched_data)

            # for total pax buying
            calculated_total_pax_buying = ( safe_get_value(row, 'actual consumption/employee') + safe_get_value(row, 'partners(direct cash sales)') +
                safe_get_value(row, 'manual entry') + safe_get_value(row, 'delta pax(gap between mg and consumption)'))
            check_mismatch(row, index, 'total pax buying', calculated_total_pax_buying, mismatched_data)

            # for buying amount
            calculated_buying_amount = (safe_get_value(row, 'total pax buying') * safe_get_value(row, 'buying price ai') * 2)
            check_mismatch(row, index, 'buying amount', calculated_buying_amount, mismatched_data)

            # for selling price
            meal_type = safe_get_value(row, 'meal type (only lunch)')
            selling_mg_pax = safe_get_value(row, 'selling mg/pax')

            if meal_type == "veg":
                if selling_mg_pax <= 500:
                    calculated_selling_price = 51.5
                elif selling_mg_pax <= 900:
                    calculated_selling_price = 50.5
                else:
                    calculated_selling_price = 49.5
            elif meal_type == "non-veg":
                if selling_mg_pax <= 500:
                    calculated_selling_price = 57.5
                elif selling_mg_pax <= 900:
                    calculated_selling_price = 55
                else:
                    calculated_selling_price = 52.5
            else:
                calculated_selling_price = None  # Handle cases where meal type is not specified or invalid

            check_mismatch(row, index, 'selling price', calculated_selling_price, mismatched_data)

            # for delta pax btc
            calculated_delta_pax_btc = max( safe_get_value(row, 'selling mg/pax') - ( safe_get_value(row, 'actual consumption/employee') +safe_get_value(row, 'manual entry')),
                safe_get_value(row, 'training new joining  staff btc'),0)
            check_mismatch(row, index, 'delta pax(gap between mg and consumption) btc', calculated_delta_pax_btc, mismatched_data)

            # for total pax selling
            actual_consumption_employee = safe_get_value(row, 'actual consumption/employee')
            manual_entry = safe_get_value(row, 'manual entry')
            training_new_joining_staff_btc = safe_get_value(row, 'training new joining  staff btc')
            partners_direct_cash_sales = safe_get_value(row, 'partners(direct cash sales)')
            selling_mg_pax = safe_get_value(row, 'selling mg/pax')

            if (actual_consumption_employee + manual_entry + training_new_joining_staff_btc + partners_direct_cash_sales) < selling_mg_pax:
             #----------------------------------------   
                calculated_total_pax_selling = selling_mg_pax
            else:
                calculated_total_pax_selling = actual_consumption_employee + manual_entry + training_new_joining_staff_btc + partners_direct_cash_sales

            check_mismatch(row, index, 'total pax selling', calculated_total_pax_selling, mismatched_data)

            # for partners + employee 50%
            calculated_partners_employee = (
                (safe_get_value(row, 'partners(direct cash sales)') * safe_get_value(row, 'selling price') * 2) +
                ((safe_get_value(row, 'actual consumption/employee') + safe_get_value(row, 'manual entry')) * safe_get_value(row, 'selling price'))
            )
            check_mismatch(row, index, 'partners(direct cash sales) +employee 50%', calculated_partners_employee, mismatched_data)

            # for total sales
            calculated_total_sales = (
                ((safe_get_value(row, 'actual consumption/employee') + safe_get_value(row, 'manual entry')) * safe_get_value(row, 'selling price')) +
                ((safe_get_value(row, 'delta pax(gap between mg and consumption) btc') * safe_get_value(row, 'selling price')) * 2) +
                safe_get_value(row, 'partners(direct cash sales) +employee 50%')
            )
            check_mismatch(row, index, 'total sales', calculated_total_sales, mismatched_data)

            # for btc
            calculated_btc = safe_get_value(row, 'total sales') - safe_get_value(row, 'partners(direct cash sales) +employee 50%')
            check_mismatch(row, index, 'btc', calculated_btc, mismatched_data)

            # for commission
            calculated_commission = safe_get_value(row, 'total sales') - safe_get_value(row, 'buying amount')
            check_mismatch(row, index, 'comission', calculated_commission, mismatched_data)


            
        except Exception as e:
            logging.error(f"Error processing row {index + 3}: {e}")

    return mismatched_data


def calculate_aggregated_values(df):
    sum_buying_pax_regular = df['total pax buying'].sum()
    sum_selling_pax_regular = df['total pax selling'].sum()
    
    sum_buying_amt_ai_regular= df['buying amount'].sum()
    sum_selling_amt_regular = df['btc'].sum()
    sum_cash_recived = df['partners(direct cash sales) +employee 50%'].sum()
    sum_commission = df['comission'].sum()
    

    valid_dates_df = df[(df['total pax buying'] > 0) | (df['total pax selling'] > 0)]
    number_of_days = valid_dates_df['date'].nunique()

    aggregated_data = {
        'Number of Days': number_of_days,
        'Buying Pax (Regular)': sum_buying_pax_regular,
        'Selling Pax (Regular)': sum_selling_pax_regular,
        'Buying Amt AI (Regular)': sum_buying_amt_ai_regular,
        'Selling Amt (Regular)': sum_selling_amt_regular,
        'Cash Recived from Employee': sum_cash_recived,
        'Commission': sum_commission,
    }

    return aggregated_data

def format_dataframe(df):
    # Format numerical columns to one decimal place
    for column in df.select_dtypes(include=['float', 'int']).columns:
        df[column] = df[column].map(lambda x: f"{x:.1f}")
    return df

def display_dataframes(pivot_df, mismatched_data, aggregated_data):
    st.subheader("")
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

   
    aggregated_df = pd.DataFrame(list(aggregated_data.items()), columns=['Parameter', 'Value'])
    st.subheader("Aggregated Values")
    st.table(format_dataframe(aggregated_df))
    

def business_logic_43(df):
    pivot_df = pivot_and_average_prices(df)
    mismatched_data = find_mismatches(df)
    aggregated_data = calculate_aggregated_values(df)
    display_dataframes(pivot_df, mismatched_data, aggregated_data)


        

    
    

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
        no_of_days = df[(df['total pax buying'] > 0) | (df['total pax selling'] > 0)]

        grouped_data = df.groupby(['identifier', 'month'])
        no_of_days_grouped = no_of_days.groupby(['identifier', 'month'])

        pnl_data = pd.DataFrame({
            'days': no_of_days_grouped['date'].nunique(),
            'buying pax': grouped_data['total pax buying'].sum(),
            'selling pax': grouped_data['total pax selling'].sum(),
            'regular buying amount': grouped_data['buying amount'].sum(),
            'regular selling amount': grouped_data['btc'].sum(),
            'cash received': grouped_data['partners(direct cash sales) +employee 50%'].sum(),
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
            'buying pax': 'buying pax',
            'selling pax': 'selling pax',
            'regular buying amount': 'regular buying',
            'regular selling amount': 'selling -gmv',
            'cash received': 'cash received'
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
            'buying pax': 'buying pax',
            'selling pax': 'selling pax',
            'regular buying amount': 'regular buying',
            'regular selling amount': 'selling -gmv',
            'cash received': 'cash received'
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
        'meal type': 'meal type (only lunch)',
        'buying pax': 'total pax buying',
        'buying amt ai': 'buying amount',
        'selling pax': 'total pax selling',
        'selling amount': 'btc',
        'commission': 'comission',
        'cash recived' : 'partners(direct cash sales) +employee 50%'
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
