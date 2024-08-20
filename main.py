import pandas as pd
import logging
import streamlit as st
import importlib

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Paths to the predefined files
P_AND_L_FILE_PATH = r"C:\Users\Darshan.Pawar\OneDrive - CPGPLC\Auto P&L\P&L.xlsx"
DUMP_FILE_PATH = r"C:\Users\Darshan.Pawar\OneDrive - CPGPLC\Auto P&L\Dump.xlsx"

def setup_page():
    # Set up the Streamlit page configuration
    st.set_page_config(page_title="Monthly MIS Checker", layout="wide")
    st.title("MIS Reviewer :chart_with_upwards_trend:")

def upload_file():
    # Sidebar file uploader for Excel files
    return st.sidebar.file_uploader('Upload Excel file', type=['xlsx', 'xls'])

def read_excel_file(uploaded_file):
    # Read the uploaded Excel file
    try:
        excel_file = pd.ExcelFile(uploaded_file)
        logging.info("Excel file uploaded successfully.")
        return excel_file
    except ValueError as e:
        st.error(f"Error reading the Excel file: {e}")
        logging.error(f"ValueError reading the Excel file: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        logging.error(f"Unexpected error reading the Excel file: {e}")
    return None

def select_sheet(excel_file):
    # Select a sheet from the uploaded Excel file
    sheet_names = excel_file.sheet_names
    return st.sidebar.selectbox('Select a sheet to display', sheet_names)

def read_sheet_to_dataframe(uploaded_file, selected_sheet):
    # Read the selected sheet into a DataFrame
    try:
        df = pd.read_excel(uploaded_file, sheet_name=selected_sheet, header=1)
        logging.info(f"Sheet '{selected_sheet}' loaded successfully.")
        return df
    except ValueError as e:
        st.error(f"ValueError reading the sheet '{selected_sheet}': {e}")
        logging.error(f"ValueError reading the sheet '{selected_sheet}': {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        logging.error(f"Unexpected error reading the sheet '{selected_sheet}': {e}")
    return None

def preprocess_dataframe(df):
    # Preprocess the DataFrame by converting column names to lowercase
    try:
        df.columns = df.columns.str.lower().str.strip()
        columns_to_convert = df.columns.difference(['date'])
        df[columns_to_convert] = df[columns_to_convert].apply(lambda col: col.str.lower().str.strip() if col.dtype == 'object' else col)
        logging.info("Columns converted to lower case successfully.")
    except Exception as e:
        st.error(f"Error processing the data: {e}")
        logging.error(f"Error processing the data: {e}")
    return df

def filter_dataframe_by_month(df):
    # Filter the DataFrame by the selected month
    try:
        if 'month' in df.columns:
            available_months = df['month'].unique()
            if 'selected_month' not in st.session_state or st.session_state.selected_month not in available_months:
                st.session_state.selected_month = available_months[0]  # Default to the first month if not set or invalid
            month = st.sidebar.selectbox("Select the month for review", available_months, index=available_months.tolist().index(st.session_state.selected_month))
            st.session_state.selected_month = month
            df_filtered = df[df['month'] == month]
            logging.info(f"Data filtered by month '{month}' successfully.")
            return df_filtered, month
        else:
            st.error("The 'month' column is not present in the dataframe.")
            logging.error("The 'month' column is not present in the dataframe.")
            return None, None
    except KeyError as e:
        st.error(f"KeyError filtering data by month: {e}")
        logging.error(f"KeyError filtering data by month: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        logging.error(f"Unexpected error filtering data by month: {e}")
    return None, None

def apply_business_logic(df_filtered, selected_sheet, month):
    # Define the business logic mapping
    business_logic_sheets = {

            "business_logic_1": [""],
            "business_logic_2": ["Odessa","Scaler-Prequin","Vector","Quzizz"],
            "business_logic_3": ["Synergy","Amadeus","Awfis"],
            "business_logic_4": ["Medtrix","MG Eli Lilly","Tekion."],
            "business_logic_5": ["Microchip Main Meal","DTCC Company Paid"],
            "business_logic_6": ["HD Works"],
            "business_logic_7": ["MPL"],
            "business_logic_8": ["Tadano Escorts","Dynasty","Citrix Driver's Lunch & Dinner","sharefile"],
            "business_logic_9": ["Rippling","Tessolve","Plain View","Ajuba","Corning", "O9 Solutions","Pratilipi"],
            "business_logic_10": ["MPL - Infinity Plates","Groww Koramangala","Groww VTP","Groww Mumbai.","Ather Mumbai","Epam"],
            "business_logic_11": ["Telstra MainMeal(Cash & Carry)"],
            "business_logic_12": ["Eli Lilly Wallet."], # get this clarified
            "business_logic_13": ["Schneider Sodexo Card."],
            "business_logic_14": ["RAKUTEN-2","Clario"],
            "business_logic_15": ["Waters Main Meal"], # used BL6 and might be same for seminens
            "business_logic_16": ["Quest Company Paid"],
            "business_logic_17": ["Waters Tuck Shop"],
            "business_logic_18": ["H&M"],
            "business_logic_19": ["Lam Research","PhonePe"],
            "business_logic_20": ["Micochip Juice Junction"],
            "business_logic_21": ["Ather BLR"],
            "business_logic_22": ["Ather Plant 1.","Ather Plant 2.","SAEL Delhi","Gojek."],  #gojek is ncr
            "business_logic_23": ["STRIPE MIS","TEA-Breakfast"],
            "business_logic_24": ["FRUIT N JUICE MIS"],
            "business_logic_25": ["Siemens","Toasttab","Gartner"],
            "business_logic_26": ["DTCC Wallet"],
            "business_logic_27": ["Siemens_Pune"],
            "business_logic_28": ["CSG-Pune"],
            "business_logic_29": ["Salesforce-GGN"],
            "business_logic_30": ["Salesforce - Jaipur"],
            "business_logic_31": ["Ather - Main Meal"],
            "business_logic_32": ["Siemens_NCR"], # NCR
            "business_logic_33": ["Postman_NCR","Citrix-Tuckshop"],
            "business_logic_34": ["Sinch Lunch"],
            "business_logic_35": ["Sinch Dinner"],
            "business_logic_36": ["STRYKER MIS - '2024"],
            "business_logic_37": ["EGL"],
            "business_logic_38": ["Truecaller"],
            "business_logic_39": ["Sharefile Wallet"],
            "business_logic_40": ["Gold Hill-Main Meal","Goldhill Juice Junction.","Healthineer International","Priteck - Main meal","Pritech park Juice junction"],
            "business_logic_41": ["Siemens-BLR","Siemens Juice Counter"],
            "business_logic_42": ["Heathineer Factory"],
            "business_logic_43": ["Airtel Center","Airtel  Plot 5","Airtel NOC Non veg","Airtel international"],
            "business_logic_44": ["Tekion"],
            "business_logic_45": ["HD Works(HYD)"],
            "business_logic_46": ["Airtel Noida"],
            "business_logic_47": ["Airtel NOC"],
            "business_logic_48": ["Airtel-Jaya"],
            "business_logic_49": ["MIQ"],
            "business_logic_50": ["MIQ MRP"],
            "business_logic_51": ["Telstra New"],
            "business_logic_52": ["Telstra (Tea Coffee)"],
            "business_logic_53": ["Accenture MDC2B","BDC7A Transport Tea","HDC 5A Transport Tea","HDC 1i OLD ","HDC 1i Sky View 10","MIS Transport Tea DDC 4","MIS Transport Tea - DDC 3"],
            "business_logic_54": ["Gojek"],
            "business_logic_55": ["Junglee MIS"],
            "business_logic_56": ["Tonbo"],
            "business_logic_57": ["Sinch"],
            "business_logic_58": ["Schneider-2"],
            "business_logic_59": ["DTCC Wallet"],
            "business_logic_60": ["Telstra-Tuck Shop"],
            "business_logic_61": ["Drivers Tea HYD","Drivers Tea Blore","Drivers Tea Chennai","Siemens - Tuckshop","Tadano Escorts"],
            "business_logic_62": ["LPG"],
            "business_logic_63": ["ABM -MEAL"],
            "business_logic_64": ["JUNGLEE GAMES GUR"],
            "business_logic_65": ["Sharefile consumables"],









            "event_logic_1": ["Telstra Event.","Events","WF Hyd Events-Reformat","WF Chennai Events-Reformat","WF BLR Events-Reformat"],
            "event_logic_2": ["Eli Lilly Event"],
            "event_logic_3": ["Waters Event"],
            "event_logic_4": ["Sinch Event sheet","infosys Event+ Additional Sales","Other Events.","Telstra Event sheet","MPL-Delhi","Grow event","LTIMindTree-event",
                              "Mumbai Other Events","JUNGLEE GAMES GUR EVENT","Pune Event MIS", "Salesforce Jaipur - Gurgaon MRP"],
            "event_logic_5": ["Other Events"],
            "event_logic_6": ["Lam Research Event"],
            "event_logic_7": ["ICON CHN EVENT"],
            "event_logic_8": ["other Event MIS"],
            "event_logic_9": ["Amazon  PNQ Events -"],
            "event_logic_10": ["Pan India Event MIS"],
            "event_logic_11": ["Telstra Event"],
            "event_logic_12": ["Airtel Event"],
            "event_logic_13": ["Icon-event-Bangalore"],


            "other_revenues": ["New Other Revenues"],
            "welfrgo_other_revenues": ["wellsFargo Other Revenues"],

                  # Your business logic mapping here...
    }

    # Determine which business logic to apply based on the selected sheet
    business_logic_module = None
    for module_name, sheets in business_logic_sheets.items():
        if selected_sheet in sheets:
            business_logic_module = module_name
            break

    # Apply the business logic if found
    if business_logic_module:
        try:
            module = importlib.import_module(business_logic_module)
            business_logic_function = getattr(module, business_logic_module)
            business_logic_function(df_filtered)
            logging.info(f"Business logic '{business_logic_module}' applied successfully.")

            pnl_data = module.load_business_logic(df_filtered, month)
            if pnl_data is not None:
                #st.write("\nP&L Data:\n")
                #st.table(pnl_data)

                tab1, tab2 = st.tabs(["Punch P&L", "Clear"])
                st.write("---")
                with tab1:
                    if st.button("Punch P&L"):
                        module.update_p_and_l(df_filtered, month, P_AND_L_FILE_PATH)
                with tab2:
                    if st.button("Clear"):
                        module.clear_p_and_l_data(df_filtered, month, P_AND_L_FILE_PATH)
            else:
                st.write("No P&L data to display.")
        except ModuleNotFoundError:
            st.error(f"Business logic module '{business_logic_module}' not found.")
            logging.error(f"Business logic module '{business_logic_module}' not found.")
        except AttributeError:
            st.error(f"Function '{business_logic_module}' not found in the module.")
            logging.error(f"Function '{business_logic_module}' not found in the module.")
        except Exception as e:
            st.error(f"Error applying business logic: {e}")
            logging.error(f"Error applying business logic: {e}")

        # Handle the dump section
        try:
            if st.button("Create Dump"):
                dump_function = getattr(module, 'dump_data')
                dump_function(df_filtered, month, DUMP_FILE_PATH)
        except Exception as e:
            st.error(f"Error in dump section: {e}")
            logging.error(f"Error in dump section: {e}")

    else:
        st.write("No business logic defined for this sheet.")
        logging.warning("No business logic defined for the selected sheet.")

def main():
    setup_page()
    uploaded_file = upload_file()

    if uploaded_file:
        # Check if a new file is uploaded and clear previous session state if true
        if 'uploaded_file_name' not in st.session_state or st.session_state.uploaded_file_name != uploaded_file.name:
            st.session_state.uploaded_file_name = uploaded_file.name
            st.session_state.excel_file = None
            st.session_state.df = None
            st.session_state.selected_sheet = None

        # Read and process the uploaded file if it's not already in session state
        if 'excel_file' not in st.session_state or st.session_state.excel_file is None:
            st.session_state.excel_file = read_excel_file(uploaded_file)
        if st.session_state.excel_file:
            selected_sheet = select_sheet(st.session_state.excel_file)
            # Check if the sheet is already loaded, if not, load and preprocess it
            if 'df' not in st.session_state or st.session_state.selected_sheet != selected_sheet:
                st.session_state.selected_sheet = selected_sheet
                st.session_state.df = read_sheet_to_dataframe(uploaded_file, selected_sheet)
                if st.session_state.df is not None:
                    st.session_state.df = preprocess_dataframe(st.session_state.df)
            if st.session_state.df is not None:
                # Filter the DataFrame by the selected month and apply business logic
                df_filtered, month = filter_dataframe_by_month(st.session_state.df)
                if df_filtered is not None:
                    apply_business_logic(df_filtered, selected_sheet, month)
    else:
        st.write("Please upload an Excel file to proceed.")

if __name__ == "__main__":
    main()
