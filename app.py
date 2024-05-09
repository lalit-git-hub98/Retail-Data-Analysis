import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import streamlit as st
import mpld3
from mpld3 import plugins
import streamlit.components.v1 as components

from pymongo import MongoClient
import hashlib



# schema = {
#     "$jsonSchema": {
#         "bsonType": "object",
#         "required": ["username", "email", "password"],
#         "properties": {
#             "name": {
#                 "bsonType": "string",
#                 "description": "must be a string and is required"
#             },
#             "email": {
#                 "bsonType": "string",
#                 "pattern": "^[a-zA-Z0-9+_.-]+@[a-zA-Z0-9.-]+$",
#                 "description": "must be a valid email address and is required"
#             },
#             "password": {
#                 "bsonType": "string",
#                 "description": "must be a string and is required"
#             }
#         }
#     }
# }

# Access your collection
# collection_name = "user_data2"
# db.create_collection(collection_name, validator=schema)


#print(client.server_info())
###############################
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def register_user(username, email, password):
    hashed_password = hash_password(password)
    user = collection.find_one({"username": username, "email": email, "password": hashed_password})
    if user:
        message = 'User already exists, please log in'
        return message, False
    else:
        collection.insert_one({"username": username, "email": email, "password": hashed_password})
        message = 'Sign Up Successful, Please Log In'
        return message, True

def login_user(username, email, password):
    hashed_password = hash_password(password)
    user = collection.find_one({"username": username, "email": email, "password": hashed_password})
    if user:
        return username, email, True
    else:
        return False
    

def log_out():
    st.session_state.logged_in = False
    st.session_state.username = None
    
    


if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

if not st.session_state.logged_in:
    connection_string = "mongodb+srv://lalitwale2006:lXAVxn3sW24fhzXv@cluster0.ynu1zhe.mongodb.net/"
    client = MongoClient(connection_string)

    db = client["user_authentication_table2"]

    collection = db["user_data2"]

    st.subheader('Sign Up')
    new_username = st.text_input("Username", key = '1')
    new_email = st.text_input("Email", key = '2')
    new_password = st.text_input("Password", type="password", key = '3')
    if st.button("Sign Up"):
        message, success = register_user(new_username, new_email, new_password)
        if success:
            st.success(message)
        else:
            st.error(message)

    st.subheader('Log In')
    old_username = st.text_input("Username", key = '4')
    old_email = st.text_input("Email", key = '5')
    old_password = st.text_input("Password", type="password", key = '6')
    if st.button('Log In'):
        given_username, given_email, success = login_user(old_username, old_email, old_password)
        if success:
            st.session_state.logged_in = True
            st.session_state.username = given_username
            st.session_state.email = given_email
            st.success('Welcome')
            st.rerun()
        else:
            st.error('Invalid Credentials')

else:
    st.set_page_config(page_title = 'Retail Data Analysis')

    st.markdown("<h1 style='text-align:center; color:black;'>Retail Data Analysis</h1>", unsafe_allow_html = True)
    log_col1, log_col2 = st.columns([0.7, 0.3])
    with log_col1:
        st.write(f'Welcome {st.session_state.username}, your email is {st.session_state.email}')
    with log_col2:
        log_log_col1, log_log_col2 = st.columns(2)
        with log_log_col1:
            st.write(' ')
        with log_log_col2:
            if st.button('Log Out'):
                log_out()
                st.rerun()

            
        

########################################### Application ###################################################

    if 'df_house' not in st.session_state:
        st.session_state.df_house = None

    if 'df_product' not in st.session_state:
        st.session_state.df_product = None

    if 'df_transaction' not in st.session_state:
        st.session_state.df_transaction = None

    if 'df_transaction_house' not in st.session_state:
        st.session_state.df_transaction_house = None

    if 'df_thp' not in st.session_state:
        st.session_state.df_thp = None

    if 'df_thp_renamed' not in st.session_state:
        st.session_state.df_thp_renamed = None

    if 'df_thp_reorder' not in st.session_state:
        st.session_state.df_thp_reorder = None
    
    if 'df_thp_renamed_dropped' not in st.session_state:
        st.session_state.df_thp_renamed_dropped = None

    if 'grouped' not in st.session_state:
        st.session_state.grouped = None

    if 'product_data_upload_check' not in st.session_state:
        st.session_state.product_data_upload_check = False
    if 'transaction_data_upload_check' not in st.session_state:
        st.session_state.transaction_data_upload_check = False
    if 'house_data_upload_check' not in st.session_state:
        st.session_state.house_data_upload_check = False

    if 'data_uploaded_check' not in st.session_state:
        st.session_state.data_uploaded_check = False

        #############################################################################################################
    if st.session_state.data_uploaded_check:
        pass
    else:
        st.session_state.df_house = pd.read_csv('400_households.csv')
        st.session_state.df_product = pd.read_csv('400_products.csv')
        st.session_state.df_transaction = pd.read_csv('400_transactions.csv')

        st.session_state.df_transaction_house = pd.merge(st.session_state.df_transaction, st.session_state.df_house, on='HSHD_NUM')
        st.session_state.df_thp = pd.merge(st.session_state.df_transaction_house, st.session_state.df_product, on='PRODUCT_NUM')

        st.session_state.df_thp.drop(['BRAND_TY', 'NATURAL_ORGANIC_FLAG'], axis = 1)

        st.session_state.df_thp_reorder = st.session_state.df_thp[['HSHD_NUM', 'BASKET_NUM', 'PURCHASE_', 'PRODUCT_NUM', 'DEPARTMENT', 'COMMODITY', 'SPEND', 'UNITS',
                                'STORE_R', 'WEEK_NUM', 'YEAR', 'L', 'AGE_RANGE', 'MARITAL', 'INCOME_RANGE', 'HOMEOWNER', 'HSHD_COMPOSITION ', 'HH_SIZE', 'CHILDREN']]

        new_names = {'PURCHASE_': 'Date',
                    'STORE_R': 'Store_Region',
                    'L': 'Loyalty_flag',
                    'MARITAL': 'Marital_Status',
                    'HOMEOWNER': 'HOMEOWNER_DESC',
                    'HH_SIZE': 'HSHD_SIZE'}

        st.session_state.df_thp_renamed = st.session_state.df_thp_reorder.rename(columns=new_names)

        st.session_state.df_thp_renamed['Date'] = pd.to_datetime(st.session_state.df_thp_renamed['Date'])
        st.session_state.df_thp_renamed['Date'] = st.session_state.df_thp_renamed['Date'].dt.strftime('%m/%d/%Y')

        #st.write(df_thp_renamed.head())

    ################################################# Dataframe by Selection ######################################################

    column_selection = ['HSHD_NUM', 'BASKET_NUM', 'Date', 'PRODUCT_NUM', 'DEPARTMENT']

    selected_column = st.selectbox('**Select Column to Sort**', column_selection)

    if selected_column == 'HSHD_NUM':
        #column_unique_values = st.session_state.df_thp_renamed[str(selected_column)].unique()
        selected_value = st.text_input('Enter Household Number')
    elif selected_column == 'BASKET_NUM':
        #column_unique_values = st.session_state.df_thp_renamed[str(selected_column)].unique()
        selected_value = st.text_input('Enter Basket Number')
    elif selected_column == 'Date':
        #column_unique_values = st.session_state.df_thp_renamed[str(selected_column)].unique()
        selected_value = st.text_input('**Enter Date in format="MM/DD/YYYY"**')
        #selected_value = str(selected_value)
    elif selected_column == 'PRODUCT_NUM':
        #column_unique_values = st.session_state.df_thp_renamed[str(selected_column)].unique()
        selected_value = st.text_input('Enter Product Number')
    elif selected_column == 'DEPARTMENT':
        column_unique_values = st.session_state.df_thp_renamed[str(selected_column)].unique()
        selected_value = st.selectbox('**Select Department**', column_unique_values)

    # column_unique_values = st.session_state.df_thp_renamed[str(selected_column)].unique()



    if st.button('Display'):
        str_id = st.session_state.df_thp_renamed[str(selected_column)].astype(str)
        if len(st.session_state.df_thp_renamed[str_id == selected_value]) == 0:
            st.write('Please enter valid ', selected_column)
        else:
            st.write(st.session_state.df_thp_renamed[str_id == selected_value])
        

    ########################################################################################################################

    ############################################# Plot Line Chart ##########################################################################

    st.header('Cutomer Engagement Analysis')

    demographic_column = ['Marital_Status', 'HSHD_SIZE', 'CHILDREN', 'INCOME_RANGE']

    selected_demographic_column = st.selectbox('**Select Metric for Customer Engagement**', demographic_column)

    no_bins = st.number_input('Enter Number of Bins', min_value = 3, step = 1)
    
    if st.button('Plot'):
        #df_hshd_married = st.session_state.df_thp_renamed[['HSHD_NUM', selected_demographic_column]]
        st.session_state.df_thp_renamed_dropped = st.session_state.df_thp_renamed.dropna()
        st.session_state.grouped = st.session_state.df_thp_renamed_dropped.groupby(selected_demographic_column)['HSHD_NUM'].value_counts()

        bins = np.linspace(st.session_state.grouped.values.min(), st.session_state.grouped.values.max(), no_bins)

        ranges = []
        for i in range(no_bins - 1):
            ranges.append([bins[i], bins[i+1]])

        line_values_x = []
        line_values_y = []
        mari_stat = []

        u_val = st.session_state.df_thp_renamed_dropped[selected_demographic_column].unique()

        for i in range(len(u_val)):  
            for j in range(len(ranges)):
                count_in_range = ((st.session_state.grouped[u_val[i]] >= ranges[j][0]) & (st.session_state.grouped[u_val[i]] < ranges[j][1])).sum()
                line_values_x.append(ranges[j][1])
                line_values_y.append(count_in_range)
                mari_stat.append(u_val[i].strip())

        data = {
            'x': line_values_x,
            'y': line_values_y,
            'label': mari_stat
        }

        df = pd.DataFrame(data)

        grouped = df.groupby('label')

        fig, ax = plt.subplots(figsize=(10, 6))


        for label, group in grouped:
            ax.plot(group['x'], group['y'], marker='o', label=label)

        # Add labels and title
        ax.set_xlabel('Customer Engagement')
        ax.set_ylabel('Number of Households')
        ax.set_title('Line Chart with Labels')
        ax.legend(title='Label')

        css = """
        table
        {
        border-collapse: collapse;
        }
        th
        {
        color: #ffffff;
        background-color: #000000;
        }
        td
        {
        background-color: #cccccc;
        }
        table, th, td
        {
        font-family:Arial, Helvetica, sans-serif;
        border: 1px solid black;
        text-align: right;
        }
        """

        for axes in fig.axes:
            for line in axes.get_lines():
                # get the x and y coords
                xy_data = line.get_xydata()
                labels = []
                for x, y in xy_data:
                    # Create a label for each point with the x and y coords
                    html_label = f'<table border="1" class="dataframe"> <thead> <tr style="text-align: right;"> </thead> <tbody> <tr> <th>x</th> <td>{x}</td> </tr> <tr> <th>y</th> <td>{y}</td> </tr> </tbody> </table>'
                    labels.append(html_label)
                # Create the tooltip with the labels (x and y coords) and attach it to each line with the css specified
                tooltip = plugins.PointHTMLTooltip(line, labels, css=css)
                # Since this is a separate plugin, you have to connect it
                plugins.connect(fig, tooltip)

        #st.pyplot(fig)
        fig_html = mpld3.fig_to_html(fig)
        components.html(fig_html, height=600, width=1000)

    st.header('Upload New Data')
    data_types = ['Product Data', 'Transaction Data', 'Household Data']
    selected_data_types = st.selectbox('Select Data to Upload', data_types)
    

    if 'temp_prod_data_up' not in st.session_state:
        st.session_state.temp_prod_data_up = None
    if 'temp_transaction_data_up' not in st.session_state:
        st.session_state.temp_transaction_data_up = None
    if 'temp_house_data_up' not in st.session_state:
        st.session_state.temp_house_data_up = None
    ########################## Product Data #################################
    if selected_data_types == 'Product Data':
        data_upload = st.file_uploader('Upload Product Data', type = ['csv'])
        if data_upload is not None:
            st.session_state.temp_prod_data_up = pd.read_csv(data_upload)
            up_cols = st.session_state.temp_prod_data_up.columns
            up_cols_strip = []
            for m in range(len(up_cols)):
                up_cols_strip.append(up_cols[m].strip())
            product_cols = []
            for o in range(len(st.session_state.df_product.columns)):
                product_cols.append(st.session_state.df_product.columns[o].strip())
            if up_cols_strip == product_cols:
                st.success('File Uploaded Successfully!')
                st.session_state.product_data_upload_check = True
            else:
                st.error('Please Upload the Correct Product Data File')
        else:
           st.session_state.product_data_upload_check = False
    ########################### Transaction Data ##################################### 
    elif selected_data_types == 'Transaction Data':
        data_upload = st.file_uploader('Upload Transaction Data', type = ['csv'])
        if data_upload is not None:
            st.session_state.temp_transaction_data_up = pd.read_csv(data_upload)
            up_cols = st.session_state.temp_transaction_data_up.columns
            up_cols_strip = []
            for m in range(len(up_cols)):
                up_cols_strip.append(up_cols[m].strip())
            transactions_cols = []
            for o in range(len(st.session_state.df_transaction.columns)):
                transactions_cols.append(st.session_state.df_transaction.columns[o].strip())
            if up_cols_strip == transactions_cols:
                st.success('File Upload Successfully!')
                st.session_state.transaction_data_upload_check = True
            else:
                st.error('Please Upload the Correct Transaction Data File')
        else:
            st.session_state.transaction_data_upload_check = False
    ################################ Household Data ####################################
    elif selected_data_types == 'Household Data':
        data_upload = st.file_uploader('Upload Household Data', type = ['csv'])
        if data_upload is not None:
            st.session_state.temp_house_data_up = pd.read_csv(data_upload)
            up_cols = st.session_state.temp_house_data_up.columns
            up_cols_strip = []
            for m in range(len(up_cols)):
                up_cols_strip.append(up_cols[m].strip())
            house_cols = []
            for o in range(len(st.session_state.df_house.columns)):
                house_cols.append(st.session_state.df_house.columns[o].strip())
            if up_cols_strip == house_cols:
                st.success('File Upload Successfully!')
                st.session_state.house_data_upload_check = True
            else:
                st.error('Please Upload the Correct Household Data File')
        else:
            st.session_state.house_data_upload_check = False

    if st.session_state.product_data_upload_check and st.session_state.transaction_data_upload_check and st.session_state.house_data_upload_check:
        ########### Merging Product Data ###############
        st.session_state.df_product = pd.concat([st.session_state.df_product, st.session_state.temp_prod_data_up])
        ########### Merging Transaction Data ###############
        st.session_state.df_transaction = pd.concat([st.session_state.df_transaction, st.session_state.temp_transaction_data_up])
        ########### Merging Household Data ###############
        st.session_state.df_house = pd.concat([st.session_state.df_house, st.session_state.temp_house_data_up])
        ############ Merging All Data ########################
        st.session_state.df_transaction_house = pd.merge(st.session_state.df_transaction, st.session_state.df_house, on='HSHD_NUM')
        st.session_state.df_thp = pd.merge(st.session_state.df_transaction_house, st.session_state.df_product, on='PRODUCT_NUM')

        st.session_state.df_thp.drop(['BRAND_TY', 'NATURAL_ORGANIC_FLAG'], axis = 1)

        st.session_state.df_thp_reorder = st.session_state.df_thp[['HSHD_NUM', 'BASKET_NUM', 'PURCHASE_', 'PRODUCT_NUM', 'DEPARTMENT', 'COMMODITY', 'SPEND', 'UNITS',
                                'STORE_R', 'WEEK_NUM', 'YEAR', 'L', 'AGE_RANGE', 'MARITAL', 'INCOME_RANGE', 'HOMEOWNER', 'HSHD_COMPOSITION ', 'HH_SIZE', 'CHILDREN']]

        new_names = {'PURCHASE_': 'Date',
                    'STORE_R': 'Store_Region',
                    'L': 'Loyalty_flag',
                    'MARITAL': 'Marital_Status',
                    'HOMEOWNER': 'HOMEOWNER_DESC',
                    'HH_SIZE': 'HSHD_SIZE'}

        st.session_state.df_thp_renamed = st.session_state.df_thp_reorder.rename(columns=new_names)

        st.session_state.df_thp_renamed['Date'] = pd.to_datetime(st.session_state.df_thp_renamed['Date'])
        st.session_state.df_thp_renamed['Date'] = st.session_state.df_thp_renamed['Date'].dt.strftime('%m/%d/%Y')
        st.success('Data Merged Successfully!!!')
        st.session_state.data_uploaded_check = True
        st.session_state.product_data_upload_check = False
        st.session_state.transaction_data_upload_check = False
        st.session_state.house_data_upload_check = False

        st.session_state.temp_prod_data_up = None
        st.session_state.temp_transaction_data_up = None
        st.session_state.temp_house_data_up = None
    else:
        files_track = []
        if not st.session_state.product_data_upload_check:
            files_track.append('Product Data')
        if not st.session_state.transaction_data_upload_check:
            files_track.append('Transaction Data')
        if not st.session_state.house_data_upload_check:
            files_track.append('House Data')
        st.warning('Please upload all the files if you want to add new data. Files not uploaded: ', files_track)



