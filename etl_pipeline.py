# Import    Necessary Libraries
import pandas as pd
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io
from dotenv import load_dotenv

# Extraction Layer
ziko_df = pd.read_csv('ziko_logistics_data.csv')
ziko_df.head()

# Data Cleaning and Transformation Layer
ziko_df.fillna({
    'Unit_Price': ziko_df['Unit_Price'].mean(),
    'Total_Cost': ziko_df['Total_Cost'].mean(),
    'Discount_Rate': 0.0,
    'Return_Reason': 'Unknown'
    }, inplace=True)

ziko_df['Date'] = pd.to_datetime(ziko_df['Date'])

# Customer Table
customer = ziko_df[['Customer_ID', 'Customer_Name', 'Customer_Email', 'Customer_Phone', 'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)


# Product Table
product = ziko_df[['Product_ID', 'Quantity', 'Product_List_Title', 'Unit_Price', 'Discount_Rate']].copy().drop_duplicates().reset_index(drop=True)


# Transaction_Fact_Table
transaction_fact =ziko_df.merge(
    customer, on=['Customer_ID', 'Customer_Name', 'Customer_Email', 'Customer_Phone', 'Customer_Address'], how='left')\
    .merge(product, on=['Product_ID', 'Quantity', 'Product_List_Title', 'Unit_Price', 'Discount_Rate'], how='left')\
    [['Transaction_ID', 'Date','Total_Cost', 'Discount_Rate', 'Sales_Channel','Order_Priority', 'Warehouse_Code', \
    'Ship_Mode', 'Delivery_Status','Customer_Satisfaction', 'Item_Returned', 'Return_Reason','Payment_Type', \
    'Taxable', 'Region', 'Country', ]]


# Tempporary loading
customer.to_csv(r'dataset/customer.csv', index=False)
product.to_csv(r'dataset/product.csv', index=False)
transaction_fact.to_csv(r'dataset/transaction_fact.csv', index=False)

print('files have been loaded successfully into local machine')

# Data Loading 
# Azure Blob Storage Configuration
load_dotenv()  # Load environment variables from .env file
connect_str = os.getenv('CONNECT_STR')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
container_name = os.getenv('CONTAINER_NAME') 
container_client = blob_service_client.get_container_client(container_name)


# create a function to upload a DataFrame to Azure Blob Storage as a parquet file

def upload_df_to_blob_as_parquet(df, container_name, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)  # Move to the beginning of the BytesIO buffer
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f"df uploaded successfully to Azure Blob Storage as {blob_name} in container {container_name}.")
    

upload_df_to_blob_as_parquet(customer, container_client, 'rawdata/customer.parquet')
upload_df_to_blob_as_parquet(product, container_client, 'rawdata/product.parquet')
upload_df_to_blob_as_parquet(transaction_fact, container_client, 'rawdata/transaction_fact.parquet')
    
    
    