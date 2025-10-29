
import glob
import linecache
from datetime import date, datetime, timedelta
import pyodbc
import os
import sys
import shutil
import configparser
import platform
import csv
from tqdm import tqdm
from utils import print_exception, openmsconnection, process_input_load_lead_contracts, load_interm_csv, process_input_load_trx_interm
from utils import process_paxticket_data, process_merchant_data, process_preferred_zone_data, generate_report
import calendar
import logging

# Create and configure logger
logging.basicConfig(filename="app.log",
                    format='%(asctime)s %(message)s',
                    filemode='a')


#
# This process will load a received CSV file from HSV that contacts the Contract Number and Lead Id number.
# Modifications
# 2025-10-17  SYNCRO : Initial creation


# Load Configuration Information from INI file
config = configparser.ConfigParser()
config.sections()
config.read('app.ini')

# handle the MS SQL Server Information
msserver = config['BIConfig']
ms_svr = msserver['ServerIP']
ms_drv = msserver['DBDriver']
ms_db = msserver['DBName']
ms_usr = msserver['DBUser']
ms_pwd = msserver['DBUserPwd']

ms_db_na = msserver['DBNameNA']
ms_db_drv = msserver['DBDriver']

# handle the MS SQL Server Information
msserver = config['MSConfig']
msp_svr = msserver['ServerIP']
msp_db_na = msserver['DBNameNA']
msp_db_drv = msserver['DBDriver']
msp_usr = msserver['DBUser']
msp_pwd = msserver['DBUserPwd']

# Settings
move_file = False


conn_writer = openmsconnection(ms_drv, ms_svr, ms_db, ms_usr, ms_pwd)

logging.info("Starting HSV Lead Contracts and CC Transactions Processing")

################################# Load Lead Contracts from HSV CSV File #################################
for in_file in glob.glob('In/*.csv'):

    # Rename the file to todays date (YYYY-MM-DD) + -HSV Lead Contracts
    file_index = 1
    today_date = date.today().strftime("%Y-%m-%d")
    new_file_name = 'In/' + today_date + '-HSV Lead Contracts (' + str(file_index) + ').csv'
    os.rename(in_file, new_file_name)
    in_file = new_file_name
    try:
        # Process the input file
        process_input_load_lead_contracts(in_file, conn_writer)
        if move_file:
            shutil.move(in_file, 'processed/')
    except Exception as e:
        print_exception(e, in_file)
        logging.error(f"Error processing Lead Contracts file: {in_file} - {str(e)}")

    file_index += 1

################################# Load Trx Interm #################################
for in_file in glob.glob('InTrx/*.csv'):
    # Rename the file to process 2025-10-14 HSV CC Transactions-OCTOBER
    file_index = 1
    today_date = datetime.today().strftime("%Y-%m-%d")
    month_name = calendar.month_name[datetime.today().month].upper()
    new_file_name = 'InTrx/' + today_date + '-HSV CC Transactions-' + month_name + ' (' + str(file_index) + ').csv'
    os.rename(in_file, new_file_name)
    in_file = new_file_name

    try:
        print(f"File : {in_file}")
        load_interm_csv(in_file, conn_writer)
        if move_file:
            shutil.move(in_file, 'processed/')

        #  Data connections and processing
        #conn_main = openmsconnection(ms_db_drv, '192.168.0.166', ms_db, ms_usr, ms_pwd)
        conn_main = openmsconnection(ms_drv, ms_svr, ms_db, ms_usr, ms_pwd)
    
        process_input_load_trx_interm(conn_main, conn_writer)   # load the new transactions into main table
    except Exception as e:
        print_exception(e, in_file)
        logging.error(f"Error processing Trx Interm file: {in_file} - {str(e)}")

################################## Match CC Transactions to Leads ##################################
month_name = calendar.month_name[datetime.today().month].upper()
runstyle = month_name
include_report = True

conn_main = openmsconnection(ms_db_drv, ms_svr, ms_db, ms_usr, ms_pwd)  # NA instance access
conn_secondary = openmsconnection(ms_db_drv, ms_svr, ms_db, ms_usr, ms_pwd)  # NA instance access
conn_worker = openmsconnection(msp_db_drv, msp_svr, msp_db_na, msp_usr, msp_pwd)  # NA instance access
conn_writer = openmsconnection(ms_db_drv, ms_svr, ms_db, ms_usr, ms_pwd)  # NA instance access

try:
    # Now process the data
    process_paxticket_data(runstyle, conn_main, conn_writer, conn_secondary, conn_worker)

    print ("Processing Merchant Data...")
    process_merchant_data(runstyle, conn_main, conn_writer, conn_secondary, conn_worker)

    print ("Processing Preferred Zone Data...")
    process_preferred_zone_data(runstyle, conn_main, conn_writer, conn_secondary, conn_worker)
except Exception as e:
    print_exception(e, "Main Processing")
    logging.error(f"Error in Main Processing: {str(e)}")

conn_main.close()
conn_secondary.close()
conn_worker.close()
conn_writer.close()

if include_report:
    conn_main = openmsconnection(ms_db_drv, ms_svr, ms_db, ms_usr, ms_pwd)  # NA instance access
    try:
        generate_report(runstyle, conn_main)
    except Exception as e:
        print_exception(e, "Generate Report")
        logging.error(f"Error generating report: {str(e)}")


################################### Close Connections ###################################
print("Processing Complete.")