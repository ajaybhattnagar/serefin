
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
from utils import print_exception, openmsconnection, process_input_load_lead_contracts

#
# This process will load a received CSV file from HSV that contacts the Contract Number and Lead Id number.
#
#
# NOTE:  DO NOTE PERFORM A REFORMAT OF FILE WITHIN PYCHARM AS IT WILL CHANGE THE CASE TESTING AND EFFECT
#        THE RESULTS OF THE REPORT (EFFECTS PEP-8 )
#
# Modifications
# 2023-09-05  FSB 1: Initial creation
# 2023-11-04  FSB 1: Added new attributes of First/Last name of Lead Id
# 2023-11-08  FSB 1: Added truncate to reset the lead/contract table
# 2024-01-30  FSB 1: Changed code to use SQL Server instead of MySQL.
#                  : Chnages mage are to replace the %s to ?, now() to CURRENT_TIMESTAMP, LENGTH() to LEN()
#
# 2025-10-17  SYNCRO : Added utils file to handle common functions

#
# Load Configuration Information from INI file
config = configparser.ConfigParser()
config.sections()
if platform.release() == '10':
    config.read('hsv_config.ini')
else:
    config.read(os.path.basename(sys.argv[0]).replace('.py', '.ini'))

#
# handle the MS SQL Server Information
msserver = config['BIConfig']
ms_svr = msserver['ServerIP']
ms_drv = msserver['DBDriver']
ms_db = msserver['DBName']
ms_usr = msserver['DBUser']
ms_pwd = msserver['DBUserPwd']


def main():
    #  Data connections and processing
    conn_writer = openmsconnection(ms_drv, ms_svr, ms_db, ms_usr, ms_pwd)

    for in_file in glob.glob('In/*.csv'):

        # Rename the file to todays date (YYYY-MM-DD) + -HSV Lead Contracts
        file_index = 1
        today_date = date.today().strftime("%Y-%m-%d")
        new_file_name = 'In/' + today_date + '-HSV Lead Contracts (' + str(file_index) + ').csv'
        os.rename(in_file, new_file_name)
        in_file = new_file_name

        # Process the input file
        process_input_load_lead_contracts(in_file, conn_writer)
        shutil.move(in_file, 'processed/')

        file_index += 1


if __name__ == "__main__":
    main()
