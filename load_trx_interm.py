
import glob
import linecache
import pyodbc
import os
import sys
import shutil
import configparser
import platform
import csv
from datetime import date, datetime, timedelta
import calendar

from utils import print_exception, load_interm_csv, process_input_load_trx_interm, openmsconnection

#
# This process handles the HSV CC transactions CSV file that is received on Tuesday/Friday weekly.  The process first
# loads the interm CSV into a table and then loads all the new records into the main Cc Transaction table for processing.
#
#
# NOTE:  DO NOTE PERFORM A REFORMAT OF FILE WITHIN PYCHARM AS IT WILL CHANGE THE CASE TESTING AND EFFECT
#        THE RESULTS OF THE REPORT (EFFECTS PEP-8 )
#
# Modifications
# 2023-10-16  FSB 1: Initial creation
# 2023-11-08  FSB 1: Merge the CSV loading
# 2024-01-09  FSB 1: Moved unused code, and reformat def seperations etc
# 2024-01-12  FSB 1: Added default value for projno on insert to main table
# 2024-01-16  FSB 1: Code Clean up
# 2024-01-30  FSB 1: Changed code to use SQL Server instead of MySQL.
#                  : Chnages mage are to replace the %s to ?, now() to CURRENT_TIMESTAMP, LENGTH() to LEN()
#
# 2025-10-17  SYNCRO : Added utils file to handle common functions
#


# Load Configuration Information from INI file
config = configparser.ConfigParser()
config.sections()
if platform.release() == '10':
    config.read('/dev/testing_config.ini')
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
    # conn_writer = openmsconnection(ms_db_drv, '192.168.0.166', ms_db, ms_usr, ms_pwd)
    conn_writer = openmsconnection(ms_drv, ms_svr, ms_db, ms_usr, ms_pwd)

    for in_file in glob.glob('InTrx/*.csv'):

        # Rename the file to process 2025-10-14 HSV CC Transactions-OCTOBER
        file_index = 1
        today_date = datetime.today().strftime("%Y-%m-%d")
        month_name = calendar.month_name[datetime.today().month].upper()
        new_file_name = 'InTrx/' + today_date + '-HSV CC Transactions-' + month_name + ' (' + str(file_index) + ').csv'
        os.rename(in_file, new_file_name)
        in_file = new_file_name

        print(f"File : {in_file}")
        load_interm_csv(in_file, conn_writer)
        shutil.move(in_file, 'processed/')

    #  Data connections and processing
    #conn_main = openmsconnection(ms_db_drv, '192.168.0.166', ms_db, ms_usr, ms_pwd)
    conn_main = openmsconnection(ms_drv, ms_svr, ms_db, ms_usr, ms_pwd)
 
    process_input_load_trx_interm(conn_main, conn_writer)   # load the new transactions into main table


if __name__ == "__main__":
    main()
