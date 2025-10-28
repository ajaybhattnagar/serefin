
from datetime import datetime
import linecache
import configparser
import pyodbc
import os
import sys
import platform

# import serefin
from utils import openmsconnection, print_exception, process_paxticket_data, process_merchant_data, process_preferred_zone_data, generate_report


#
# This process will update the WRK_HSV_CC_TRANSACTION table with information identified in the PenAir backend.
#
# 1.  Workbook to be generated daily
# 2.  New TAB added to the existing monthly workbook to reprecent the previoues days activities
# 3.  Report is to be mailed to the stake holders

# NOTE:  DO NOTE PERFORM A REFORMAT OF FILE WITHIN PYCHARM AS IT WILL CHANGE THE CASE TESTING AND EFFECT
#        THE RESULTS OF THE REPORT (EFFECTS PEP   -1 )
#
# Modifications
# 2023-09-06  FSB  1: Initial creation
# 2023-09-18  FSB  1: Added product type to transaction details line
# 2023-09-21  FSB  1: Added agent name to transaction line
#                  2: Add supplier name to transaction details line
#                  3: Add Web Reference to the transaction details line
# 2023-10-19  FSB 1: Added partial lookup with travel date and last PAX name
# 2023-11-06  FSB 1: Changed select SQL from using the Date_Occurred to the Date_Posted
# 2023-11-15  FSB 1: Remove date selection range from base processing
#                 2: Remove the restriction of the no PAX name and ticket number
#                 3: Add extra processing based on the merchant name like TRAVEL 9012344567
#                    as the 2nd string is the Your Ref
# 2024-01-03  FSB 1: Removed FolderMaster Status restriction on all select statements
#                 2: Removed FolderMaster BAID on selects where full project no and folder no are part of parameters
# 2024-01-05  FSB 1: Added automatic workbook generation and email process
# 2024-01-11  FSB 1: Tweak SQL
# 2024-01-30  FSB 1: Changed code to use SQL Server instead of MySQL.
#                  : Chnages mage are to replace the %s to ?, now() to CURRENT_TIMESTAMP, LENGTH() to LEN()
# 2024-04-18  FSB 1: Added handling of PREFERRED ZONE transactions to identify lead's
#

# 2025-10-17  SYNCRO : Added utils file to handle common functions
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
ms_db = msserver['DBName']
ms_db_na = msserver['DBNameNA']
ms_db_drv = msserver['DBDriver']
ms_usr = msserver['DBUser']
ms_pwd = msserver['DBUserPwd']

#
# handle the MS SQL Server Information
msserver = config['MSConfig']
msp_svr = msserver['ServerIP']
msp_db_na = msserver['DBNameNA']
msp_db_drv = msserver['DBDriver']
msp_usr = msserver['DBUser']
msp_pwd = msserver['DBUserPwd']

# Main code
def main(argv):

    runstyle = 'OCTOBER'
    include_report = True

    # try:
        # opts, args = getopt.getopt(argv,"", ["run=", "report"])
    # except getopt.GetoptError:
        # print('match_cc_trx_backend.py --run <type_of_run> [--report]')
        # print_exception()
        # sys.exit(2)

    # for opt, arg in opts:
        # if opt == '-h':
            # print('match_cc_trx_backend.py --run <type_of_run>  [--report]')
            # sys.exit()
        # elif opt == '--report':
            # include_report = True
        # elif opt == '--run':
            # runstyle = arg

    conn_main = openmsconnection(ms_db_drv, ms_svr, ms_db, ms_usr, ms_pwd)  # NA instance access
    conn_secondary = openmsconnection(ms_db_drv, ms_svr, ms_db, ms_usr, ms_pwd)  # NA instance access
    conn_worker = openmsconnection(msp_db_drv, msp_svr, msp_db_na, msp_usr, msp_pwd)  # NA instance access
    conn_writer = openmsconnection(ms_db_drv, ms_svr, ms_db, ms_usr, ms_pwd)  # NA instance access

    # Now process the data
    process_paxticket_data(runstyle, conn_main, conn_writer, conn_secondary, conn_worker)
    process_merchant_data(runstyle, conn_main, conn_writer, conn_secondary, conn_worker)
    process_preferred_zone_data(runstyle, conn_main, conn_writer, conn_secondary, conn_worker)

    conn_main.close()
    conn_secondary.close()
    conn_worker.close()
    conn_writer.close()

    if include_report:
        conn_main = openmsconnection(ms_db_drv, ms_svr, ms_db, ms_usr, ms_pwd)  # NA instance access
        generate_report(runstyle, conn_main)


if __name__ == "__main__":
    main(sys.argv[1:])
