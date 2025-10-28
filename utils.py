
import glob
import linecache
import pyodbc
import os
import sys
import shutil
import configparser
import platform
import csv
from tqdm import tqdm
from datetime import date, datetime, timedelta

def print_exception():
    """This method is use to trace and print exception"""
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    line_no = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, line_no, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, line_no, line.strip(), exc_obj))

def openmsconnection(drv, server, db, user, pwd):
    """Open database connection to Microsoft SQL Server"""
    try:
        conn = pyodbc.connect('DRIVER=' + drv + ';SERVER=' + server + ';PORT=1433;DATABASE=' + db + ';UID=' + user + ';PWD=' + pwd)
    except Exception as e:
        print_exception()
        sys.exit(1)

    return conn



######################################################### load lead contract functions ########################################################
def process_input_load_lead_contracts(in_file, conn_writer):
    # Collect the master list of bookings that are leaving

    cursor_writer = conn_writer.cursor()  # MySQL Write Only
    # cursor_writer.execute('SET autocommit = 0')

    # reset the lead/contract table for new data insert
    cursor_writer.execute('TRUNCATE table wrk_hsv_lead_contract;')

    ins_sql = """insert into wrk_hsv_lead_contract(contract_number, LeadID, lastname, 
    firstname, date_created, exp_date, travel_date) values(?, ?, ?, ?, ?, ?, ?);"""

    with open(in_file, newline='') as csvfile:
        in_csv = csv.reader(csvfile, delimiter=',')
        line_count = 0

        total_lines = sum(1 for _ in csvfile) - 1  # minus header line
        csvfile.seek(0)  # reset file read position
        pbar = tqdm(total=total_lines, desc="Processing rows", unit="row", ncols=80)

        for row in in_csv:
            if len(row) < 5:
                return
            if line_count == 0:
                line_count += 1
            else:
                #
                # process the rest of the data
                date_element_1 = row[4]
                if date_element_1 == '':
                    date_element_1 = '01/01/1900'
                date_element_2 = row[5]
                if date_element_2 == '':
                    date_element_2 = '01/01/1900'
                date_element_3 = row[6]
                if date_element_3 == '':
                    date_element_3 = '01/01/1900'
                create_date = datetime.strptime(date_element_1, '%m/%d/%Y')
                exp_date = datetime.strptime(date_element_2, '%m/%d/%Y')
                travel_date = datetime.strptime(date_element_3, '%m/%d/%Y')
                ins_data = None
                ins_data = (row[0], row[1], row[2], row[3], create_date, exp_date, travel_date)
                try:
                    cursor_writer.execute(ins_sql, ins_data)
                except Exception as e:
                    print(f"Problem details : {row}")
                    print_exception()

                conn_writer.commit()
                pbar.update(1)
    pbar.close()



######################################################## load trx_interm.py functions ########################################################
def load_interm_csv(in_file, conn_writer):
    """Collect the master list of bookings that are leaving"""

    cursor_writer = conn_writer.cursor()  # MySQL Write Only
    # cursor_writer.execute('SET autocommit = 0')

    # reset the interm tbale to hold the new information
    cursor_writer.execute('truncate table wrk_hsv_cc_transactions_interm;')

    ins_sql = """insert into wrk_hsv_cc_transactions_interm( Transaction_ID, Record_Type, Merchant, Merchant_Location,
     Account_Number, MCC, Date_Occurred, Date_Posted, Original_Amount, Original_Currency_Code, Conversion_Rate,
     Settlement_Amount, Allocation, Transaction_Description, Reference_Number, Purch_Description, Purch_Quantity,
     Purch_Unit_Cost, Purch_Unit_Of_Measure, Purch_Extended_Amount, Passenger_Name, Ticket_Number,
     Travel_Date, Travel_Legs, import_date, period, insert_date) 
     values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?,?, CURRENT_TIMESTAMP);"""

    # Get the full month name for period processing from the file name
    period = in_file.split('-')[-1].split('.')[0]
    with open(in_file, newline='') as csvfile:
        in_csv = csv.reader(csvfile, delimiter=',')
        line_count = 0
        
        total_lines = sum(1 for _ in csvfile) - 1  # minus header line
        csvfile.seek(0)  # reset file read position
        pbar = tqdm(total=total_lines, desc="Processing rows", unit="row", ncols=80)


        for row in in_csv:
            if row:
                if line_count == 0:
                    line_count += 1
                else:
                    #
                    # process the rest of the data
                    ins_data = None
                    try:
                        ins_data = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                                    row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18],
                                    row[19], row[20], row[21], row[22], row[23], row[24], period
                                    )
                    except Exception as e:
                        print(f"Problem with ID: {row[0]}")
                        print_exception()
                    try:
                        cursor_writer.execute(ins_sql, ins_data)
                    except Exception as e:
                        print(f"Details : {row}")
                        print_exception()
                    
                    conn_writer.commit()
                    pbar.update(1)
        pbar.close()

def process_input_load_trx_interm(conn_main, conn_writer):
    """process all new transaction records that do not exist in the main transacction tables"""

    cursor_main = conn_main.cursor()  # MySQL Write Only
    cursor_writer = conn_writer.cursor()  # MySQL Write Only
    # cursor_writer.execute('SET autocommit = 0')

    sel_sql = """select Transaction_ID, Record_Type, Merchant, Merchant_Location,
    Account_Number, MCC, Date_Occurred, Date_Posted, Original_Amount, Original_Currency_Code, Conversion_Rate,
    Settlement_Amount, Allocation, Transaction_Description, Reference_Number, Purch_Description, Purch_Quantity,
    Purch_Unit_Cost, Purch_Unit_Of_Measure, Purch_Extended_Amount, Passenger_Name, Ticket_Number, Travel_Date, 
    Travel_Legs, import_date, period from wrk_hsv_cc_transactions_interm 
    where Transaction_ID not in (select Transaction_ID from wrk_hsv_cc_transactions);"""

    ins_sql = """insert into wrk_hsv_cc_transactions( Transaction_ID, Record_Type, Merchant, Merchant_Location,
    Account_Number, MCC, Date_Occurred, Date_Posted, Original_Amount, Original_Currency_Code, Conversion_Rate,
    Settlement_Amount, Allocation, Transaction_Description, Reference_Number, Purch_Description, Purch_Quantity,
    Purch_Unit_Cost, Purch_Unit_Of_Measure, Purch_Extended_Amount, Passenger_Name, Ticket_Number, Travel_Date, 
    Travel_Legs, import_date, period, insert_date, projno) 
    values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, '') ; """

    cursor_main.execute(sel_sql)
    row = cursor_main.fetchone()

    while row:
        #
        # process the rest of the data
        ins_data = None
        print(f"Insert Trx : {row[0]}")
        try:
            ins_data = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                        row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19],
                        row[20], row[21], row[22], row[23], row[24], row[25]
                        )
        except Exception as e:
            print(f"Problem with ID: {row[0]}")
            print_exception()
        try:
            cursor_writer.execute(ins_sql, ins_data)
        except Exception as e:
            print(f"Details : {row}")
            print_exception()

        row = cursor_main.fetchone()  # Process next available booking
        conn_writer.commit()

    # print(f"Updating Null Lead Id's\r\n")
    # cursor_writer.execute('update wrk_hsv_cc_transactions set leadid='' where leadid is null; ')
    # conn_writer.commit()
