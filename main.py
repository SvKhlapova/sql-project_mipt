import sqlite3
import pandas as pd
import datetime
import os
conn = sqlite3.connect('BANK.db')
cursor = conn.cursor()

# загурзка в базу данных из sql-файла
def load_SQL(path):
    with open(path, 'r', encoding='utf-8') as file:
        script = file.read()
        cursor.executescript(script)  
    conn.commit()


# загурзка в базу данных из xlsx-файлов
def load_excel(path, tableName):
    pass_file = pd.ExcelFile(path)
    
    for sheet in pass_file.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet)
        df.to_sql(tableName, conn, index=False, if_exists="replace")
        print(df)
    pass_file.close()
    conn.commit()


# загурзка в базу данных из txt-файла
def load_txt(path):
    read_trans = pd.read_csv(path)
    df = pd.read_csv(path, sep=";", encoding= 'utf-8')
    df.to_sql('STG_transactions', conn, index=False, if_exists="replace")
    conn.commit()

# создание и заполнение таблиц измерений
def DWH_DIM_tables(con):
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_cards(
            id integer primary key autoincrement,
            card_num varchar(128), 
            account_num varchar(128), 
            create_dt date,
            update_dt date
        )
    ''')
    conn.commit()

    cursor.execute('''
        INSERT INTO DWH_DIM_cards(
            card_num, 
            account_num, 
            create_dt, 
            update_dt
        ) SELECT
            tc1.card_num, 
            tc1.account, 
            tc1.create_dt, 
            tc1.update_dt 
        FROM cards tc1
        LEFT JOIN DWH_DIM_cards tc2
        on tc1.card_num = tc2.card_num
        where tc2.card_num is null
    ''')
    conn.commit()

    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_accounts(
            id integer primary key autoincrement,
            account_num varchar(128), 
            valid_to date, 
            client varchar(128),
            create_dt date, 
            update_dt date
        )
    ''')
    conn.commit()

    cursor.execute('''
        INSERT INTO DWH_DIM_accounts(
            account_num, 
            valid_to, 
            client, 
            create_dt, 
            update_dt
        ) SELECT
            ta1.account, 
            ta1.valid_to, 
            ta1.client, 
            ta1.create_dt, 
            ta1.update_dt 
        FROM accounts ta1
        LEFT JOIN DWH_DIM_accounts ta2
        on ta1.account = ta2.account_num
        where ta2.account_num is null
    ''')
    conn.commit()

    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_clients(
            id integer primary key autoincrement,
            client_id varchar(128), 
            last_name varchar(128), 
            first_name varchar(128), 
            patronymic varchar(128), 
            date_of_birth date, 
            passport_num varchar(128), 
            passport_valid_to date, 
            phone varchar(128),
            create_dt date, 
            update_dt date
        )
    ''')
    conn.commit()

    cursor.execute('''
        INSERT INTO DWH_DIM_clients(
            client_id, 
            last_name, 
            first_name, 
            patronymic, 
            date_of_birth,
            passport_num, 
            passport_valid_to, 
            phone, 
            create_dt, 
            update_dt
        ) SELECT
            tcl1.client_id, 
            tcl1.last_name, 
            tcl1.first_name, 
            tcl1.patronymic, 
            tcl1.date_of_birth,
            tcl1.passport_num, 
            tcl1.passport_valid_to, 
            tcl1.phone, 
            tcl1.create_dt,
            tcl1.update_dt 
        FROM clients tcl1
        LEFT JOIN DWH_DIM_clients tcl2
        ON tcl1.client_id = tcl2.client_id
        where tcl2.client_id is null       
    ''')
    conn.commit()

    cursor.execute('drop table if exists cards')
    cursor.execute('drop table if exists accounts')
    cursor.execute('drop table if exists clients')
    conn.commit()


# создание и заполнение таблиц фактов: transactions
cursor.execute('drop table if exists STG_transactions')

def transactions(path, date):
    read_trans = pd.read_csv(path)
    df = pd.read_csv(path, sep=";", encoding= 'utf-8')
    df.to_sql('STG_transactions', conn, index=False, if_exists="replace")
    
    cursor.execute('''
        CREATE TABLE if not exists DWH_Fact_transactions(
            trans_id varchar(128),
            trans_date date,
            amt decimal(10,2),
            card_num varchar(128),
            oper_type varchar(128),
            oper_result varchar(128),
            terminal varchar(128),
            create_dt date, 
            update_dt date
        )
    ''')
    conn.commit()

    cursor.execute('''
        INSERT INTO DWH_Fact_transactions(
            trans_id, 
            trans_date, 
            amt, 
            card_num, 
            oper_type, 
            oper_result,
            terminal
        ) SELECT
           t1.transaction_id, 
           t1.transaction_date, 
           t1.amount, 
           t1.card_num, 
           t1.oper_type,
           t1.oper_result, 
           t1.terminal 
        FROM STG_transactions t1
        LEFT JOIN DWH_Fact_transactions t2
        on t1.transaction_id = t2.trans_id
        where t2.trans_id is null
    ''')
    conn.commit()

# создание и заполнение таблиц фактов: passport_blacklist
cursor.execute('drop table if exists STG_passport_blacklist')

def passport_blacklist(path):
    pass_file = pd.ExcelFile(path)
    load_excel(path, 'STG_passport_blacklist')
    
    cursor.execute('''
        CREATE TABLE if not exists DWH_Fact_passport_blacklist(
            id integer primary key autoincrement,
            passport_num varchar(128),
            entry_dt date,
            create_dt date, 
            update_dt date
        )
    ''')
    conn.commit()

    cursor.execute('''
        INSERT INTO DWH_Fact_passport_blacklist(
            passport_num, 
            entry_dt
        ) SELECT
            tp1.passport, 
            tp1.date 
        FROM STG_passport_blacklist tp1
        LEFT JOIN DWH_Fact_passport_blacklist tp2
        on tp1.passport = tp2.passport_num
        where tp2.passport_num is null
    ''')        
    conn.commit()

# создание и заполнение таблиц фактов: terminals
cursor.execute('drop table if exists STG_terminals')

def terminals(path):
    pass_file = pd.ExcelFile(path)
    load_excel(path, 'STG_terminals')
    
    cursor.execute('''
        ALTER TABLE STG_terminals
        ADD create_dt date
    ''')
    cursor.execute('''
        ALTER TABLE STG_terminals
        ADD update_dt date
    ''')
    cursor.execute('''
        CREATE TABLE if not exists DWH_DIM_terminals(
            id integer primary key autoincrement,
            terminal_id varchar(128),
            terminal_type varchar(128),
            terminal_city varchar(128),
            terminal_address varchar(128),
            create_dt date, 
            update_dt date
        )
    ''')
    conn.commit()
    cursor.execute('''
        INSERT INTO DWH_DIM_terminals(
            terminal_id,
            terminal_type,
            terminal_city,
            terminal_address,
            create_dt, 
            update_dt
        ) SELECT
            te1.terminal_id,
            te1.terminal_type,
            te1.terminal_city,
            te1.terminal_address,
            te1.create_dt, 
            te1.update_dt
        FROM STG_terminals te1
        LEFT JOIN DWH_DIM_terminals te2
        ON te1.terminal_id = te2.terminal_id
        WHERE te2.terminal_id is Null
    ''')
    conn.commit()
    pass_file.close()

# создание отчета по мошенническим операциям:
def fraud_report(date):
    # паспорт заблокирован:
    cursor.execute('''
        CREATE TABLE if not exists REP_FRAUD_pass(
            event_dt date,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt date
        )
    ''')
    cursor.execute('''     
        INSERT INTO REP_FRAUD_pass(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            t4.trans_date,
            t1.passport_num as passport,
            t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as fio,
            t1.phone,
            'passport in blacklist' as event_type,
            current_timestamp
        FROM DWH_DIM_clients t1
        JOIN DWH_DIM_accounts t2
        ON t1.client_id = t2.client
            JOIN DWH_DIM_cards t3
            ON t2.account_num = t3.account_num
                JOIN DWH_Fact_transactions t4
                ON t3.card_num = t4.card_num
        WHERE t4.trans_date in(
                            SELECT transaction_date
                            FROM STG_transactions)
        AND passport in (
            SELECT passport_num
            FROM DWH_Fact_passport_blacklist
            )
    ''')
    conn.commit()
     
     # паспорт недействителен:     
    cursor.execute('''
        CREATE TABLE if not exists REP_FRAUD_pass_not_val(
        event_dt date,
        passport varchar(128),
        fio varchar(128),
        phone varchar(128),
        event_type varchar(128),
        report_dt date
        )
    ''')        
    cursor.execute('''
        INSERT INTO REP_FRAUD_pass_not_val(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        )SELECT 
            t4.trans_date as event_dt,
            t1.passport_num as passport,
            t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as fio,
            t1.phone,
            'passport not valid' as event_type,
            current_date as report_dt
        FROM DWH_Fact_transactions t4 
        LEFT JOIN DWH_DIM_cards t3
        ON t4.card_num = t3.card_num
            LEFT JOIN DWH_DIM_accounts t2
            ON t3.account_num = t2.account_num
                LEFT JOIN DWH_DIM_clients t1
                ON t2.client = t1.client_id
        WHERE t4.trans_date IN (
                            SELECT transaction_date
                            FROM stg_transactions)
        AND t1.passport_valid_to < t4.trans_date
    ''')
    conn.commit() 

    # недействующий договор:
    cursor.execute('''
        CREATE TABLE if not exists REP_FRAUD_acc(
            event_dt date,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt date
        )
    ''')

    cursor.execute('''
        INSERT INTO REP_FRAUD_acc(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            t4.trans_date as event_dt,
            t1.passport_num as passport,
            t1.last_name || ' ' || t1.first_name || ' ' || t1.patronymic as fio,
            t1.phone,
            'account not valid' as event_type,
            current_timestamp
        FROM DWH_Fact_transactions t4
        INNER JOIN DWH_DIM_cards t3 
        ON t4.card_num = t3.card_num
        and t4.trans_date in (
                            SELECT transaction_date
                            from STG_transactions)
            INNER JOIN DWH_DIM_accounts t2 
            ON t3.account_num = t2.account_num
                INNER JOIN DWH_DIM_clients t1
                ON t2.client = t1.client_id
            WHERE t2.valid_to < t4.trans_date
    ''')
    conn.commit() 

    # совершение операций в разных городах в течение одного часа:
    cursor.execute('''
        CREATE TABLE if not exists REP_FRAUD_city(
            event_dt date,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt date
        )
    ''')

    cursor.execute('''
        INSERT INTO REP_FRAUD_city(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            trans_date as event_dt,
            passport_num as passport,
            last_name || ' ' || first_name || ' ' || patronymic as fio,
            phone,
            'different city' as event_type,
            current_timestamp
        FROM (
            SELECT
                card_num,
                terminal_city,
                lag_term_city,
                trans_date,
                lag_trans_date 
            FROM (
                SELECT 
                    t4.card_num,
                    t4.trans_date,
                    t6.terminal_city,
                    LAG (t6.terminal_city) over (PARTITION BY t4.card_num ORDER BY t4.trans_date ASC) as lag_term_city,
                    LAG (t4.trans_date) over (PARTITION BY t4.card_num ORDER BY t4.trans_date ASC) as lag_trans_date
                FROM DWH_Fact_transactions t4
                INNER JOIN DWH_DIM_terminals t6
                ON t4.terminal = t6.terminal_id
                ) tt
                WHERE terminal_city <> lag_term_city 
                AND trans_date in(
                                SELECT transaction_date
                                FROM STG_transactions) 
                AND trans_date - '1 hour' < lag_trans_date) tt_1
        INNER JOIN DWH_DIM_cards t3
        ON tt_1.card_num = t3.card_num
            INNER JOIN DWH_DIM_accounts t2
            ON t3.account_num = t2.account_num
                INNER JOIN DWH_DIM_clients t1
                ON t2.client = t1.client_id
    ''')
    conn.commit() 

    # попытка подбора суммы:
    cursor.execute('''
        CREATE TABLE if not exists REP_FRAUD_amount(
            event_dt date,
            passport varchar(128),
            fio varchar(128),
            phone varchar(128),
            event_type varchar(128),
            report_dt date
        )
    ''')

    cursor.execute('''
        INSERT INTO REP_FRAUD_amount(
            event_dt,
            passport,
            fio,
            phone,
            event_type,
            report_dt
        ) SELECT
            s3.trans_date as event_dt,
            s3.passport_num as passport,
            s3.last_name || ' ' || s3.first_name || ' ' || s3.patronymic as fio,
            s3.phone,
            'selection of the amount' as event_type,
            current_timestamp
        FROM (
            SELECT 
                trans_date, 
                t1.*
            FROM (
                SELECT * 
                FROM (
                    SELECT *
                    FROM (
                        WITH tt4 as(
                                SELECT 
                                    *,
                                    LAG (trans_date, 3) over (PARTITION BY card_num ORDER BY trans_date ASC) as prev_date,
                                    LAG (amt, 1) over (PARTITION BY card_num ORDER BY trans_date ASC) as prev_1_amt,
                                    LAG (amt, 2) over (PARTITION BY card_num ORDER BY trans_date ASC) as prev_2_amt,
                                    LAG (amt, 3) over (PARTITION BY card_num ORDER BY trans_date ASC) as prev_3_amt,
                                    LAG (oper_result, 1) over (PARTITION BY card_num ORDER BY trans_date ASC) as prev_1_res,
                                    LAG (oper_result, 2) over (PARTITION BY card_num ORDER BY trans_date ASC) as prev_2_res,
                                    LAG (oper_result, 3) over (PARTITION BY card_num ORDER BY trans_date ASC) as prev_3_res
                                FROM DWH_Fact_transactions t4), 
                        current_dt as (SELECT transaction_date FROM stg_transactions)
                        SELECT  
                            *
                        FROM tt4       
                        WHERE oper_result = 'SUCCESS' 
                            AND prev_1_res = 'REJECT' 
                            AND prev_2_res = 'REJECT' 
                            AND prev_3_res = 'REJECT'
                            AND prev_1_amt > amt
                            AND prev_2_amt > prev_1_amt
                            AND prev_3_amt > prev_2_amt
                            AND trans_date - '20 minute' < prev_date
                            AND trans_date IN (
                                    SELECT trans_date
                                    FROM current_dt)
                        ) as stt4
                    JOIN DWH_DIM_cards t3
                    ON stt4.card_num = t3.card_num) as s1
                JOIN DWH_DIM_accounts t2
                ON s1.account_num = t2.account_num) as s2
            JOIN DWH_DIM_clients t1
            ON  s2.client = t1.client_id) as s3
    ''')  
    conn.commit() 

    # вывод таблиц:
def showTable(tableName):
    cursor.execute(f'SELECT * FROM {tableName}')

    for row in cursor.fetchall():
        print(' '.join([str(item) for item in row]))


# # запуск загрузки из ddl_dml.sql
load_SQL('ddl_dml.sql')

# # запуск создания таблиц измерений 
DWH_DIM_tables('BANK.db')

# showTable('DWH_DIM_cards')
# print('---'*10)
# showTable('DWH_DIM_accounts')
# print('---'*10)
# showTable('DWH_DIM_clients')
# print('---'*10)


# загрузка данных,создание и заполнение таблиц фактов на 01 число:
load_excel('passport_blacklist_01032021.xlsx', 'blacklist')
load_excel('terminals_01032021.xlsx', 'terminals')
load_txt('transactions_01032021.txt')
transactions('transactions_01032021.txt', '2021-03-01')
passport_blacklist('passport_blacklist_01032021.xlsx')
terminals('terminals_01032021.xlsx')

# создание отчета о мошенничестве на 01 число:
fraud_report('2021-03-01')

# переименование загрузочных файлов на 01 число и перемещение их в архив:
file_name1 = 'transactions_01032021.txt'
file_name2 = 'passport_blacklist_01032021.xlsx'
file_name3 = 'terminals_01032021.xlsx'

backup_path1 = os.path.join("archive", "transactions_01032021.txt.backup")
backup_path2 = os.path.join("archive", "passport_blacklist_01032021.xlsx.backup")
backup_path3 = os.path.join("archive", "terminals_01032021.xlsx.backup")

os.rename(file_name1, backup_path1)
os.rename(file_name2, backup_path2)
os.rename(file_name3, backup_path3)


# загрузка данных,создание и заполнение таблиц фактов на 02 число:
load_excel('passport_blacklist_02032021.xlsx', 'blacklist')
load_excel('terminals_02032021.xlsx', 'terminals')
load_txt('transactions_02032021.txt')
transactions('transactions_02032021.txt', '2021-03-02')
passport_blacklist('passport_blacklist_02032021.xlsx')
terminals('terminals_02032021.xlsx')

# создание отчета о мошенничестве на 02 число:
fraud_report('2021-03-02')

# переименование загрузочных файлов на 02 число и перемещение их в архив:
file_name1 = 'transactions_02032021.txt'
file_name2 = 'passport_blacklist_02032021.xlsx'
file_name3 = 'terminals_02032021.xlsx'

backup_path1 = os.path.join("archive", "transactions_02032021.txt.backup")
backup_path2 = os.path.join("archive", "passport_blacklist_02032021.xlsx.backup")
backup_path3 = os.path.join("archive", "terminals_02032021.xlsx.backup")

os.rename(file_name1, backup_path1)
os.rename(file_name2, backup_path2)
os.rename(file_name3, backup_path3)


# загрузка данных,создание и заполнение таблиц фактов на 03 число:
load_excel('passport_blacklist_03032021.xlsx', 'blacklist')
load_excel('terminals_03032021.xlsx', 'terminals')
load_txt('transactions_03032021.txt')
transactions('transactions_03032021.txt', '2021-03-03')
passport_blacklist('passport_blacklist_03032021.xlsx')
terminals('terminals_03032021.xlsx')

# создание отчета о мошенничестве на 03 число:
fraud_report('2021-03-03')


# переименование загрузочных файлов на 03 число и перемещение их в архив :
file_name1 = 'transactions_03032021.txt'
file_name2 = 'passport_blacklist_03032021.xlsx'
file_name3 = 'terminals_03032021.xlsx'

backup_path1 = os.path.join("archive", "transactions_03032021.txt.backup")
backup_path2 = os.path.join("archive", "passport_blacklist_03032021.xlsx.backup")
backup_path3 = os.path.join("archive", "terminals_03032021.xlsx.backup")

os.rename(file_name1, backup_path1)
os.rename(file_name2, backup_path2)
os.rename(file_name3, backup_path3)

# вывод таблиц из отчета о мошенничестве
showTable('REP_FRAUD_pass') 
showTable('REP_FRAUD_pass_not_val')
showTable('REP_FRAUD_acc')
showTable('REP_FRAUD_city')
showTable('REP_FRAUD_amount')



# python main.py  

