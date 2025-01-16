import psycopg2

conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="pass")
cur = conn.cursor()


query = "insert into logs.logs_ds (etl_table, date_start, operation_status) values ('f101_round_v2FROMcvs',clock_timestamp()::TIME,11);"
cur.execute(query)

with open('C:/Users/maksi/OneDrive/Рабочий стол/NeoFlex/DE Проектное/1.4/table.csv') as f:
    cur.copy_expert("COPY dm.dm_f101_round_f_v2 FROM STDIN WITH CSV DELIMITER ';' HEADER ", f)

query = "UPDATE logs.logs_ds SET DATE_END = NOW()::TIME, OPERATION_STATUS = 0, TIME_ETL = clock_timestamp()::TIME - DATE_START WHERE OPERATION_STATUS = 11;"
cur.execute(query)
conn.commit()
