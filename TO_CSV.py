import psycopg2

conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="pass")
cur = conn.cursor()


query = "insert into logs.logs_ds (etl_table, date_start, operation_status) values ('f101_round_v2TOcvs',clock_timestamp()::TIME,10);"
cur.execute(query)


sql = "COPY (SELECT * FROM dm.dm_f101_round_f) TO STDOUT WITH CSV DELIMITER ';'  HEADER "
with open("C:/Users/maksi/OneDrive/Рабочий стол/NeoFlex/DE Проектное/1.4/table.csv", "w") as file:
    cur.copy_expert(sql, file)


query = "UPDATE logs.logs_ds SET DATE_END = NOW()::TIME, OPERATION_STATUS = 0, TIME_ETL = clock_timestamp()::TIME - DATE_START WHERE OPERATION_STATUS = 10;"
cur.execute(query)
conn.commit()