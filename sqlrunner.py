import csv
import json
from datetime import date

import paramiko
import pymysql

today = date.today()
today = today.strftime("%Y%m%d")
print(today)

# from paramiko import SSHClient
from sshtunnel import SSHTunnelForwarder

with open("/Users/SergioPina/Documents/pyCredents.json") as f:
    credents = json.load(f)

custCountry = 'novartises'
# ssh config  , pending to put this file rout in credents.Json

mypkey = paramiko.RSAKey.from_private_key_file('/Program Files/DBeaver/sergio.pina.pem')
ssh_host = 'bdpeu001.aktana.com'
ssh_port = 22
# mysql config
sql_hostname = custCountry + 'rds.aktana.com'
sql_main_database = custCountry + 'prod'
sql_port = 3306
host = custCountry + 'rds.aktana.com'
tunnel = SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=credents['username'],
    ssh_password=credents['password'],
    ssh_pkey=mypkey,
    remote_bind_address=(sql_hostname, sql_port))
tunnel.start()
# tunnel.close()
conn = pymysql.connect(host='127.0.0.1', user=credents['username'],
                       passwd=credents['password'], db=sql_main_database,
                       port=tunnel.local_bind_port)

print("MySQL connection succeeded at {}.", pymysql.DATETIME)

cursor = conn.cursor()

sql_file = open("C:/Users/SergioPina/Documents/git/repReport/dummy.sql")
sql_as_string = sql_file.read()
sql_file.close()
sqlCommands = sql_as_string.split(';')

for command in sqlCommands:
    qry = command.strip('\n')
    if qry != "":
        out = cursor.execute(qry)
        print(out)
        data = cursor.fetchall()
        print(data)
        with open("C:/Users/SergioPina/Desktop/dummy.csv", 'w', newline='') as f_handle:
            writer = csv.writer(f_handle)
            # Add the header/column names
            # header = ['result']
            # writer.writerow(header)
            # Iterate over `data`  and  write to the csv file
            for row in data:
                writer.writerow(row)




