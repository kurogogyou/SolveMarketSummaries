from multiprocessing import connection
import os, pandas as pd
import sqlalchemy as s

sql_file = "corpreport.sql"
queryfromfile = open(sql_file).read()
cleanup_query = '''
	drop table if exists ##ReportDates_dev;
	drop table if exists ##TempBidOfferVolume_dev;
	drop table if exists ##TempLosers_dev;
	drop table if exists ##TempMostQuoted_dev;
	drop table if exists ##TempObservedPrices_dev;
	drop table if exists ##TempSentiment_dev;
	drop table if exists ##TempTopQuoteVolume_dev;
	drop table if exists ##TempWinners_dev;
'''

testquery = open("testquery.sql").read()

# pyodbc
servername = 'saaws-sql14'
port = '1433'
dbname = 'SolveComposite'

engine = s.create_engine('mssql+pymssql://@{}:{}/{}'.format(servername, port, dbname)) #?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server
#engine = create_engine('mssql+pyodbc://@saaws-sql14/SolveComposite?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server')
connection = engine.connect()
#cursor = connection.cursor()

# cursor = connection.execute(queryfromfile)
df = pd.read_sql(testquery, connection)
print(df)
df.to_excel("output.xlsx")
#cursor.close()
#print(cursor.description)




# Cursores

#cursor = connection.execute('SELECT * FROM ##TempBidOfferVolume_dev')
# Results set 1
# column_names = [col[0] for col in cursor.description] # Get column names from MySQL

# df1_data = []
# for row in cursor.fetchall():
#     df1_data.append({name: row[i] for i, name in enumerate(column_names)})

# Results set 2
#cursor.nextset()

# print(cursor.description)
# column_names = [col[0] for col in cursor.description] # Get column names from MySQL

# df2_data = []
# for row in cursor.fetchall():
#     df2_data.append({name: row[j] for j, name in enumerate(column_names)})

# while 1:
#     row = cursor.fetchone()
#     if not row:
#         break
#     print(row)


# connection.execute(cleanup_query)
# cursor.close()

# print(df1_data)
# print(df2_data)

# df1 = pd.DataFrame(df1_data)
# df2 = pd.DataFrame(df2_data)

# print(df1)
# print(df2)

# pymssql
#engine = create_engine('mssql+pymssql://scott:tiger@hostname:port/dbname')

# connection = pyodbc.connect('DRIVER={SQL Server}; SERVER=saaws-sql14; DATABASE=SolveComposite; Trusted_Connection=yes;')

# cursor = connection.cursor()


# while 1:
#     row = cursor.fetchone()
#     if not row:
#         break
#     print(row)



