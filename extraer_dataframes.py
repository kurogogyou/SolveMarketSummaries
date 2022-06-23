from multiprocessing import connection
import os, pandas as pd
import pyodbc
import sqlalchemy as s

query = ''' 
DECLARE @ReportDate DATETIME = NULL, @Period VARCHAR(10) = 'WEEK';
	IF @ReportDate IS NULL
	BEGIN
		DECLARE @CurrentDate DATETIME = GETDATE();

		SELECT @ReportDate = CASE
								 WHEN DATEPART(WEEKDAY, @CurrentDate) > 5
								 THEN DATEADD(DAY, +4, DATEADD(WEEK, DATEDIFF(WEEK, 0, @CurrentDate), 0))
								 ELSE DATEADD(DAY, -3, DATEADD(WEEK, DATEDIFF(WEEK, 0, @CurrentDate), 0))
							 END;
	END

	DECLARE @snapHour INT = 15;

	SELECT @ReportDate = DATEADD(HOUR, @snapHour, @ReportDate);
	DECLARE @previousPeriodEndDate DATETIME = CASE
												WHEN @Period='WEEK' THEN DATEADD(WEEK, -1, @ReportDate)
												WHEN @Period='MONTH' THEN DATEADD(MONTH, -1, @ReportDate)
											   END

	DECLARE @previousPeriodStartDate DATETIME = CASE
													WHEN @Period='WEEK' THEN  DATEADD(wk, DATEDIFF(wk,0,@previousPeriodEndDate), 0)
													WHEN @Period='MONTH' THEN DATEADD(month, DATEDIFF(month, 0, @previousPeriodEndDate), 0)
												END,
			@reportStartDate DATETIME =  CASE
											WHEN @Period='WEEK' THEN  DATEADD(wk, DATEDIFF(wk,0,@ReportDate), 0)
											WHEN @Period='MONTH' THEN DATEADD(month, DATEDIFF(month, 0, @ReportDate), 0)
										  END;

	DECLARE @startDate DATE = DATEADD(DAY, CASE DATENAME(WEEKDAY, @previousPeriodStartDate)
											WHEN 'Sunday' THEN -2
											WHEN 'Monday' THEN -3
											ELSE -1 END, DATEDIFF(DAY, 0, @previousPeriodStartDate))

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'Start'

	SELECT @ReportDate [ReportDate], @previousPeriodEndDate [PreviousReportDate];

	SELECT  [MuniCompositeId],
			SecurityId,
			[Date],
		    [BidComposite],
		    [OfferComposite],
		    [ProviderCount],
		    [BidCount],
            [OfferCount],
			[IsSnap],
			[JsonDetail],
			[UpdatedDate],
			[IsQuarantined],
			[Period],
			JSON_VALUE(JsonDetail, '$.OgBdCmp') As OriginalBidComposite,
			JSON_VALUE(JsonDetail, '$.OgOfCmp') As OriginalOfferComposite,
			JSON_VALUE(JsonDetail, '$.BdOfSprMsg') As BidOfferSpreadMessage,
			CONVERT(BIT, NULL) PeriodRecord,
			CONVERT(INT, NULL) CommonProviderCount,
			CONVERT(FLOAT, NULL) CommonProviderPercChanged
	INTO ##TempEODSnap
	FROM
	(
		SELECT *,
			   CASE
				   WHEN [Date] >= @reportStartDate
						AND [Date] <= @ReportDate
				   THEN 1
				   WHEN [Date] >= @previousPeriodStartDate
						AND [Date] <= @previousPeriodEndDate
				   THEN 2
				   ELSE 3
			   END [Period],
			   ROW_NUMBER() OVER (PARTITION BY SecurityId, CONVERT(DATE, [Date]) ORDER BY [MuniCompositeId] DESC) Ord
		FROM [SolveMarketData].[dbo].[MuniComposite]
		WHERE (
				  [BidComposite] IS NOT NULL
				  OR OfferComposite IS NOT NULL
			  )
			  AND ISNULL([IsQuarantined], 0) = 0
			  AND [Date] >= @startDate
			  AND [Date] <= @ReportDate
			  AND DATEPART(DW, [Date]) NOT IN ( 1, 7 )
			  AND [IsSnap] = 1
			  AND CONVERT(VARCHAR, [Date], 8) = '15:00:00'
	) a
	WHERE Ord=1

	OPTION(RECOMPILE);
    select * from ##TempEODSnap;
'''
query2 = 'select * from ##TempEODSnap;'

sql_file = "corpreport.sql"

queryfromfile = open(sql_file).read()


# pyodbc
servername = 'saaws-sql14'
port = '1433'
dbname = 'SolveComposite'

engine = s.create_engine('mssql+pymssql://@{}:{}/{}'.format(servername, port, dbname)) #?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server
#engine = create_engine('mssql+pyodbc://@saaws-sql14/SolveComposite?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server')
connection = engine.connect().connection
cursor = connection.cursor()

# cursor.execute(''' 
#                 SELECT top 5 a.[SecurityId]
#                 ,a.[Date]
#                 ,a.[ProviderCountUsed]
#                 ,a.[BidCountUsed]
#                 ,a.[IsSnap]
#                 ,a.[IsQuarantined]
#                 FROM [SolveComposite].[dbo].[CorpComposite] as A'''
#                 )

# print(cursor.keys())

#cursor.execute('EXECUTE SolveComposite.dbo.[spCorpCompositeReport] @Sector=\'IG\'')
#cursor.execute(query)
#cursor.execute(query2)

cursor.execute(queryfromfile)
#print(cursor.description)

#cursor.execute('DROP TABLE IF EXISTS ##TempEODSnap;')
#cursor.execute('DROP TABLE IF EXISTS #TempEODSnap;')

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

while 1:
    row = cursor.fetchone()
    if not row:
        break
    print(row)


cursor.close()

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

