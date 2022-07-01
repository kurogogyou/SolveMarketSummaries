DECLARE @ReportDate DATETIME = NULL, @Period VARCHAR(10) = 'WEEK', @Sector VARCHAR(10) = 'HY';

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

    drop table if exists ##ReportDates_dev;
	SELECT @ReportDate [ReportDate], @previousPeriodEndDate [PreviousReportDate]
    into ##ReportDates_dev;
	
	DROP TABLE IF EXISTS #TempEODSnap
	SELECT  CorpCompositeId,
			SecurityId,
			[Date],
		    [BidComposite],
		    [OfferComposite],
		    TRY_CONVERT(FLOAT, JSON_VALUE([a].[AdditionalData], '$.Counts.All.PVR')) AS [ProviderCount],
		    TRY_CONVERT(FLOAT, JSON_VALUE([a].[AdditionalData], '$.Counts.All.BID')) AS [BidCount],
			TRY_CONVERT(FLOAT, JSON_VALUE([a].[AdditionalData], '$.Counts.All.OFR')) AS [OfferCount],
			[IsSnap],			
			[UpdatedDate],
			[IsQuarantined], 
			[Period],

			TRY_CONVERT(DECIMAL(12, 5), JSON_VALUE([a].[AdditionalData], '$.OgBidCmp')) As OriginalBidComposite,
			TRY_CONVERT(DECIMAL(12, 5), JSON_VALUE([a].[AdditionalData], '$.OgOfrCmp')) As OriginalOfferComposite,
			--JSON_VALUE(JsonDetail, '$.BdOfSprMsg') As BidOfferSpreadMessage,
			CONVERT(BIT, NULL) PeriodRecord, 
			CONVERT(INT, NULL) CommonProviderCount, 
			CONVERT(FLOAT, NULL) CommonProviderPercChanged
	INTO #TempEODSnap
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
			   ROW_NUMBER() OVER (PARTITION BY SecurityId, CONVERT(DATE, [Date]) ORDER BY CorpcompositeId DESC) Ord
		FROM [SolveComposite].[dbo].CorpComposite
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
	select top 500 * from #TempEODSnap;
	drop table if exists #TempEODSnap;