USE [Temporary]
GO

/****** Object:  StoredProcedure [dbo].[sp_MuniCompositeReports]    Script Date: 6/14/2022 4:35:02 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_MuniCompositeReports_dev]
	@ReportDate DATETIME = NULL,
	@Period VARCHAR(10) = 'WEEK'
AS
/******************************************************************************
Date            :
Created by      : Safi Parvez
Description     : Generates the output for Muni Marketing Reports
EXEC SP_HELPTEXT2 [sp_MuniCompositeReports]

Example:
EXEC [sp_MuniCompositeReports]

Last modified by:
2022-02-07, Gautam Nemlekar
2022-03-30, Dennys Rodriguez

*******************************************************************************/
BEGIN
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

	SELECT @ReportDate [ReportDate], @previousPeriodEndDate [PreviousReportDate]

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
	INTO #TempEODSnap_dev
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

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'A - Getting Data'

	CREATE CLUSTERED INDEX IX_TempCluster ON [#TempEODSnap_dev](SecurityId, [Period], [Date] DESC)

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'B - Clustered Index'

	UPDATE [s1]
	SET PeriodRecord = a.[PeriodEndRecord]
	FROM [#TempEODSnap_dev] s1
		JOIN
		(
			SELECT [s].[MuniCompositeId],
				   CASE
					   WHEN ROW_NUMBER() OVER (PARTITION BY SecurityId, [Period] ORDER BY [Date] DESC) = 1
					   THEN 1
					   ELSE 0
				   END PeriodEndRecord
			FROM [#TempEODSnap_dev] s
		) a
			ON s1.[MuniCompositeId] = a.[MuniCompositeId];

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'C - Setting Period Record'

	SELECT [a].[SecurityId],
		   [a].[MuniCompositeId] ReportPeriod_MuniCompositeId,
		   [a].[BidComposite] ReportPeriod_BidComposite,
		   [a].[OfferComposite] ReportPeriod_OfferComposite,
		   [a].[OfferCount] ReportPeriod_OfferCount,
		   [a].[BidCount] ReportPeriod_BidCount,
		   a.[ProviderCount] ReportPeriod_ProviderCount,
		   [a].[JsonDetail] ReportPeriod_JsonDetail,
		   [b].[MuniCompositeId] PreviousPeriod_MuniCompositeId,
		   [b].[BidComposite] PreviousPeriod_BidComposite,
		   [b].[OfferComposite] PreviousPeriod_OfferComposite,
		   [b].[OfferCount] PreviousPeriod_OfferCount,
		   [b].[BidCount] PreviousPeriod_BidCount,
		   b.[ProviderCount] PreviousPeriod_ProviderCount,
		   [b].[JsonDetail] PreviousPeriod_JsonDetail
	INTO #TempSummary_dev
	FROM
	(
		SELECT [MuniCompositeId], [SecurityId], [BidComposite], [OfferComposite], [JsonDetail], [BidCount], [OfferCount],[ProviderCount]
		FROM [#TempEODSnap_dev]
		WHERE PeriodRecord=1
		AND [Period]=1
	) a
	CROSS APPLY
	(
		SELECT s1.[MuniCompositeId], s1.[BidComposite], s1.[OfferComposite], s1.[JsonDetail], [BidCount], [OfferCount],[ProviderCount]
		FROM [#TempEODSnap_dev] s1
		WHERE s1.SecurityId=a.[SecurityId]
		AND [s1].[PeriodRecord]=1
		AND [s1].[Period]=2
	) b

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'D - Created Summary'
 	SELECT rs.SecurityId, [SecurityName], rs.[MuniEmmaSecurityDesc] SecurityDescription, [rs].[MuniIssuerState] [State], rs.[MuniDatedDate] [DatedDate], rs.[PrimaryIdentifier] [Identifier], rs.MuniCoupon [Coupon], rs.MuniMaturityDate [MaturityDate]
   -- SELECT rs.SecurityId, [SecurityName], rs.SecurityNameAlt SecurityDescription, [rs].[MuniIssuerState] [State], rs.[MuniDatedDate] [DatedDate], rs.[PrimaryIdentifier] [Identifier], rs.MuniCoupon [Coupon], rs.MuniMaturityDate [MaturityDate]
	INTO #TempSecurityInfo_dev
	FROM [SolveSM].dbo.[rSecurity] rs
	JOIN
	(
		SELECT DISTINCT SecurityId FROM [#TempEODSnap_dev]
	) a ON [a].[SecurityId] = [rs].[SecurityId]
	WHERE rs.[MuniIssuerState] !=''

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'E - Getting Security Data'

	--Generating TOP 300 Winners
	SELECT TOP 300
			ROW_NUMBER() OVER (ORDER BY ReportPeriod_OfferComposite/PreviousPeriod_OfferComposite DESC) Ord,
			s.SecurityId,
			[s].[Identifier],
			s.SecurityName,
			[s].[SecurityDescription],
			concat(rs.MuniIssuerState, ' ', rs.MuniIssuerTicker, ' ' , cast(s.Coupon as decimal(10,2)) , ' ' , convert(varchar(10), s.MaturityDate, 101)) ShortName, --Dennys Rodriguez, muni short name 31/03/2022
			s.Coupon,
			s.MaturityDate,
			[ts].[ReportPeriod_OfferCount] Dealers,
			(ReportPeriod_OfferComposite/PreviousPeriod_OfferComposite)-1 [PercentageChanged],
			ReportPeriod_OfferComposite [ReportOfferComposite],
			PreviousPeriod_OfferComposite [PreviousOfferComposite]
	INTO #TempWinners_dev
	FROM [#TempSummary_dev] ts
	JOIN [#TempSecurityInfo_dev] s ON ts.SecurityId=s.SecurityId
	--
	LEFT JOIN SolveSM.dbo.rSecurity rs ON ts.SecurityId = rs.SecurityId --Dennys Rodriguez, muni short name 31/03/2022
	--
	WHERE [ts].[ReportPeriod_OfferComposite] IS NOT NULL AND ts.[PreviousPeriod_OfferComposite] IS NOT NULL AND ts.[ReportPeriod_OfferCount]>=2

	ORDER BY Ord

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'F - Building Top 300 Winners'

	SELECT TOP 300
			ROW_NUMBER() OVER (ORDER BY ReportPeriod_OfferComposite/PreviousPeriod_OfferComposite) Ord,
			s.SecurityId,
			[s].[Identifier],
			s.SecurityName,
			[s].[SecurityDescription],
			concat(rs.MuniIssuerState, ' ', rs.MuniIssuerTicker, ' ' , cast(s.Coupon as decimal(10,2)) , ' ' , convert(varchar(10), s.MaturityDate, 101)) ShortName, --Dennys Rodriguez, muni short name 31/03/2022
			s.Coupon,
			s.MaturityDate,
			[ts].[ReportPeriod_OfferCount] Dealers,
			(ReportPeriod_OfferComposite/PreviousPeriod_OfferComposite)-1 [PercentageChanged],
			ReportPeriod_OfferComposite [ReportOfferComposite],
			PreviousPeriod_OfferComposite [PreviousOfferComposite]
	INTO #TempLosers_dev
	FROM [#TempSummary_dev] ts
	JOIN [#TempSecurityInfo_dev] s ON ts.SecurityId=s.SecurityId
	--
	LEFT JOIN SolveSM.dbo.rSecurity rs ON ts.SecurityId = rs.SecurityId --Dennys Rodriguez, muni short name 31/03/2022
	--
	WHERE [ts].[ReportPeriod_OfferComposite] IS NOT NULL AND ts.[PreviousPeriod_OfferComposite] IS NOT NULL AND ts.[ReportPeriod_OfferCount]>=2
	ORDER BY Ord

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'G - Building Top 300 Losers'
	SELECT [a].[SecurityId], [a].[Date], a.[Period], SUM([a].[Value]*[a].[SumWeights]) / SUM(a.[SumWeights]) WeightedPrice
	INTO #TempObservedPrices_dev
	FROM
	(
		SELECT s.[MuniCompositeId],

			   s.[Date],

			   s.[Period],

			   s.SecurityId,

			   j.[AggregateColorId],

			   j.[Provider],

			   j.[QuoteType],

			   CONVERT(FLOAT,j.[Value]) [Value],

			   SUM(CONVERT(FLOAT, jw.[WeightValue])) SumWeights

		FROM [#TempEODSnap_dev] s

		JOIN

		(

			SELECT SecurityId

			FROM [#TempWinners_dev]

			UNION ALL

			SELECT [SecurityId]

			FROM [#TempLosers_dev]

		) a ON [a].[SecurityId] = [s].[SecurityId]

		CROSS APPLY

		OPENJSON(s.JsonDetail, N'$.OfWts')

		WITH

		(

			AggregateColorId BIGINT N'$.AggId',

			[Provider] VARCHAR(50) N'$.Prv',

			[QuoteType] VARCHAR(50) N'$.QT',

			[Value] VARCHAR(50) N'$.VL',

			[ModeledPrice] VARCHAR(50) N'$.PRC',

			[Weights] NVARCHAR(MAX) N'$.Wts' AS JSON

		) AS j

		CROSS APPLY

		OPENJSON(j.Weights, '$')

		WITH (

			WeightValue VARCHAR(50) N'$.Wt'

		) AS jw

		WHERE s.[Period]<=2

		AND j.[QuoteType]='PRICE'
		GROUP BY s.SecurityId,
				 s.[MuniCompositeId],
				 s.[Date],
				 j.[AggregateColorId],
				 j.[Provider],
				 j.[QuoteType],
				 j.[Value],
				 s.[Period]
	) a
	GROUP BY [a].[SecurityId], [a].[Date], [a].[Period]
	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'H.1 - Building Common Providers 1'
	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'H.3 - Building Common Providers 3'

	--TOP 200 Winners & Losers
	SELECT tw.*,
                CASE WHEN cp.[WeightedPrice] >0 AND pp.[WeightedPrice]>0 THEN (cp.[WeightedPrice]/pp.[WeightedPrice])-1 ELSE NULL END [PxOnlyPercentageChanged],
	    		cp.[WeightedPrice] [ReportPxOnlyComposite],
                pp.WeightedPrice [PreviousPxOnlyComposite]
	FROM [#TempWinners_dev] tw
	OUTER APPLY
	(
		SELECT TOP 1 t3.[WeightedPrice]
		FROM #TempObservedPrices_dev t3
        WHERE 	t3.[SecurityId]=tw.[SecurityId]
		AND t3.[Period]=2
		ORDER BY t3.[Date] DESC
	) pp
	OUTER APPLY
	(
		SELECT TOP 1 t3.[WeightedPrice]
		FROM #TempObservedPrices_dev t3
        WHERE 	t3.[SecurityId]=tw.[SecurityId]
		AND t3.[Period]=1
		ORDER BY t3.[Date] DESC
	) cp
	ORDER BY Ord

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'I- Displaying Top 200 Winners'

	SELECT tl.*,
			    CASE WHEN cp.[WeightedPrice] >0 AND pp.[WeightedPrice]>0 THEN (cp.[WeightedPrice]/pp.[WeightedPrice])-1 ELSE NULL END [PxOnlyPercentageChanged],
				cp.[WeightedPrice] [ReportPxOnlyComposite],
	             pp.WeightedPrice [PreviousPxOnlyComposite]
	FROM #TempLosers_dev tl
	OUTER APPLY
	(
		SELECT TOP 1 t3.[WeightedPrice]
		FROM #TempObservedPrices_dev t3
        WHERE 	t3.[SecurityId]=tl.[SecurityId]
		AND t3.[Period]=2
		ORDER BY t3.[Date] DESC
	) pp
	OUTER APPLY
	(
		SELECT TOP 1 t3.[WeightedPrice]
		FROM #TempObservedPrices_dev t3
        WHERE 	t3.[SecurityId]=tl.[SecurityId]
		AND t3.[Period]=1
		ORDER BY t3.[Date] DESC
	) cp
	ORDER BY Ord


	DECLARE @PivotDates VARCHAR(MAX) = '', @Sql VARCHAR(MAX)=''

	--Sentiment
	SELECT [Date],
			CONVERT(DECIMAL(28,10), SUM(CASE WHEN IsDeclining=1 THEN 1 ELSE 0 END)) / CONVERT(DECIMAL(28, 10), COUNT(*)) [Decliners],
			CONVERT(DECIMAL(28,10), SUM(CASE WHEN IsDeclining=0 THEN 1 ELSE 0 END)) / CONVERT(DECIMAL(28, 10), COUNT(*)) [Advancers],
			CONVERT(DECIMAL(28,10), SUM(CASE WHEN IsDeclining IS NULL THEN 1 ELSE 0 END)) / CONVERT(DECIMAL(28, 10), COUNT(*)) [Flats],
			SUM(CASE WHEN IsDeclining=1 THEN 1 ELSE 0 END) [DeclinersCount],
			SUM(CASE WHEN IsDeclining=0 THEN 1 ELSE 0 END) [AdvancersCount],
			SUM(CASE WHEN IsDeclining IS NULL THEN 1 ELSE 0 END) [FlatsCount]
	FROM
	(
		SELECT CASE WHEN e3.[OfferComposite] > e1.[OfferComposite] THEN 1 WHEN e3.[OfferComposite] < e1.[OfferComposite] THEN 0 ELSE NULL END IsDeclining,
				CONVERT(DATE, e1.[Date]) [Date]
		FROM [#TempEODSnap_dev] e1
		CROSS APPLY(SELECT TOP 1 * FROM [#TempEODSnap_dev] e2 WHERE e1.SecurityId=e2.SecurityId AND e2.[Date] < e1.[Date] ORDER BY [Date] DESC) e3
	) a
	GROUP BY [Date]
	ORDER BY [Date]

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'L - Displaying Sentiment'

	--Top Quote Volume Movers
	SELECT TOP 50
			ROW_NUMBER() OVER(ORDER BY a.TotalQuotes-b.TotalQuotes DESC) [Rank],
			a.SecurityId,
			[a].[Identifier],
			SecurityName,
			[SecurityDescription],
			ShortName,
			Coupon,
			MaturityDate,
			b.TotalQuotes TotalQuotes_LookBack,
			a.TotalQuotes TotalQuotes_Report,
			a.TotalQuotes - b.TotalQuotes [Increase],
			CASE WHEN ISNULL(b.TotalQuotes, 0) > 0
					THEN CONVERT(DECIMAL(28, 10), a.TotalQuotes)/CONVERT(DECIMAL(28, 10), b.TotalQuotes)-1
					ELSE 0
			END [PercentageIncrease]
	FROM
	(
		SELECT	sm.SecurityId,
				sm.[Identifier],
				sm.SecurityName,
				sm.[SecurityDescription],
				concat(rs.MuniIssuerState, ' ', rs.MuniIssuerTicker, ' ' , cast(sm.Coupon as decimal(10,2)) , ' ' , convert(varchar(10), sm.MaturityDate, 101)) ShortName, --Dennys Rodriguez, muni short name 31/03/2022
				sm.Coupon,
				sm.MaturityDate,
				SUM(ISNULL(BidCount, 0) + ISNULL(OfferCount, 0)) TotalQuotes
		FROM [#TempEODSnap_dev] s
		JOIN [#TempSecurityInfo_dev] sm ON s.SecurityId = sm.SecurityId
		LEFT JOIN SolveSM.dbo.rSecurity rs ON sm.SecurityId = rs.SecurityId
		WHERE [Period] = 1
		AND sm.[DatedDate]<=@previousPeriodStartDate
		GROUP BY sm.SecurityId,
				 sm.[Identifier],
				 sm.SecurityName,
				 sm.[SecurityDescription],
				 sm.Coupon,
				 sm.MaturityDate,
				 rs.MuniIssuerState,
				 rs.MuniIssuerTicker
	) a
	JOIN
	(
		SELECT sm.SecurityId,
			   SUM(ISNULL(BidCount, 0) + ISNULL(OfferCount, 0)) TotalQuotes
		FROM [#TempEODSnap_dev] s
		JOIN [#TempSecurityInfo_dev] sm ON s.SecurityId = sm.SecurityId
		WHERE [Period] = 2
		AND sm.[DatedDate]<=@previousPeriodStartDate
		GROUP BY sm.SecurityId,
				 sm.SecurityName
	) b ON a.SecurityId=b.SecurityId;

	--Most Quoted
	SELECT TOP 50
			sm.SecurityId,
			sm.[Identifier],
			sm.SecurityName,
			sm.[SecurityDescription],
			concat(rs.MuniIssuerState, ' ', rs.MuniIssuerTicker, ' ' , cast(sm.Coupon as decimal(10,2)) , ' ' , convert(varchar(10), sm.MaturityDate, 101)) ShortName, --Dennys Rodriguez, muni short name 31/03/2022
			sm.Coupon,
			sm.MaturityDate,
			s.[ReportPeriod_ProviderCount] [Dealers]
	FROM [#TempSummary_dev]  s
	JOIN [#TempSecurityInfo_dev] sm ON s.SecurityId=sm.SecurityId
	LEFT JOIN SolveSM.dbo.rSecurity rs ON sm.SecurityId = rs.SecurityId
	ORDER BY Dealers DESC


	--Bid And Offer Volume
	--from here


	SELECT *
	INTO #TempBOSpread_dev
	FROM
	(
		SELECT	ISNULL(sm.[State], '') [State],
				CONVERT(date, [Date]) [Date],
				AVG(COALESCE(ts.[OriginalOfferComposite],[ts].[OfferComposite])-COALESCE(ts.[OriginalBidComposite],[ts].[BidComposite])) AvgSpread,
				1 Ord
		FROM [#TempEODSnap_dev] ts
		JOIN [#TempSecurityInfo_dev] sm on ts.SecurityID = sm.SecurityId
		WHERE ts.[Period]<=2
		AND COALESCE(ts.[OriginalOfferComposite],[ts].[OfferComposite]) > COALESCE(ts.[OriginalBidComposite],[ts].[BidComposite])
		GROUP BY CONVERT(date, [Date]), sm.[State]
		UNION ALL
		SELECT 'All States' [State], CONVERT(date, [Date]) [Date], AVG(COALESCE(ts.[OriginalOfferComposite],[ts].[OfferComposite])-COALESCE(ts.[OriginalBidComposite],[ts].[BidComposite])) AvgSpread, 0 Ord
		FROM [#TempEODSnap_dev] ts
		JOIN [#TempSecurityInfo_dev] sm on ts.SecurityID = sm.SecurityId
		WHERE ts.[Period]<=2
		AND COALESCE(ts.[OriginalOfferComposite],[ts].[OfferComposite]) > COALESCE(ts.[OriginalBidComposite],[ts].[BidComposite])
		GROUP BY CONVERT(date, [Date])
	) a
	ORDER BY Ord, [State], [Date]

	SELECT @PivotDates = '', @Sql =''

	SELECT @PivotDates = @PivotDates + '[' + CONVERT(VARCHAR, [Date], 1)+'],'
	FROM
	(
		SELECT DISTINCT [Date]
		FROM #TempBOSpread_dev
	) a
	ORDER BY [Date]



	


	SELECT @PivotDates = LEFT(@PivotDates, LEN(@PivotDates)-1)
	SELECT @Sql = '
		SELECT [State],
		' + @PivotDates + '
		FROM
		(SELECT [State], [Date], [AvgSpread], Ord
		FROM #TempBOSpread_dev) st
		PIVOT
		(
			MAX([AvgSpread])

			FOR [Date] IN (' + @PivotDates + ')
		) As PivotTable

		ORDER BY Ord, [State];
	'
	--EXECUTE (@Sql)

		---------------------------------------------------------------------
		--Raw data --2022-03-30
	SELECT rs.SecurityId,
	rs.[PrimaryIdentifier] [Identifier],
	[SecurityName],	
	rs.MuniDatedDate [IssueDate],
	rs.MuniIssuerState [State],
    rs.[MuniIssuerTicker] [Ticker],
			
			CASE WHEN sm.WAC IS NOT NULL THEN sm.WAC
				 WHEN yc.CurveValue IS NOT NULL THEN 
					 CASE WHEN sm.[coupon_floor] IS NOT NULL
							  AND (yc.CurveValue+(sm.[floating_spread]/100))<sm.[coupon_floor]
						  THEN sm.[coupon_floor]
						  WHEN yc.[CurveValue]+(sm.[floating_spread]/100) <0 THEN 0
						  ELSE yc.[CurveValue]+(sm.[floating_spread]/100)
					   END
				 ELSE sm.[coupon]
			END [Coupon],		   
		   sm.[maturity_date] [MaturityDate],
			rs.[MuniRatingFitchName] [Rating],
			rs.MuniPurposeSectorName [Sector]
		   --rs.[CorpIssuerMoodysIndustryName] [MoodysIndustry],
	   
	--INTO #TempSecurityInfo
	FROM [SolveSM].dbo.[rSecurity] rs
	JOIN (SELECT DISTINCT SecurityId FROM [#TempEODSnap_dev]) a ON [a].[SecurityId] = [rs].[SecurityId]
	JOIN SolveSm.dbo.[SecurityMuni] sm ON rs.[SecurityId]=sm.[security_id] 
	LEFT JOIN [SolveSM].dbo.[Code] flt_idx ON sm.[floating_index]=[flt_idx].[code_id]
	LEFT JOIN solvesm.dbo.[YieldCurve] yc ON [yc].[SectorType] = [rs].[SectorType] AND yc.[CurveType]='FloatingIndex' AND yc.FloatingIndexId=flt_idx.[code_id]	
	------------------------------------------------------------------------------------------

	EXECUTE [sp_MuniCompositeReports_AvgYields] @startDate, @ReportDate, @previousPeriodEndDate, 1
END
GO


