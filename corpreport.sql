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

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'A - Getting Data'

	CREATE CLUSTERED INDEX IX_TempCluster ON [#TempEODSnap](SecurityId, [Period], [Date] DESC)

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'B - Clustered Index'

	UPDATE [s1]
	SET PeriodRecord = a.[PeriodEndRecord]
	FROM [#TempEODSnap] s1
		JOIN
		(
			SELECT [s].[CorpCompositeId],
				   CASE
					   WHEN ROW_NUMBER() OVER (PARTITION BY SecurityId, [Period] ORDER BY [Date] DESC) = 1
					   THEN 1
					   ELSE 0
				   END PeriodEndRecord
			FROM [#TempEODSnap] s
		) a
			ON s1.[CorpCompositeId] = a.[CorpCompositeId];

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'C - Setting Period Record'


	
	DROP TABLE IF EXISTS #TempSummary
	SELECT [a].[SecurityId],
		   [a].[CorpCompositeId] ReportPeriod_MuniCompositeId,
		   [a].[BidComposite] ReportPeriod_BidComposite,
		   [a].[OfferComposite] ReportPeriod_OfferComposite,
		   [a].[OfferCount] ReportPeriod_OfferCount, 
		   [a].[BidCount] ReportPeriod_BidCount,
		   a.[ProviderCount] ReportPeriod_ProviderCount,
		   --[a].[JsonDetail] ReportPeriod_JsonDetail, 
		   [b].CorpCompositeId PreviousPeriod_MuniCompositeId,
		   [b].[BidComposite] PreviousPeriod_BidComposite,
		   [b].[OfferComposite] PreviousPeriod_OfferComposite,
		   [b].[OfferCount] PreviousPeriod_OfferCount, 
		   [b].[BidCount] PreviousPeriod_BidCount,
		   b.[ProviderCount] PreviousPeriod_ProviderCount
		   --[b].[JsonDetail] PreviousPeriod_JsonDetail	   
	INTO #TempSummary
	FROM 
	(
		SELECT CorpCompositeId, [SecurityId], [BidComposite], [OfferComposite], [BidCount], [OfferCount],[ProviderCount]
		FROM [#TempEODSnap] es		
		WHERE PeriodRecord=1
		AND [Period]=1
	) a
	CROSS APPLY
	(
		SELECT s1.CorpCompositeId, s1.[BidComposite], s1.[OfferComposite], [BidCount], [OfferCount],[ProviderCount]
		FROM [#TempEODSnap] s1
		WHERE s1.SecurityId=a.[SecurityId]
		AND [s1].[PeriodRecord]=1
		AND [s1].[Period]=2
	) b

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'D - Created Summary'
		
	DROP TABLE IF EXISTS #TempSecurityInfo
	SELECT rs.SecurityId,
		   rs.[CorpIssuerTicker] [Ticker],
		   [SecurityName],		   
			b.[RatingOrder], 
			b.[Rating],	
			rs.[CorpIssuerSectorName] [Sector],
			COALESCE(
			CASE WHEN b.[Rating] IS NOT NULL THEN 
				 CASE WHEN b.[RatingOrder] <=10	THEN 'IG' ELSE 'HY' END
				 ELSE rs.[CorpIssuerSectorName]
			END, 'HY') [DerivedSector],
		   rs.[CorpIssueDate] [IssueDate],
		   rs.[PrimaryIdentifier] [Identifier],
			CASE WHEN sc.WAC IS NOT NULL THEN sc.WAC
				 WHEN yc.CurveValue IS NOT NULL THEN 
					 CASE WHEN sc.[coupon_floor] IS NOT NULL
							  AND (yc.CurveValue+(sc.[floating_spread]/100))<sc.[coupon_floor]
						  THEN sc.[coupon_floor]
						  WHEN yc.[CurveValue]+(sc.[floating_spread]/100) <0 THEN 0
						  ELSE yc.[CurveValue]+(sc.[floating_spread]/100)
					   END
				 ELSE sc.[coupon]
			END [Coupon],		   
		   sc.[maturity_date] [MaturityDate], 
		   rs.[CorpIssuerMoodysIndustryName] [MoodysIndustry]
	INTO #TempSecurityInfo
	FROM [SolveSM].dbo.[rSecurity] rs
	JOIN (SELECT DISTINCT SecurityId FROM [#TempEODSnap]) a ON [a].[SecurityId] = [rs].[SecurityId]
	JOIN SolveSm.dbo.[SecurityCorp] sc ON rs.[SecurityId]=sc.[security_id]
	LEFT JOIN [SolveSM].dbo.[Code] flt_idx ON sc.[floating_index]=[flt_idx].[code_id]
	LEFT JOIN solvesm.dbo.[YieldCurve] yc ON [yc].[SectorType] = [rs].[SectorType] AND yc.[CurveType]='FloatingIndex' AND yc.FloatingIndexId=flt_idx.[code_id]		
	OUTER APPLY
	(
		SELECT mr.[MaxOrder] [RatingOrder], conv_rating.[code_title] [Rating]
		FROM 
		(
			SELECT 
						 MAX(CASE  WHEN c1.print_order IS NOT NULL 
								   AND (c1.[print_order] >= c2.[print_order] OR c2.[print_order] is null) 
								   AND (c1.[print_order] >= c3.[print_order] OR c3.[print_order] is null) 
								   AND (c1.[print_order] >= c4.[print_order] OR c4.[print_order] is null)
								THEN c1.[print_order]
								WHEN c2.print_order IS NOT NULL 
								   AND (c2.[print_order] >= c1.[print_order] OR c1.[print_order] is null) 
								   AND (c2.[print_order] >= c3.[print_order] OR c3.[print_order] is null) 
								   AND (c2.[print_order] >= c4.[print_order] OR c4.[print_order] is null)
							   THEN c2.[print_order]
							   WHEN c3.print_order IS NOT NULL 
								   AND (c3.[print_order] >= c1.[print_order] OR c1.[print_order] is null) 
								   AND (c3.[print_order] >= c2.[print_order] OR c2.[print_order] is null) 
								   AND (c3.[print_order] >= c4.[print_order] OR c4.[print_order] is null)
							   THEN c3.[print_order]
							   ELSE c4.[print_order]
							   END) [MaxOrder]  
			FROM SolveSm.dbo.[CorpRating] cr 
			LEFT JOIN SolveSm.dbo.Code c1 ON cr.[SpRating]=c1.[code_id] AND c1.[print_order]<=19
			LEFT JOIN SolveSm.dbo.Code c2 ON cr.[MoodysRating]=c2.[code_id] AND c2.[print_order] <=20
			LEFT JOIN [SolveSM].dbo.[Code] c3 ON cr.[FitchRating]=c3.[code_id] AND c3.[print_order]<=20
			LEFT JOIN [SolveSM].dbo.[Code] c4 ON cr.[OtherRating]=c4.[code_id] AND c4.[print_order]<=20
			--LEFT JOIN solvesm.dbo.code conv_moodys ON c2.[print_order]=[conv_moodys].[print_order] AND conv_moodys.[code_group]='rating_sp' AND c2.code_title NOT IN ('WR', 'NR')	
			WHERE cr.[CorpIssuerId]=sc.[corp_issuer_id]
			AND cr.[PaymentRank]=sc.[payment_rank]	
		) mr
		LEFT JOIN solvesm.dbo.code conv_rating ON mr.[MaxOrder]=conv_rating.[print_order] AND conv_rating.[code_group]='rating_fitch'
	) b
	WHERE ISNULL(sc.[is_conv],0)=0

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'E - Getting Security Data'

	DELETE t 
	FROM [#TempEODSnap] t
	JOIN [#TempSecurityInfo] s ON [s].[SecurityId] = [t].[SecurityId]
	WHERE s.[DerivedSector]!=@Sector

	DELETE t 
	FROM [#TempSummary] t
	JOIN [#TempSecurityInfo] s ON [s].[SecurityId] = [t].[SecurityId]
	WHERE s.[DerivedSector]!=@Sector

	DELETE s FROM [#TempSecurityInfo] s WHERE s.DerivedSector !=@Sector
	
	--Generating TOP 300 Winners	
	DROP TABLE IF EXISTS #TempWinners
	SELECT TOP 300 
			ROW_NUMBER() OVER (ORDER BY [ts].[ReportPeriod_BidComposite]/[ts].[PreviousPeriod_BidComposite] DESC) Ord, 
			s.SecurityId, 
			[s].[Identifier],
			s.Ticker,
			SecurityName, 
			s.Coupon,
			s.MaturityDate,
			[ts].[ReportPeriod_BidCount] Dealers, 
			([ts].[ReportPeriod_BidComposite]/[ts].[PreviousPeriod_BidComposite])-1 [PercentageChanged], 
			[ts].[ReportPeriod_BidComposite] [ReportBidComposite], 
			[ts].[PreviousPeriod_BidComposite] [PreviousBidComposite]
	INTO #TempWinners
	FROM [#TempSummary] ts
	JOIN [#TempSecurityInfo] s ON ts.SecurityId=s.SecurityId
	WHERE ([ts].[ReportPeriod_BidComposite] IS NOT NULL AND ts.[PreviousPeriod_BidComposite] > 0) AND [ts].[ReportPeriod_BidCount]>=2
	ORDER BY Ord

	--d.rodriguez testing for market summary automation
	-- drop table if exists ##TempWinners_dev;
	-- SELECT * 
	-- INTO ##TempWinners_dev
	-- FROM #TempWinners

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'F - Building Top 300 Winners'
		
	DROP TABLE IF EXISTS #TempLosers
	SELECT TOP 300 
			ROW_NUMBER() OVER (ORDER BY ReportPeriod_BidComposite/PreviousPeriod_BidComposite) Ord, 
			s.SecurityId, 
			[s].[Identifier],
			s.Ticker,
			SecurityName, 
			s.Coupon,
			s.MaturityDate,
			[ts].[ReportPeriod_BidCount] Dealers, 
			([ts].[ReportPeriod_BidComposite]/[ts].[PreviousPeriod_BidComposite])-1 [PercentageChanged], 
			[ts].[ReportPeriod_BidComposite] [ReportBidComposite], 
			[ts].[PreviousPeriod_BidComposite] [PreviousBidComposite]
	INTO #TempLosers
	FROM [#TempSummary] ts
	JOIN [#TempSecurityInfo] s ON ts.SecurityId=s.SecurityId
	WHERE ([ts].[ReportPeriod_BidComposite] IS NOT NULL AND ts.[PreviousPeriod_BidComposite] >0) AND [ts].[ReportPeriod_BidCount]>=2
	ORDER BY Ord

	--d.rodriguez testing for market summary automation
	-- drop table if exists ##TempLosers_dev;
	-- SELECT * 
	-- INTO ##TempLosers_dev
	-- FROM #TempLosers

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'G - Building Top 300 Losers'

	DROP TABLE IF EXISTS #TempObservedPrices
	SELECT [a].[SecurityId], [a].[Date], a.[Period], SUM([a].[Value]*[a].[SumWeights]) / SUM(a.[SumWeights]) WeightedPrice
	INTO #TempObservedPrices
	FROM 
	(
		SELECT s.[CorpCompositeId],
			   s.[Date],
			   s.[Period],
			   s.SecurityId,
			   j.[AggregateColorId],
			   j.[Provider],
			   j.[QuoteType],
			   CONVERT(FLOAT,j.[Value]) [Value],
			   SUM(CONVERT(FLOAT, jw.[WeightValue])) SumWeights	
		FROM [#TempEODSnap] s
		JOIN
		(
			SELECT SecurityId
			FROM [#TempWinners]
			UNION ALL
			SELECT [SecurityId]
			FROM [#TempLosers]
		) a ON [a].[SecurityId] = [s].[SecurityId]
		JOIN [SolveComposite].[dbo].[CorpCompositeDebug] d ON s.[CorpCompositeId]=d.[CorpCompositeID]
		CROSS APPLY
		OPENJSON(d.[DebugData], N'$.BdWts')
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
				 s.[CorpCompositeId],
				 s.[Date],
				 j.[AggregateColorId],
				 j.[Provider],
				 j.[QuoteType],
				 j.[Value], 
				 s.[Period]
	) a
	GROUP BY [a].[SecurityId], [a].[Date], [a].[Period]


	--d.rodriguez testing for market summary automation
	drop table if exists ##TempObservedPrices_dev;
	SELECT * 
	INTO ##TempObservedPrices_dev
	FROM #TempObservedPrices
	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'H.1 - Building Common Providers 1'



	--TOP 200 Winners & Losers
    drop table if exists ##TempWinners_dev;
	SELECT tw.*,	 
			    CASE 
					WHEN cp.[WeightedPrice] >0 AND pp.[WeightedPrice]>0 
					THEN CONVERT(DECIMAL(20, 6),  (cp.[WeightedPrice]/pp.[WeightedPrice])-1 )
					ELSE NULL 
				END [PxOnlyPercentageChanged],
				cp.[WeightedPrice] [ReportPxOnlyComposite], 
	            pp.WeightedPrice [PreviousPxOnlyComposite] 
    INTO ##TempWinners_dev
	FROM [#TempWinners] tw
	OUTER APPLY 
	( 
		SELECT TOP 1 t3.[WeightedPrice]
		FROM #TempObservedPrices t3 
        WHERE 	t3.[SecurityId]=tw.[SecurityId]
		AND t3.[Period]=2
		ORDER BY t3.[Date] DESC        
	) pp
	OUTER APPLY 
	( 
		SELECT TOP 1 t3.[WeightedPrice]
		FROM #TempObservedPrices t3 
        WHERE 	t3.[SecurityId]=tw.[SecurityId]
		AND t3.[Period]=1
		ORDER BY t3.[Date] DESC        
	) cp
	ORDER BY Ord

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'I- Displaying Top 200 Winners'

    drop table if exists ##TempLosers_dev;
	SELECT tl.*,	 
			    CASE 
					WHEN cp.[WeightedPrice] >0 AND pp.[WeightedPrice]>0 
					THEN CONVERT(DECIMAL(20, 6), (cp.[WeightedPrice]/pp.[WeightedPrice])-1)
					ELSE NULL 
				END [PxOnlyPercentageChanged],
				cp.[WeightedPrice] [ReportPxOnlyComposite], 
	             pp.WeightedPrice [PreviousPxOnlyComposite] 
	INTO ##TempLosers_dev
    FROM #TempLosers tl
	OUTER APPLY 
	( 
		SELECT TOP 1 t3.[WeightedPrice]
		FROM #TempObservedPrices t3 
        WHERE 	t3.[SecurityId]=tl.[SecurityId]
		AND t3.[Period]=2
		ORDER BY t3.[Date] DESC        
	) pp
	OUTER APPLY 
	( 
		SELECT TOP 1 t3.[WeightedPrice]
		FROM #TempObservedPrices t3 
        WHERE 	t3.[SecurityId]=tl.[SecurityId]
		AND t3.[Period]=1
		ORDER BY t3.[Date] DESC        
	) cp
	ORDER BY Ord

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'J - Displaying Top 200 Losers'
	
	IF @Sector='HY'
	BEGIN

		DECLARE @PivotDates VARCHAR(MAX) = '', @Sql VARCHAR(MAX)=''

		DROP TABLE IF EXISTS #TempAvgPriceByIndustry;
		SELECT a.[MoodysIndustry],
			   a.Date,
			   a.AvgPrice, 
			   a.Ord 
		INTO #TempAvgPriceByIndustry
		FROM 
		(
			SELECT sm.[MoodysIndustry], CONVERT(DATE, [Date]) [Date],  AVG([ts].[BidComposite]) AvgPrice, 1 Ord 
			FROM [#TempEODSnap] ts 
			JOIN [#TempSecurityInfo] sm on ts.[SecurityId]=sm.[SecurityId]
			WHERE ts.[Period]<=2
			GROUP BY CONVERT(DATE, [Date]), sm.[MoodysIndustry]
			UNION ALL
			SELECT 'All Industries', CONVERT(DATE, [Date]) [Date],  AVG([ts].[BidComposite]) AvgPrice, 0 Ord
			FROM [#TempEODSnap] ts 
			JOIN [#TempSecurityInfo] sm on ts.[SecurityId]=sm.[SecurityId]
			WHERE ts.[Period]<=2
			GROUP BY CONVERT(DATE, [Date])
		) a
		ORDER BY Ord, [a].[MoodysIndustry], [Date]

	
		SELECT @PivotDates = @PivotDates + '[' + CONVERT(VARCHAR, [Date], 1)+'],' 
		FROM 
		(
			SELECT DISTINCT [Date]
			FROM #TempAvgPriceByIndustry
		) a
		ORDER BY [Date]	

		SELECT @PivotDates = LEFT(@PivotDates, LEN(@PivotDates)-1)

		drop table if exists ##TempAvgPriceByIndustry_dev;
		SELECT @Sql = '
		
			SELECT [MoodysIndustry], 
			' + @PivotDates + '
			into ##TempAvgPriceByIndustry_dev
			FROM
			(SELECT [MoodysIndustry], [Date], [AvgPrice], Ord
			FROM #TempAvgPriceByIndustry) st
			PIVOT
			(
				MAX([AvgPrice])
				FOR [Date] IN (' + @PivotDates + ')
			) As PivotTable
			ORDER BY Ord, [MoodysIndustry];		
		'
		EXECUTE (@Sql)
	END

	----EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'K - Displaying Avg Price By State'

	--Sentiment
	drop table if exists ##TempSentiment_dev;
	SELECT [Date], 
			CONVERT(DECIMAL(28,10), SUM(CASE WHEN IsDeclining=1 THEN 1 ELSE 0 END)) / CONVERT(DECIMAL(28, 10), COUNT(*)) [Decliners], 
			CONVERT(DECIMAL(28,10), SUM(CASE WHEN IsDeclining=0 THEN 1 ELSE 0 END)) / CONVERT(DECIMAL(28, 10), COUNT(*)) [Advancers], 
			CONVERT(DECIMAL(28,10), SUM(CASE WHEN IsDeclining IS NULL THEN 1 ELSE 0 END)) / CONVERT(DECIMAL(28, 10), COUNT(*)) [Flats], 
			SUM(CASE WHEN IsDeclining=1 THEN 1 ELSE 0 END) [DeclinersCount], 
			SUM(CASE WHEN IsDeclining=0 THEN 1 ELSE 0 END) [AdvancersCount],
			SUM(CASE WHEN IsDeclining IS NULL THEN 1 ELSE 0 END) [FlatsCount]

			--d.rodriguez
			INTO ##TempSentiment_dev
	FROM 
	(
		SELECT CASE WHEN e3.[BidComposite] > e1.[BidComposite] THEN 1 WHEN e3.[BidComposite] < e1.[BidComposite] THEN 0 ELSE NULL END IsDeclining,
				CONVERT(DATE, e1.[Date]) [Date]
		FROM [#TempEODSnap] e1
		CROSS APPLY(SELECT TOP 1 * FROM [#TempEODSnap] e2 WHERE e1.SecurityId=e2.SecurityId AND e2.[Date] < e1.[Date] ORDER BY [Date] DESC) e3		
	) a
	GROUP BY [Date]
	ORDER BY [Date]
		


	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'L - Displaying Sentiment'

	--Top Quote Volume Movers
	drop table if exists ##TempTopQuoteVolume_dev;
	SELECT TOP 50 
			ROW_NUMBER() OVER(ORDER BY a.TotalQuotes-b.TotalQuotes DESC) [Rank],
			a.SecurityId,
			[a].[Identifier],
			SecurityName,
			Coupon,
			MaturityDate,
			b.TotalQuotes TotalQuotes_LookBack, 
			a.TotalQuotes TotalQuotes_Report, 
			a.TotalQuotes - b.TotalQuotes [Increase], 
			CASE WHEN ISNULL(b.TotalQuotes, 0) > 0 
					THEN CONVERT(DECIMAL(28, 10), a.TotalQuotes)/CONVERT(DECIMAL(28, 10), b.TotalQuotes)-1
					ELSE 0 
			END [PercentageIncrease]	
			--d.rodriguez
			INTO ##TempTopQuoteVolume_dev
	FROM 
	(
		SELECT	sm.SecurityId,
				sm.[Identifier],
				sm.SecurityName,
				sm.Coupon,
				sm.MaturityDate,
				SUM(ISNULL(BidCount, 0) + ISNULL(OfferCount, 0)) TotalQuotes
		FROM [#TempEODSnap] s
		JOIN [#TempSecurityInfo] sm ON s.SecurityId = sm.SecurityId
		WHERE [Period] = 1
		AND sm.[IssueDate]<=@previousPeriodStartDate
		GROUP BY sm.SecurityId,
				 sm.[Identifier],
				 sm.SecurityName, 
				 sm.Coupon, 
				 sm.MaturityDate
	) a
	JOIN 
	(
		SELECT sm.SecurityId,
			   SUM(ISNULL(BidCount, 0) + ISNULL(OfferCount, 0)) TotalQuotes
		FROM [#TempEODSnap] s
		JOIN [#TempSecurityInfo] sm ON s.SecurityId = sm.SecurityId
		WHERE [Period] = 2
		AND sm.[IssueDate]<=@previousPeriodStartDate
		GROUP BY sm.SecurityId,
				 sm.SecurityName
	) b ON a.SecurityId=b.SecurityId;

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'M - Volume Movers'
	
	--Most Quoted
    drop table if exists ##TempMostQuoted_dev;
	SELECT TOP 50 
			sm.SecurityId,
			sm.[Identifier],
			sm.SecurityName,			
			sm.Coupon,
			sm.MaturityDate,
			s.[ReportPeriod_ProviderCount] [Dealers]

			--d.rodriguez
			INTO ##TempMostQuoted_dev

	FROM [#TempSummary]  s 
	JOIN [#TempSecurityInfo] sm ON s.SecurityId=sm.SecurityId	
	ORDER BY Dealers DESC

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'N - Most Quoted'

	
		--TABLE 6 - Sector - Bid-Offer Spread
		
	DROP TABLE IF EXISTS #TempBOSpread
	SELECT * 
	INTO #TempBOSpread
	FROM 
	(
		SELECT	ISNULL(sm.[MoodysIndustry], '') MoodysIndustry, 
				CONVERT(date, [Date]) [Date], 
				AVG(COALESCE(ts.[OriginalOfferComposite],[ts].[OfferComposite])-COALESCE(ts.[OriginalBidComposite],[ts].[BidComposite])) AvgSpread,
				1 Ord 
		FROM [#TempEODSnap] ts 
		JOIN [#TempSecurityInfo] sm on ts.SecurityID = sm.SecurityId
		WHERE ts.[Period]<=2
		AND COALESCE(ts.[OriginalOfferComposite],[ts].[OfferComposite]) > COALESCE(ts.[OriginalBidComposite],[ts].[BidComposite])
		GROUP BY CONVERT(date, [Date]), sm.[MoodysIndustry]
		UNION ALL
		SELECT 'All Industries' MoodysIndustry, CONVERT(date, [Date]) [Date], AVG(COALESCE(ts.[OriginalOfferComposite],[ts].[OfferComposite])-COALESCE(ts.[OriginalBidComposite],[ts].[BidComposite])) AvgSpread, 0 Ord 
		FROM [#TempEODSnap] ts 		
		JOIN [#TempSecurityInfo] sm on ts.SecurityID = sm.SecurityId	
		WHERE ts.[Period]<=2
		AND COALESCE(ts.[OriginalOfferComposite],[ts].[OfferComposite]) > COALESCE(ts.[OriginalBidComposite],[ts].[BidComposite])
		GROUP BY CONVERT(date, [Date])
	) a
	ORDER BY Ord, [a].MoodysIndustry, [Date]

	--d.rodriguez

	SELECT @PivotDates = '', @Sql =''
	
	SELECT @PivotDates = @PivotDates + '[' + CONVERT(VARCHAR, [Date], 1)+'],' 
	FROM 
	(
		SELECT DISTINCT [Date]
		FROM #TempBOSpread
	) a
	ORDER BY [Date]

	SELECT @PivotDates = LEFT(@PivotDates, LEN(@PivotDates)-1)
    
    drop table if exists ##TempBOSpread_dev;
	SELECT @Sql = '
		
		SELECT [MoodysIndustry], 
		' + @PivotDates + ' 
        INTO ##TempBOSpread_dev
		FROM
		(SELECT [MoodysIndustry], [Date], [AvgSpread], Ord
		FROM #TempBOSpread) st
		PIVOT
		(
			MAX([AvgSpread])
			FOR [Date] IN (' + @PivotDates + ')
		) As PivotTable
		ORDER BY Ord, [MoodysIndustry];		
	'
	EXECUTE (@Sql)


	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'O - B/O Spread'
			
	--Bid And Offer Volume	
	DROP TABLE IF EXISTS #TempBidOfferVolume;
	SELECT MoodysIndustry, [Date], [Side], QuoteCount, [Ord] 
	INTO #TempBidOfferVolume
	FROM 
	(
		SELECT  ISNULL(rs.MoodysIndustry,'')[MoodysIndustry], [Date], 'BID' [Side], SUM([BidCountByMessage]) [QuoteCount], 1 Ord
		FROM [SolveReporting].dbo.[SecurityStats] ss
		JOIN [#TempSecurityInfo] rs ON [rs].[SecurityId] = [ss].[SecurityId]
		WHERE ss.[SectorType]='CORP' AND ss.[IsAggregate]=1 AND ss.[Date]>=@startDate AND CONVERT(DATE, ss.[Date])<=@ReportDate
		GROUP BY ISNULL(rs.MoodysIndustry,''), [Date]
		UNION ALL
		SELECT  ISNULL(rs.MoodysIndustry,'') [MoodysIndustry], [Date], 'OFFER' [Side], SUM([ss].[OfferCountByMessage]) [QuoteCount], 1 Ord
		FROM [SolveReporting].dbo.[SecurityStats] ss
		JOIN [#TempSecurityInfo] rs ON [rs].[SecurityId] = [ss].[SecurityId]
		WHERE ss.[SectorType]='CORP' AND ss.[IsAggregate]=1 AND ss.[Date]>=@startDate AND CONVERT(DATE, ss.[Date])<=@ReportDate
		GROUP BY ISNULL(rs.MoodysIndustry,''), [Date]
		UNION ALL
		SELECT  'All Industries' [MoodysIndustry], [Date], 'BID' [Side], SUM([BidCountByMessage]) [QuoteCount], 0 Ord
		FROM [SolveReporting].dbo.[SecurityStats] ss
		JOIN [#TempSecurityInfo] rs ON [rs].[SecurityId] = [ss].[SecurityId]
		WHERE ss.[SectorType]='CORP' AND ss.[IsAggregate]=1 AND ss.[Date]>=@startDate AND CONVERT(DATE, ss.[Date])<=@ReportDate
		GROUP BY [Date]
		UNION ALL
		SELECT  'All Industries' [MoodysIndustry], [Date], 'OFFER' [Side], SUM([ss].[OfferCountByMessage]) [QuoteCount], 0 Ord
		FROM [SolveReporting].dbo.[SecurityStats] ss
		JOIN [#TempSecurityInfo] rs ON [rs].[SecurityId] = [ss].[SecurityId]
		WHERE ss.[SectorType]='CORP' AND ss.[IsAggregate]=1 AND ss.[Date]>=@startDate AND CONVERT(DATE, ss.[Date])<=@ReportDate
		GROUP BY [Date]
	) a
	WHERE DATEPART(WEEKDAY, [Date]) NOT IN (1,7)
	ORDER BY Ord, MoodysIndustry,[Date], [Side]
	
	--d.rodriguez
	drop table if exists ##TempBidOfferVolume_dev;
	SELECT *
	INTO ##TempBidOfferVolume_dev
	FROM #TempBidOfferVolume
----------------------------------------
	SELECT @PivotDates = '', @Sql =''
	
	SELECT @PivotDates = @PivotDates + '[' + CONVERT(VARCHAR, [Date], 1)+'],' 
	FROM 
	(
		SELECT DISTINCT [Date]
		FROM #TempBidOfferVolume
	) a
	ORDER BY [Date]	

	SELECT @PivotDates = LEFT(@PivotDates, LEN(@PivotDates)-1)

	drop table if exists ##TempMoodyIndustry_dev;
	SELECT @Sql = '
		
		SELECT [MoodysIndustry], 
		[Side],
		' + @PivotDates + '--d.rodriguez
		INTO ##TempMoodyIndustry_dev
		FROM
		(SELECT [MoodysIndustry], [Date], [Side], [QuoteCount], Ord
		FROM #TempBidOfferVolume) st
		PIVOT
		(
			MAX([QuoteCount])
			FOR [Date] IN (' + @PivotDates + ')
		) As PivotTable
		ORDER BY Ord, [MoodysIndustry], [Side]
		;
				
	'
	EXECUTE (@Sql)

	--EXECUTE [SolveMarketData].[dbo].[spPrintNow] N'P - B/O Volume'


	--SELECT * 
	--FROM [#TempSecurityInfo]

	DECLARE @DealerCountFieldsSql VARCHAR(MAX)='', 
			@BidCountFieldsSql VARCHAR(MAX) = '', 
			@OfferCountFieldsSql VARCHAR(MAX) = '',
			@BidCompositeFieldsSql VARCHAR(MAX) = '', 
			@OfferCompositeFieldsSql VARCHAR(MAX) = '',
			@OuterAppliesSql VARCHAR(MAX)=''

	SELECT 
		   @BidCompositeFieldsSql+='tbl_' + CONVERT(VARCHAR, [Date], 112) + '.BidComposite [B_CMP_' + CONVERT(VARCHAR, [Date], 1) + '],',
		   @OfferCompositeFieldsSql+='tbl_' + CONVERT(VARCHAR, [Date], 112) + '.OfferComposite [O_CMP_' + CONVERT(VARCHAR, [Date], 1) + '],',
		   @DealerCountFieldsSql+='tbl_' + CONVERT(VARCHAR, [Date], 112) + '.ProviderCount [DL_CT_' + CONVERT(VARCHAR, [Date], 1) + '],', 
		   @BidCountFieldsSql+='tbl_' + CONVERT(VARCHAR, [Date], 112) + '.BidCount [B_CT_' + CONVERT(VARCHAR, [Date], 1) + '],', 
		   @OfferCountFieldsSql+='tbl_' + CONVERT(VARCHAR, [Date], 112) + '.OfferCount [O_CT_' + CONVERT(VARCHAR, [Date], 1) + '],', 
		   @OuterAppliesSql+=' OUTER APPLY (SELECT TOP 1 * FROM [#TempEODSnap] es_' + CONVERT(VARCHAR, [Date], 112) + ' WHERE es_' + CONVERT(VARCHAR, [Date], 112) + '.SecurityId=si.SecurityId AND CONVERT(DATE, es_' + CONVERT(VARCHAR, [Date], 112) + '.[Date])=''' + CONVERT(VARCHAR, [Date], 101) + ''') tbl_' + CONVERT(VARCHAR, [Date], 112) 		  
	FROM 
	(
		SELECT CONVERT(DATE, [Date]) [Date]
		FROM [#TempEODSnap]
		GROUP BY CONVERT(DATE, [Date])
	) a
	ORDER BY [Date]

	--SELECT * 
	--FROM [#TempEODSnap]
	--WHERE [SecurityId] IN (9278277,
	--		9278013)
	--ORDER BY [SecurityId], [Date]

	SELECT @OfferCountFieldsSql=LEFT(@OfferCountFieldsSql, LEN(@OfferCountFieldsSql)-1);
	
	drop table if exists ##TempRawData;
	SELECT @Sql='
		SELECT si.SecurityId, si.Identifier, si.SecurityName, si.MoodysIndustry, si.Coupon, si.Rating, 
			  ' + @BidCompositeFieldsSql+ '
			  ' + @OfferCompositeFieldsSql+ '
			  ' + @DealerCountFieldsSql+ '
			  ' + @BidCountFieldsSql+ '
			  ' + @OfferCountFieldsSql+ '
		INTO ##TempRawData
		FROM #TempSecurityInfo si 		
		' + @OuterAppliesSql + '
	';
--	PRINT @Sql
	EXECUTE(@Sql);
	
	IF(@Sector='IG')
	BEGIN	
		EXECUTE SolveComposite.dbo.spCorpCompositeReport_AvgYields @startDate, @ReportDate, @previousPeriodEndDate, NULL, @Sector
	END

--  --------------------------------------------------------
    drop table if exists #TempEODSnap;
    drop table if exists #TempSummary;
    drop table if exists #TempSecurityInfo;
    drop table if exists #TempWinners;
    drop table if exists #TempLosers;
    drop table if exists #TempObservedPrices;
    drop table if exists #TempAvgPriceByIndustry;
    drop table if exists #TempBOSpread;
    drop table if exists #TempBidOfferVolume;
