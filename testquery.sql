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
	
	DROP TABLE IF EXISTS #TempEODSnap;
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

	CREATE CLUSTERED INDEX IX_TempCluster ON [#TempEODSnap](SecurityId, [Period], [Date] DESC)

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

	DROP TABLE IF EXISTS #TempSummary;
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

    DROP TABLE IF EXISTS #TempSecurityInfo;
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
    ;
    	
	DROP TABLE IF EXISTS #TempLosers;
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
    ;

    /*
        Este Statement rompe el query completo... 
        Investigando por qué.
    */
    DROP TABLE IF EXISTS #TempObservedPrices;
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
    ;

/*
	--d.rodriguez testing for market summary automation
	drop table if exists ##TempObservedPrices_dev;
	SELECT * 
	INTO ##TempObservedPrices_dev
	FROM #TempObservedPrices
*/

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
    ;

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
    ;

--  --------------------------------------------------------------------

    drop table if exists ##ProgressTest;
	select top 500 * 
    into ##ProgressTest
    from #TempLosers;

--  ----------------------------------------------------------------

	drop table if exists #TempEODSnap;
    DROP TABLE IF EXISTS #TempSummary;
    drop table if exists #TempSecurityInfo;
    drop table if exists #TempLosers;
    drop table if exists #TempWinners;
    drop table if exists #TempObservedPrices;
