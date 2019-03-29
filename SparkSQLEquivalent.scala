
    spark.sql(s"""
             CREATE OR REPLACE TEMPORARY VIEW  master_body_data_view
    	        AS SELECT 
                    rt.track_id,
                    count(rt.track_id) as cnt,
                    COALESCE(rt.person_listens,NULL),
                    url as track_url,
                    COALESCE(col1,0),
                    COALESCE(col2,0),
                    1 as static_reporting_id,
                    count(distinct track_listens) as customers,
                    new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(rt.track_date_created).getTime()/1000 as epoch_ts,
                    trim(ra.artist_name) as artist,
                    rg.genre_id,
                    CASE WHEN rt.track_listens > 20 
	                THEN 'Super Hit'
	                else 'Hit'
                    END as Trending,
                    from_unixtime(unix_timestamp()) AS current_time,
                    FROM  raw_tracks rt
                    JOIN raw_artist ra
                    ON(rt.track_id=ra.track_id)
                    LEFT OUTER JOIN raw_genres rg
                    ON(rt.genre_id=rg.genre_id)
                    WHERE rt.track_date_created='11/26/2008 01:48:14 AM'
                    AND rt.track_number<> rt.track_id
                    AND rt.track_explicit not in ('Radio-Unsafe')
                    GROUP BY 
                    rt.track_id
                    ORDER BY track_id,
                    col1	""")
