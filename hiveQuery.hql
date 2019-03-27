-- Here I am trying to write a hive query where we can use maximum operations on every column
SELECT 
rt.track_id,
1 as static_reporting_id,
new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse(rt.track_date_created).getTime()/1000 as epoch_ts,
trim(ra.artist_name) as artist,
rg.genre_id,
CASE WHENrt. track_listens > 20 
	 THEN 'Super Hit'
	 else 'Hit'
END as Trending,
from_unixtime(unix_timestamp()) AS current_time,
FROM  raw_tracks rt
JOIN raw_artist ra
ON(rt.track_id=ra.track_id)
LEFT OUTER JOIN raw_genres rg
ON(rt.genre_id=rg.genre_id)
where rt.track_date_created='11/26/2008 01:48:14 AM'
and rt.track_number<> rt.track_id
and rt.track_explicit not in ('Radio-Unsafe')
GROUP BY 
rt.track_id;
