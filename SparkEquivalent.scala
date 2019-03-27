//This is work in progress code
val tracks:DataFrame=spark.table("raw_tracks")
val artist:DataFrame=spark.table("raw_artist")
val genres:DataFrame=spark.table("raw_genres")

tracks
.select("track_id")
.withColumn("static_reporting_id",lit(1))
