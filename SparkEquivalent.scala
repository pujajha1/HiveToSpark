//This is work in progress code
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val tracks:DataFrame=spark.table("raw_tracks")
val artist:DataFrame=spark.table("raw_artist")
val genres:DataFrame=spark.table("raw_genres")

val rt = tracks
        .select("track_id")
        .withColumn("static_reporting_id",lit(1))
        .withColumn("epoch_new_ts",lit(new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a")
        .parse("track_date_created").getTime()/1000))//track_date_created format='11/26/2008 01:48:14 AM'
        .withColumn("Trending",when(tracks("track_listens")>20,"Super Hit")
        .otherwise("Hit"))
        .withColumn("dt",lit(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        .format(LocalDateTime.now)))
        .filter("track_date_created='11/26/2008 01:48:14 AM'")
        .filter("track_number<>track_id")
        .filter("track_explicit not in ('Radio-Unsafe')")

val ra = artist
        .withColumn("artist", trim(col("artist_name")))

val rg = genres
        .select("genre_id"
