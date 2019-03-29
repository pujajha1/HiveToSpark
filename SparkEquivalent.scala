//This is work in progress code
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val tracks:DataFrame=spark.table("raw_tracks")
val artist:DataFrame=spark.table("raw_artist")
val genres:DataFrame=spark.table("raw_genres")

val rt = tracks
        .select("track_id")
         .na.fill("NULL",Seq("person_listens"))
         .withColumnRenamed("url","track_url")
         .na.fill(Map("col1" -> 0 , "col2" -> 0 ))
         .withColumn("static_reporting_id",lit(1))
         .withColumn("epoch_new_ts",lit(new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a")
         .parse("track_date_created").getTime()/1000))//track_date_created format='11/26/2008 01:48:14 AM'
         .withColumn("Trending",when(tracks("track_listens")>20,"Super Hit")
         .otherwise("Hit"))
         .withColumn("current_time",lit(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
         .format(LocalDateTime.now)))
         .filter("track_date_created='11/26/2008 01:48:14 AM'")
         .filter("track_number<>track_id")
         .filter("track_explicit not in ('Radio-Unsafe')")
         .groupBy("track_id").agg(count("*").alias("cnt_track_id"),countDistinct("track_listens").alias("customers"))

val ra = artist
        .withColumn("artist", trim(col("artist_name")))

val rg = genres
        .select("genre_id")

val rartrg=rt
           .join(ra,col("track_id")===col("track_id"),"inner")
           .join(rg,col("genre_id")===col("genre_id"),"LEFT OUTER")
           .select("track_id",
                    "static_reporting_id",
                    "epoch_new_ts",
                    "track_date_created",
                    "Trending",
                    "current_time",
                    "artist",
                    "genre_id",
                    "cnt_track_id")

rartg

    

