import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

val conf = new SparkConf()
val sc = new SparkContext(conf)

sc.textFile("201508_station_data.csv").filter(line => !line.contains("station_id"))

case class Station(id: Int, name: String, lat: Double, lon: Double,
                   dockcount: Int, landmark: String, installation: java.sql.Date)

val stationRDD: RDD[(Int, String, Double, Double, Int, String, java.sql.Date)] =
  sc.textFile("201508_station_data.csv").filter(line => !line.contains("station_id")).
    filter(line => !line.contains("station_id")).
    map { line =>
      val dateFormat = new SimpleDateFormat("MM/dd/yyy")

      val elms = line.split(",")
      val id = elms(0).toInt
      val name = elms(1)
      val lat = elms(2).toDouble
      val lon = elms(3).toDouble
      val dockcount = elms(4).toInt
      val landmark = elms(5)
      val parsedInstallation = dateFormat.parse(elms(6))
      val installation = new java.sql.Date(parsedInstallation.getTime())
      (id, name, lat, lon, dockcount, landmark, installation)
    }

val stationDF = stationRDD.
  map { case (id, name, lat, lon, dockcount, landmark, installation) =>
    Station(id, name, lat, lon, dockcount, landmark, installation)
  }.toDF().cache()

val rdd: RDD[Row] = stationDF.rdd
