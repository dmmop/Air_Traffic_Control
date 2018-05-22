import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object flight_control {
	def main(args: Array[String]): Unit = {
		val APP_NAME = "Spark landing planes"
		//Spark Context
		val conf = new SparkConf()
				.setAppName(APP_NAME)
				.setMaster("local")
				.set("spark.sql.parquet.compression.codec", "snappy")

		val sc: SparkContext = new SparkContext(conf)
		sc.setLogLevel("WARN")

		//Spark Session
		val spark = SparkSession.builder()
				.appName(APP_NAME)
				.master("local")
				.config("spark.sql.parquet.compression.codec", "snappy")
				.getOrCreate()

		// Read file path from first argument
		val fileName_source = "resources/planes.csv"
		val fileName_result = "resources/landing.csv"

		val plainsDF: DataFrame = spark.read
				.format("com.databricks.spark.csv")
				.option("sep", ",")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(fileName_source)

		// File to return etl results
		val etl_exit = new PrintWriter(fileName_result)
		val log = (row: Row) => landing_log_result(row, etl_exit)

		// Create two lists separating the plains by size
		val big = new ListBuffer[Row]
		val small = new ListBuffer[Row]

		// Initialize lists with data
		big.appendAll(
			plainsDF.filter(col("Size").equalTo("BIG"))
					.orderBy(col("Passengers").desc)
					.collect()
		)
		small.appendAll(
			plainsDF.filter(col("Size").equalTo("SMALL"))
					.orderBy(col("Passengers").desc)
					.collect()
		)


		// List to order planes
		val landing = new ListBuffer[Row]

		// Land big planes
		val land_big_plane = () => {
			log(big.head)
			big.remove(0)
		}
		// Land small planes
		val land_small_plane = (pos: Int) => {
			for (o <- 0 until pos) {
				log(small.head)
				small.remove(0)
			}
		}

		/**
		  * Check if the big plane have more passengers than two small planes,
		  * then land the option which has more passengers than the other.
		  * And continue checking until there are no more big planes*/
		while (big.nonEmpty) {
			small.length match {
				case 0 => land_big_plane()
				case 1 => if (big.head.getInt(3) > small.head.getInt(3)) land_big_plane() else land_small_plane(1)
				case _ => if (big.head.getInt(3) >= small.head.getInt(3) + small(1).getInt(3)) {
					land_big_plane()
				} else land_small_plane(2)
			}
		}
		// Lastly land the remaining small planes.
		if (small.nonEmpty) land_small_plane(small.length)

		etl_exit.close()
	}

	private def landing_log_result(row: Row, writer: PrintWriter): Unit = {
		//		writer.println(row.getString(0))
		writer.println(row)
	}
}
