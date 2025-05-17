package tn.insat

import org.apache.spark.sql.SparkSession

object SparkEnergy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Chicago Energy Benchmarking - Spark Analysis")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val filePath = "hdfs://hadoop-master:9000/input/Chicago_Energy_Benchmarking_-_Covered_Buildings_20250513.csv"

    val rdd = sc.textFile(filePath)
    val header = rdd.first()
    val data = rdd
      .filter(_ != header)
      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))
      .filter(_.length >= 6)
      .map(cols => (cols(3).trim, cols(5).trim))

    val sectorCount = data.map { case (sector, _) => (sector, 1) }
                          .reduceByKey(_ + _)
    sectorCount.saveAsTextFile("hdfs://hadoop-master:9000/output_spark_sector")

    val yearCount = data.map { case (_, year) => (year, 1) }
                        .reduceByKey(_ + _)
    yearCount.map { case (k, v) => s"$k\t$v" }
             .saveAsTextFile("hdfs://hadoop-master:9000/output_spark_year")

    val combo = data.map { case (sector, year) => ((sector, year), 1) }
                    .reduceByKey(_ + _)
    combo.map { case ((s, y), c) => s"$s,$y,$c" }
         .saveAsTextFile("hdfs://hadoop-master:9000/output_spark_combined")

    spark.stop()
  }
}