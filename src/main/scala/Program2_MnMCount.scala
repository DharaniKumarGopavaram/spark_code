import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Program2_MnMCount {
  def main(args : Array[String]) : Unit = {
      val spark = SparkSession
        .builder()
        .config("spark.master","local")
        .appName("MnM count")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      val mnmFile = "/Users/dharani-kumar/Desktop/spark_programs/data/mnm_dataset.csv"
      //Reading the file into a spark dataframe
      val mnmDF = spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load(mnmFile)

      //Aggregate count of all the colors and group by state,color and orderBy in descending order
      val countMnMDF = mnmDF.select("State","Color","Count")
        .groupBy("State","Color")
        .sum("Count")
        .orderBy(desc("sum(Count)"))

      println(countMnMDF.queryExecution.logical)

      countMnMDF.show(60, truncate = false)

      //Find the aggregate counts of california by filtering
      val caCountMnMDF = mnmDF.select("State","Color","Count")
        .where(col("State") === "CA")
        .groupBy("State","Color")
        .sum("Count")
        .orderBy(desc("sum(Count)"))

    caCountMnMDF.show(10, truncate = false)
    spark.stop()
  }
}
