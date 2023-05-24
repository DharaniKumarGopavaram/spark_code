import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Program3_AuthorsAges {
  def main(args : Array[String]) : Unit = {
      val spark = SparkSession
        .builder()
        .config("spark.master","local")
        .appName("AuthorAges")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")
      val dataDF = spark.createDataFrame(Seq(
        ("John",34),
        ("Jane",36),
        ("John",45),
        ("Johan",50)
      )).toDF("name","age")

      dataDF
        .groupBy("name")
        .agg(avg("age"))
        .show(5,truncate = false)
  }
}
