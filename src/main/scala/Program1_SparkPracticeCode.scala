import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Program1_SparkPracticeCode {
  def main(args : Array[String]) : Unit = {
    /*
      Spark computations are expressed as operations.
      These operations are then converted into low-level RDD-based bytecode as tasks,
      which are distributed to Spark’s executors for execution.
      Every computation expressed in high-level Structured APIs is decomposed into low-level
      optimized and generated RDD operations and then converted into Scala bytecode for the
      executors’ JVMs. This generated RDD operation code is not accessible to users,
      nor is it the same as the user-facing RDD APIs.
    */
    val spark = SparkSession
      .builder()
      .config("spark.master","local")
      .appName("Spark Practice")
      .getOrCreate()

    //setting the log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    /*
      In the below lines of code
      `1. We are reading a text file as a dataframe
       2. The show(10,false) operation on dataframe displays the first 10 lines without truncating
          By default the truncate Boolean flag is set to true
    */
    val strings = spark.read.text("/Users/dharani-kumar/Downloads/spark-3.4.0-bin-hadoop3/README.md")
    strings.show(10, truncate = false)

    //Filtering for the lines that contains the work Spark in them
    val filtered = strings.filter(col("value").contains("Spark"))
    filtered.show(10,truncate = false)
    println(s"The total number of lines in the file that contains Spark is ${filtered.count()}")
  }
}
