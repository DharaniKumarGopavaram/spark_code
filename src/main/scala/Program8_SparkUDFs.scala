import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.IntegerType

object Program8_SparkUDFs {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master","local")
      .appName("Spark UDFs")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    /*
      User-Defined Functions :-
      While Apache Spark has a plethora of built-in functions, the flexibility of Spark allows for
      data engineers and data scientists to define their own functions too.
      These are known as user-defined functions (UDFs).
      Note that UDFs operate per session and they will not be persisted in the underlying metastore
    */

    val cubed : Long => Long = (s : Long) => s * s * s //create cubed function
    spark.udf.register("cubed",cubed) //Register UDF
    spark.range(1,11).createOrReplaceTempView("udf_test") //create a temporary view
    spark.sql("select Id,cubed(id) as Id_Cubed from udf_test").show(truncate = false)
    //calling the UDF function using dataframe
    spark.range(1,11)
      .withColumn("cubed",expr("cubed(Id)"))
      .show(truncate = false)
  }
}
