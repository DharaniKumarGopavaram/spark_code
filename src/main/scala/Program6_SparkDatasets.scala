import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

case class DeviceIOData(
                         device_id : Long,
                         device_name : String,
                         ip : String,
                         cca2 : String,
                         cca3 : String,
                         cn : String,
                         latitude : Double,
                         longitude : Double,
                         scale : String,
                         temp : Long,
                         humidity : Long,
                         battery_level : Long,
                         c02_level : Long,
                         lcd : String,
                         timestamp : Long
                       )

case class DeviceTempByCountry(temp : Long, device_name : String,device_id : Long,cca3 : String)

object Program6_SparkDatasets {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master","local")
      .appName("Spark DataSets")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    /*
      Conceptually, you can think of a DataFrame in Scala as an alias for a collection of generic objects,
      Dataset[Row], where a Row is a generic untyped JVM object that may hold different types of fields.
      A Dataset, by contrast, is a collection of strongly typed JVM objects in Scala or a class in Java.
      Or, as the Dataset documentation puts it, a Dataset is:
          a strongly typed collection of domain-specific objects that can be transformed in parallel using
          functional or relational operations. Each Dataset [in Scala] also has an untyped view called a
          DataFrame, which is a Dataset of Row.
      In Spark’s supported languages, Datasets make sense only in Java and Scala, whereas in Python and R
      only DataFrames make sense. This is because Python and R are not compile-time type-safe;
      types are dynamically inferred or assigned during execution, not during compile time.
      The reverse is true in Scala and Java: types are bound to variables and objects at compile time.
      Row is a generic object type in Spark, holding a collection of mixed types that can be accessed
      using an index. Internally, Spark manipulates Row objects, converting them to the equivalent types
    */

    val row = Row(350,true,"Learning Spark")
    println(s"The row first element is :- ${row.getInt(0)}")
    println(s"The row second element is :- ${row.getBoolean(1)}")
    println(s"The row third element is :- ${row.getString(2)}")

    //By contrast, typed objects are actual Java or Scala class objects in the JVM.
    /*
      Creating datasets :-
      As with creating DataFrames from data sources, when creating a Dataset you have to know the schema.
      In other words, you need to know the data types.
      When creating a Dataset in Scala, the easiest way to specify the schema for the resulting Dataset
      is to use a case class.
      Once we defined our case class we can use it to read our file and convert the returned Dataset[Row]
      into Dataset[DeviceIOData]
    */

    import spark.implicits._
    val deviceDS = spark.read.json("/Users/dharani-kumar/Desktop/spark_programs/data/iot_devices.json")
      .as[DeviceIOData]
    deviceDS.show(10, truncate = false)

    //Dataset Operations
    val filterTempDS = deviceDS.filter(d => d.temp > 30 && d.humidity > 70)
    filterTempDS.show(5,truncate = false)

    /*
    In this query, we used a function as an argument to the Dataset method filter().
    This is an overloaded method with many signatures. The version we used,
    filter(func: (T) > Boolean): Dataset[T], takes a lambda function, func: (T) > Boolean, as its argument.
    The argument to the lambda function is a JVM object of type DeviceIoData.
    As such, we can access its individual data fields using the dot (.) notation, like you would in a
    Scala class or JavaBean.
    Another thing to note is that with DataFrames, you express your filter() conditions as SQL-like DSL
    operations, which are language-agnostic.
    With Datasets, we use language-native expressions as Scala or Java code.
    */

    val dsTemp = deviceDS.filter(d => d.temp > 30 && d.humidity > 70)
      .map(d => (d.temp,d.device_name,d.device_id,d.cca3))
      .toDF("temp","device_name","device_id","cca3")
      .as[DeviceTempByCountry]

    dsTemp.show(5,truncate = false)
    println(dsTemp.first())

    //Alternatively, you could express the same query using column names and then cast to a Dataset[DeviceTempByCountry]:
    val dsTemp2 = deviceDS.select("temp","device_name","device_id","cca3")
      .where(col("temp") > 30)
      .as[DeviceTempByCountry]

    dsTemp2.show(10, truncate = false)

    //If you want to take advantage of and benefit from Tungsten’s efficient serialization with Encoders, use Datasets.
    //If you want errors caught during compilation rather than at runtime, choose Datasets
  }
}
