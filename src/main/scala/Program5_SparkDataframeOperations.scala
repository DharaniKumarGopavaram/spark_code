import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode

object Program5_SparkDataframeOperations {
  def main(args : Array[String]) : Unit = {
      /*
        Spark provides an interface, DataFrameReader, that enables you to read data into a
        DataFrame from myriad data sources in formats such as JSON, CSV, Parquet, Text, Avro,
        ORC, etc. Likewise, to write a DataFrame back to a data source in a particular format,
        Spark uses DataFrameWriter.
      */
      val spark = SparkSession
        .builder()
        .config("spark.master","local")
        .appName("Commom DataFrame operations")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      //If you don’t want to specify the schema, Spark can infer schema from a sample at a lesser cost. For example, you can use the samplingRatio option:

      val fireProgrammaticSchema = StructType(List(
        StructField("CallNumber",IntegerType,nullable = false),
        StructField("UnitID",StringType,nullable = false),
        StructField("IncidentNumber",IntegerType,nullable = false),
        StructField("CallType",StringType,nullable = false),
        StructField("CallDate",StringType,nullable = false),
        StructField("WatchDate",StringType,nullable = false),
        StructField("CallFinalDisposition",StringType,nullable = false),
        StructField("AvailableDtTm",StringType,nullable = false),
        StructField("Address",StringType,nullable = false),
        StructField("City",StringType,nullable = false),
        StructField("Zipcode",IntegerType,nullable = false),
        StructField("Battalion",StringType,nullable = false),
        StructField("StationArea",IntegerType,nullable = false),
        StructField("Box",IntegerType,nullable = false),
        StructField("OriginalPriority",IntegerType,nullable = false),
        StructField("Priority",IntegerType,nullable = false),
        StructField("FinalPriority",IntegerType,nullable = false),
        StructField("ALSUnit",BooleanType,nullable = false),
        StructField("CallTypeGroup",StringType,nullable = false),
        StructField("NumAlarms",IntegerType,nullable = false),
        StructField("UnitType",StringType,nullable = false),
        StructField("UnitSequenceInCallDispatch",IntegerType,nullable = false),
        StructField("FirePreventionDistrict",IntegerType,nullable = false),
        StructField("SupervisorDistrict",IntegerType,nullable = false),
        StructField("Neighborhood",StringType,nullable = false),
        StructField("Location",StringType,nullable = false),
        StructField("RowID",StringType,nullable = false),
        StructField("Delay",DoubleType,nullable = false)
      ))

      val sfFireFile = "/Users/dharani-kumar/Desktop/spark_programs/data/sf-fire-calls.csv"

      val sfFireProgrammaticDF = spark.read.schema(fireProgrammaticSchema)
        .option("header","true")
        .csv(sfFireFile)

      sfFireProgrammaticDF.show(20, truncate = false)

      /*
        To write the DataFrame into an external data source in your format of choice, you can use the
        DataFrameWriter interface. Like DataFrameReader, it supports multiple data sources.
        Parquet, a popular columnar format, is the default format; it uses snappy compression to compress the
        data. If the DataFrame is written as Parquet, the schema is preserved as part of the Parquet metadata.
        In this case, subsequent reads back into a DataFrame do not require you to manually supply a schema.
      */

      //saving the sfFire Data in parquet format
      //We are using SaveMode.Overwrite which will delete the directory if it exists and loads the data again
      //If we specify repartition(4) that means it will create 4 files in the final directory
      sfFireProgrammaticDF.repartition(4).write.mode(SaveMode.Overwrite).format("parquet")
        .save("/Users/dharani-kumar/Desktop/spark_programs/data/Parquet_data")

      /*
        Projections and Filters :-
        A projection in relational parlance is a way to return only the rows matching a certain relational
        condition by using filters. In Spark, projections are done with the select() method,
        while filters can be expressed using the filter() or where() method.
      */

      //filtering where CallType is not equal to "Medical Incident"
      val fewSfFireDF = sfFireProgrammaticDF
        .select("IncidentNumber","AvailableDtTm","CallType")
        .filter(col("CallType") =!= "Medical Incident")

      fewSfFireDF.show(10, truncate = false)

      //Query to know how many distinct CallTypes were recorded
      sfFireProgrammaticDF
        .select("CallType")
        .where(col("CallType").isNotNull)
        .agg(countDistinct(col("CallType")).alias("DistinctCallTypeCount"))
        .show()

      //Query to list all the distinct CallTypes
      sfFireProgrammaticDF
        .select("CallType")
        .where(col("CallType").isNotNull)
        .distinct()
        .show(truncate = false)

      /*
        Renaming, adding and dropping columns.
        Sometimes you want to rename particular columns for reasons of style or convention,
        and at other times for readability or brevity.
        Spaces in column names can be problematic, especially when you want to write or save a
        DataFrame as a Parquet file (which prohibits this).
        By specifying the desired column names in the schema with StructField, as we did, we effectively
        changed all names in the resulting DataFrame.
        Alternatively, you could selectively rename columns with the withColumnRenamed() method.
        Because DataFrame transformations are immutable, when we rename a column using withColumnRenamed()
        we get a new DataFrame while retaining the original with the old column name.
        Modifying the contents of a column or its type are common operations during data exploration.
        In some cases the data is raw or dirty, or its types are not amenable to being supplied as arguments
        to relational operators.
      */

      val newFireDF = sfFireProgrammaticDF
        .withColumnRenamed("Delay","ResponseDelayedInMinutes")

      newFireDF.select("ResponseDelayedInMinutes")
        .where(col("ResponseDelayedInMinutes") > 10)
        .show(30,truncate = false)

      //We defined the below three columns as StringType while defining the schema
      newFireDF.select("CallDate","WatchDate","AvailableDtTm").show(10,truncate = false)

      //How can we convert this columns to_date and to_timestamp
      val fireTsDF = newFireDF.withColumn("IncidentDate",to_timestamp(col("CallDate"),"MM/dd/yyyy"))
        .drop("CallDate") //after converting we are dropping the old column
        .withColumn("OnWatchDate",to_timestamp(col("WatchDate"),"MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTs",to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm")

      fireTsDF.select("IncidentDate","OnWatchDate","AvailableDtTs")
        .show(20, truncate = false)

      //Now that we have modified the dates we can perform data operations like below
      fireTsDF.select(year(col("IncidentDate")).alias("DistinctYears"))
        .distinct()
        .orderBy("DistinctYears")
        .show()

      //What were most common types of fire calls
      fireTsDF.select("CallType")
        .where(col("CallType").isNotNull)
        .groupBy("CallType")
        .count()
        .orderBy(desc("count"))
        .show(20,truncate = false)

      /*
        The DataFrame API also offers the collect() method, but for extremely large DataFrames this is
        resource-heavy (expensive) and dangerous, as it can cause out-of-memory (OOM) exceptions.
        Unlike count(), which returns a single number to the driver, collect() returns a collection of all
        the Row objects in the entire DataFrame or Dataset.
        If you want to take a peek at some Row records you’re better off with take(n), which will return
        only the first n Row objects of the DataFrame.
      */

      //We are using an alias for functions here because scala also has sum, min, max, avg which will collide with Spark methods
      {
        import org.apache.spark.sql.{functions => F}
        fireTsDF.select(F.sum("NumAlarms"),F.avg("ResponseDelayedInMinutes"),
          F.min("ResponseDelayedInMinutes"),F.max("ResponseDelayedInMinutes")).show()
      }
  }
}
