import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, upper}

object Program7_SparkSQL {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master","local")
      .appName("SparkSQL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val flightsSchema = "date String,delay Int,distance Int,origin String,destination String"

    val flightDelaysDF = spark.read.schema(flightsSchema).format("csv")
      .option("header","true")
      .load("/Users/dharani-kumar/Desktop/spark_programs/data/departuredelays.csv")

    flightDelaysDF
      .createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql("select * from us_delay_flights_tbl").show(10,truncate = false)

    //Find all flights whose distance is greater than 1000 miles
    spark.sql(
      """
        |select distinct distance, origin, destination
        |from us_delay_flights_tbl
        |where distance > 1000
        |order by distance desc
        |""".stripMargin).show(20,truncate = false)

    //Find flights between a source and destination which are having at-least two hour delay
    spark.sql(
      """
        |select date, origin, destination, distance, delay
        |from us_delay_flights_tbl
        |where origin = 'SFO' and destination = 'ORD'
        |and delay > 120
        |""".stripMargin).show(30,truncate = false)

    spark.sql(
      """
        |select origin, distance, delay,
        |       case
        |            when delay > 360 then 'Very Long delays'
        |            when delay >= 120 and delay <= 360 then 'Long delays'
        |            when delay >= 60 and delay < 120 then 'Short delays'
        |            when delay > 0 and delay < 60 then 'tolerable delays'
        |            when delay = 0 then 'No delays'
        |            else 'early'
        |       end as flight_delays
        |from us_delay_flights_tbl
        |order by origin, delay
        |""".stripMargin).show(50, truncate = false)

    //Reading Parquet files into a dataframe
    //there is no need to supply the schema because parquet saves it as part of metadata
    println("Reading parquet files data to a dataframe")
    val parquetDF = spark.read.format("parquet")
      .load("/Users/dharani-kumar/Desktop/spark_programs/data/Parquet_data")

    parquetDF.show(10,truncate = false)

    //Reading Parquet files into a spark sql table
    println("Reading parquet data to a SQL table")
    spark.sql(
      """
        |create or replace temporary view sample_view
        |using parquet
        |options (path "/Users/dharani-kumar/Desktop/spark_programs/data/Parquet_data")
        |""".stripMargin)

    spark.sql("select * from sample_view").show(10,truncate = false)

    //Writing dataframes to Parquet file
    println("Writing dataframe data to Parquet files")
    flightDelaysDF.repartition(4).write.format("parquet")
      .mode("overwrite")
      .option("compression","snappy")
      .save("/Users/dharani-kumar/Desktop/spark_programs/data/flight_delays_data")

    println("Writing dataframe data to Parquet files is successfully completed")

    /*
      JavaScript Object Notation (JSON) is also a popular data format.
      It came to prominence as an easy-to-read and easy-to-parse format compared to XML.
      It has two representational formats: single-line mode and multiline mode.
      Both modes are supported in Spark.
      In single-line mode each line denotes a single JSON object, whereas in multiline mode the entire
      multiline object constitutes a single JSON object.
      To read in this mode, set multiLine to true in the option() method.
    */

    println("Reading json files into a dataframe")
    val jsonSchema = "ORIGIN_COUNTRY_NAME String,DEST_COUNTRY_NAME String,count Long"

    val jsonDF = spark.read.format("json").schema(jsonSchema)
      .option("multiLine","true")
      .load("/Users/dharani-kumar/Desktop/spark_programs/data/json_files/2010-correct-summary.json")

    jsonDF.show(20,truncate = false)

    println("Reading json file data to a spark sql table")
    spark.sql(
      """
        |create or replace temporary view json_view
        |using json
        |options (path="/Users/dharani-kumar/Desktop/spark_programs/data/json_files/2010-correct-summary.json",multiline = true)
        |""".stripMargin)

    spark.sql("select * from json_view").show(40, truncate = false)

    println("Writing dataframe to json files")
    jsonDF.write.format("json")
      .mode("overwrite")
      .save("/Users/dharani-kumar/Desktop/spark_programs/data/json_write_data")

    println("Writing dataframe to json files is successfully completed")

    val singleJsonString = jsonDF.repartition(1).toJSON.collect().mkString("[",",","]")
    println(singleJsonString)

    //Reading a csv file into a dataframe
    //The option("mode","FAILFAST") will throw an error if there is any incorrect record in the csv file
    //By ddefault the mode is permissive
    val csvDF = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("nullValue","")
      .load("/Users/dharani-kumar/Desktop/spark_programs/data/csv_data/2010-summary.csv")

    csvDF.show(20,truncate = false)

    //Reading a csv file into a spark sql table
    spark.sql(
      """
        |create or replace temporary view csv_view
        |using csv
        |options (path '/Users/dharani-kumar/Desktop/spark_programs/data/csv_data/2010-summary.csv',
        | header 'true',
        | inferSchema 'true',
        | mode 'PERMISSIVE')
        |""".stripMargin)

    spark.sql("select * from csv_view").show(20,truncate = false)

    //writing dataframe to csv files
    println("Writing dataframe data to csv file")
    csvDF.select(upper(col("DEST_COUNTRY_NAME")).alias("DEST_COUNTRY_NAME"),upper(col("ORIGIN_COUNTRY_NAME")).alias("ORIGIN_COUNTRY_NAME"),col("count"))
      .write
      .option("header","true")
      .format("csv")
      .mode("overwrite")
      .save("/Users/dharani-kumar/Desktop/spark_programs/data/csv_write_data")

    println("writing data frame data to csv file is completed successfully")

    /*
      csv datasource options :-

      Property name	    Values	                  Meaning	                                    Scope
      ------------------------------------------------------------------------------------------------------
      compression	      none, bzip2, deflate,     Use this compression codec for writing.	    Write
                        gzip, lz4, or snappy
      dateFormat	      yyyy-MM-dd or             Use this format or any format 	            Read/write
                        DateTimeFormatter	        from Java’s DateTimeFormatter.
      multiLine	        true, false	              Use multiline mode.                         Read
                                                  Default is false (single-line mode).
      inferSchema	      true, false	              If true, Spark will determine the           Read
                                                  column data types.
                                                  Default is false.
      sep	              Any character	            Use this character to separate              Read/write
                                                  column values in a row.
                                                  Default delimiter is a comma (,).
      escape	          Any character	            Use this character to escape quotes.        Read/write
                                                  Default is \.
      header	          true, false	              Indicates whether the first line is         Read/write
                                                  a header denoting each column name.
                                                  Default is false.
    */


  }
}
