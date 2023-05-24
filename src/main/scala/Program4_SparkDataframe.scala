import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object Program4_SparkDataframe {
  def main(args : Array[String]) : Unit = {
    /*
      Spark DataFrames are like distributed in-memory tables with named columns and schemas,
      where each column has a specific data type: integer, string, array, map, real, date,
      timestamp, etc. To a human’s eye, a Spark DataFrame is like a table.

      A named column in a DataFrame and its associated Spark data type can be declared in the schema.

      Basic Scala data types in Spark :-
      ----------------------------------
      Data type	        Value assigned in Scala	        API to instantiate
      ------------------------------------------------------------------------
      ByteType	        Byte	                          DataTypes.ByteType
      ShortType	        Short	                          DataTypes.ShortType
      IntegerType	      Int	                            DataTypes.IntegerType
      LongType	        Long	                          DataTypes.LongType
      FloatType	        Float	                          DataTypes.FloatType
      DoubleType	      Double	                        DataTypes.DoubleType
      StringType	      String	                        DataTypes.StringType
      BooleanType	      Boolean	                        DataTypes.BooleanType
      DecimalType	      java.math.BigDecimal	          DecimalType

      For complex data analytics, you won’t deal only with simple or basic data types.
      Your data will be complex, often structured or nested, and you’ll need Spark to handle
      these complex data types.
      They come in many forms: maps, arrays, structs, dates, timestamps, fields etc.,

      Scala structured data types in Spark :-
      ---------------------------------------

      Data type	          Value assigned in Scala	      API to instantiate
      --------------------------------------------------------------------------------------
      BinaryType	        Array[Byte]	                  DataTypes.BinaryType
      TimestampType	      java.sql.Timestamp	          DataTypes.TimestampType
      DateType	          java.sql.Date	                DataTypes.DateType
      ArrayType	          scala.collection.Seq	        DataTypes.createArrayType(ElementType)
      MapType	            scala.collection.Map	        DataTypes.createMapType(keyType, valueType)
      StructType	        org.apache.spark.sql.Row	    StructType(ArrayType[fieldTypes])
      StructField	        A value type corresponding    StructField(name, dataType, [nullable])
                          to the type of this field
    */

    val spark = SparkSession
      .builder()
      .appName("DataFrame Practice")
      .config("spark.master","local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //we can define variable names tied to a Spark data type like below
    val nameTypes = StringType
    val firstName = nameTypes
    val lastName = nameTypes

    /*
      Two ways to define schema for data frame
      1. To define it programmatically
      2. To employ a Data Definition Language String, which is much simpler and easier to read

      To define a schema programmatically for a dataframe with three names columns
      author, title and pages we can use the below code

      Dharani,Kumar,100
      Kavya,Gopavaram,200
      John,Jane,300
    */

    val authorProgrammaticSchema = StructType(List(
      StructField("Author",StringType,nullable = false),
      StructField("Title",StringType,nullable = false),
      StructField("Pages",IntegerType,nullable = false)
    ))
    val authorsFile = "/Users/dharani-kumar/Desktop/spark_programs/data/authorData.csv"

    val authorDF = spark.read.schema(authorProgrammaticSchema)
      .format("csv")
      .load(authorsFile)

    authorDF.show(truncate = false)

    //Defining schema for the same data using DDL string
    val authorDDLSchema = "author String,title String,pages Int"
    val authorDDLDF = spark.read.schema(authorDDLSchema)
      .format("csv")
      .load(authorsFile)

    authorDDLDF.show(truncate = false)

    /*
      For the following data defining a schema using programmatic approach and also using DDL string
      {"Id":1, "First": "Jules", "Last":"Damji", "Url":"https://tinyurl.1", "Published":"1/4/2016", "Hits": 4535, "Campaigns": ["twitter", "LinkedIn"]}
      {"Id":2, "First": "Brooke","Last": "Wenig","Url": "https://tinyurl.2", "Published": "5/5/2018", "Hits":8908, "Campaigns": ["twitter", "LinkedIn"]}
      {"Id": 3, "First": "Denny", "Last": "Lee", "Url": "https://tinyurl.3","Published": "6/7/2019","Hits": 7659, "Campaigns": ["web", "twitter", "FB", "LinkedIn"]}
    */

    val blogsFile = "/Users/dharani-kumar/Desktop/spark_programs/data/blogs.json"
    val blogsProgrammaticSchema = StructType(List(
      StructField("Id",IntegerType,nullable = false),
      StructField("First",StringType,nullable = false),
      StructField("Last",StringType,nullable = false),
      StructField("Url",StringType,nullable = false),
      StructField("Published",StringType,nullable = false),
      StructField("Hits",IntegerType,nullable = false),
      StructField("Campaigns",ArrayType(StringType),nullable = false)
    ))
    val blogsProgrammaticDF = spark.read.schema(blogsProgrammaticSchema)
      .format("json")
      .load(blogsFile)

    //converting the Published string column to date
    blogsProgrammaticDF
      .withColumn("PublishedDate",to_date(col("Published"),"M/d/yyyy"))
      .show(truncate = false)

    blogsProgrammaticDF.printSchema // Print the schema
    println(blogsProgrammaticDF.schema)

    val blogsDDLSchema = "Id Int,First String,Last String,Url String,Published String,Hits Int,Campaigns Array<String>"
    val blogsDDLDF = spark.read.schema(blogsDDLSchema)
      .format("json")
      .load(blogsFile)

    blogsDDLDF.show(truncate = false)
    blogsDDLDF.printSchema
    println(blogsDDLDF.schema)

    /*
      Columns and Expressions
      Named columns in DataFrames are conceptually similar to named columns in pandas or R
      DataFrames or in an RDBMS table: they describe a type of field.
      You can list all the columns by their names, and you can perform operations on their
      values using relational or computational expressions.
      In Spark’s supported languages, columns are objects with public methods (represented by the Column type)
    */

    println(blogsProgrammaticDF.columns.mkString(",")) //returns all the columns available in the dataframe as an Array[String]

    //use an expression to compute a value
    blogsProgrammaticDF
      .select(expr("Hits * 2")).show(2,truncate = false)

    //we can use col as well to do the same thing
    blogsProgrammaticDF
      .select(col("Hits") * 2).show(2,truncate = false)

    //Use an expression to compute big hitters for blogs
    //This adds a new column, BigHitters based on the conditional expression
    blogsProgrammaticDF
      .withColumn("BigHitters",expr("Hits > 10000")).show( truncate = false)

    //concatenate three columns with our own separator using lit and select the newly created column
    blogsProgrammaticDF
      .withColumn("AuthorsId",concat(col("First"),lit("_"),col("Last"),lit("_"),col("Id")))
      .select("AuthorsId")
      .show(truncate = false)

    blogsProgrammaticDF
      .sort(col("Id").desc)
      .show(truncate = false)

    //The $ before the name of the column which is a function in Spark that converts column named Id to a Column
    import spark.implicits._
    blogsProgrammaticDF
      .sort($"Id".desc)
      .show(truncate = false)

    //Column objects in a dataframe can't exist in isolation; each column is part of a row in a record and all the rows together constitute a data frame

    /*
      Rows :-
      A row in Spark is a generic Row object, containing one or more columns.
      As Row is an object and an ordered collection of fields, you can instantiate a Row in
      each of Spark's supported languages and access its fields by an index starting from 0
    */

    val blogsRow = Row(7,"Dharani","Kumar","https://tinyurl.7","3/4/2022",49283,Array("Twitter","FB","Instagram"))
    println(s"blogsRow first element :- ${blogsRow(0)}")

    //We are using getAs[Array[String]] to get the value of array in our Row
    val campaigns = blogsRow.getAs[Array[String]](6)
    println(s"blogsRow array element :- ${campaigns.mkString(",")}")
    println(s"blogsRow array element second value :- ${campaigns(1)}")


    spark.stop()

  }
}
