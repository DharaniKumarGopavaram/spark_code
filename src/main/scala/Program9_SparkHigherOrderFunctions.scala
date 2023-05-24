import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Program9_SparkHigherOrderFunctions {
  def main(args : Array[String]) : Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.master","local")
      .appName("Spark Higher Order Functions")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val data = Seq(
      (1,Array(98,90,98,94)),
      (2,Array(97,87))
    )

    val studentsDF = spark.createDataFrame(data).toDF("Sid","Marks")
    studentsDF.createOrReplaceTempView("students_data")

    //In this below SQL statement we first explode(Marks) which creates a new row with Sid and for each element Mark in Marks
    //While collect_list() returns a list of objects with duplicates, the GROUP BY statement requires shuffle operations, meaning the order of the re-collected array isn’t necessarily the same as that of the original array. As values could be any number of dimensions (a really wide and/or really long array) and we’re doing a GROUP BY, this approach could be very expensive.
    println("using explode and collect_list in the spark sql")

    spark.sql(
      """
        |select Sid,collect_list(subject_mark + 1) as Incremented_Marks
        |from (select Sid, explode(Marks) as subject_mark
        |from students_data) x
        |group by Sid
        |""".stripMargin).show(100,truncate = false)

    //Doing the same thing using dataframe API
    println("using explode and collect_list in dataframe API")
    studentsDF
      .select(col("Sid"),explode(col("Marks")).as("Mark"))
      .groupBy("Sid")
      .agg(collect_list(col("Mark") + 1).as("Incremented_Marks"))
      .show(100,truncate = false)

    //We can do the same thing using UDF
    println("Achieving the explode and collect_list functionality using udf")
    def addOne(marks : List[Int]) : List[Int] = {
      marks.map(x => x + 2)
    }

    spark.udf.register("plusOneInt", addOne(_: List[Int]): List[Int])

    //While this is better than using explode() and collect_list() as there won’t be any ordering issues, the serialization and deserialization process itself may be expensive.
    // It’s also important to note, however, that collect_list() may cause executors to experience out-of-memory issues for large data sets, whereas using UDFs would alleviate these issues.
    spark.sql("select Sid,plusOneInt(Marks) from students_data")
      .show(100,truncate = false)

    //we need to use expr when calling udf's using dataframe
    studentsDF.select(col("Sid"),expr("plusOneInt(Marks)"))
      .show(100,truncate = false)

    //array_distinct removes duplicates within an array
    println("Array distinct function which removes duplicates within the array")
    spark.sql("select array_distinct(Array(1,2,3,null,3,4)) as array_distinct_function")
      .show()

    //array_intersect returns intersection of two arrays without duplicates
    println("Array intersect function which returns intersection of two arrays without duplicates")
    spark.sql("select array_intersect(Array(1,2,2,3,4,5),Array(1,2,2,6)) as array_intersect_function")
      .show()

    //array_union returns the union of two arrays without duplicates
    println("Array union returns the union of two arrays without duplicates")
    spark.sql("select array_union(Array(1,2,2,3,4,5),Array(1,2,2,6)) as array_union_function")
      .show()

    //array_except returns elements in array1 but not in array2 without duplicates
    println("array_except returns elements in array1 but not in array2 without duplicates ")
    spark.sql("select array_except(Array(1,2,2,2,3,3,4,5),Array(1,2,2,6)) as array_intersect_function")
      .show()

    //array_join concatenates elements of an array using delimiter
    println("array_join concatenates elements of an array using delimiter")
    spark.sql(""" select Sid,array_join(Marks,"| ") as concatenated_marks from students_data """)
      .show()

    //array_max returns the maximum value within the array ; null elements are skipped
    println("array_max returns the maximum value within the array ; null elements are skipped")
    spark.sql("select array_max(Array(1,4,2,20,null,56,45,23)) as array_max_function")
      .show()

    //array_min returns the minimum value within the array ; null elements are skipped
    println("array_min returns the minimum value within the array ; null elements are skipped")
    spark.sql("select array_min(Array(1,4,2,20,null,56,45,23)) as array_min_function")
      .show()

    //array_position returns the 1-based index of the first element of the given array as Long
    println("array_position returns the 1-based index of the first element of the given array as Long")
    spark.sql("select array_position(Array(3,2,1,4,1),1) as array_position_function")
      .show()

    //array_remove removes all the elements that are equal to the given element from the given array
    println("array_remove removes all the elements that are equal to the given element from the given array")
    spark.sql(""" select array_remove(Array(1,2,3,4,1,2,1,1,2,null,null),1) as array_remove_function """)
      .show(truncate = false)

    //arrays_overlap returns true if array1 contains at-least one non-null element also present in array2
    println("arrays_overlap returns true if array1 contains at-least one non-null element also present in array2")
    spark.sql("select arrays_overlap(array(1,2,3),Array(4,5)) as array_overlap_function")
      .show()

    println("arrays_overlap example 2")
    spark.sql("select arrays_overlap(array(1,2,3),Array(3,5)) as array_overlap_function")
      .show()

    //array_sort sorts the input array in ascending order with the null elements placed at the end of the array
    println("array_sort sorts the input array in ascending order with the null elements placed at the end of the array")
    spark.sql(""" select array_sort(Array(4,5,2,1,2,5,2,6,7,2,8,1,2,"a","d","b")) as array_sort_function """)
      .show(truncate = false)

    //concat function concatenates strings, arrays etc.,
    println("concat function concatenates strings, arrays etc.,")
    spark.sql(""" select concat(Array(1,2,3),Array(4,5,6,7)) as concat_function """)
      .show(truncate = false)

    spark.sql(""" select concat("Dharani","|","Kumar","|","Gopavaram") as concat_function """)
      .show(truncate = false)

    //array_repeat returns an array containing the specified element the specified number of times
    println("array_repeat returns an array containing the specified element the specified number of times")
    spark.sql(""" select array_repeat("Dharani",20) as array_repeat_function""")
      .show(truncate = false)

    //reverse function returns the reverse order of elements
    println("reverse function returns the reverse order of elements")
    spark.sql("""select reverse(Array(3,2,4,1)) as array_reverse_function""")
      .show()

    //sequence function generates an array of elements from start to stop(inclusive) by incremental step
    println("sequence function generates an array of elements from start to stop(inclusive) by incremental step")
    spark.sql(""" select sequence(1,5) as sequence_function_example_1""").show()
    spark.sql(""" select sequence(5,1) as sequence_function_example_2""").show()
    spark.sql(""" select sequence(to_date("2023-01-01"),to_date("2023-05-25"),interval 1 day) as sequence_function_example_3 """)
      .show(truncate = false)
    spark.sql(""" select sequence(to_timestamp("2023-01-01 01","yyyy-MM-dd HH"),to_timestamp("2023-01-01 23","yyyy-MM-dd HH"),interval 1 hour) as sequence_function_example_4 """)
      .show(truncate = false)
  }
}
