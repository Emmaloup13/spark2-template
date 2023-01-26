package sql_practice

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2() : Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demoCom = spark.read
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")

    println("Total population of France")

    demoCom.agg(sum("Population").as("TotalPop")).show()

    val groupedCom = demoCom.groupBy("Departement")
      .agg(sum("Population")
      .as("Pop"))
      .sort($"Pop".desc)

    groupedCom.show()

    val dep = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .csv("data/input/departements.txt")
      .withColumnRenamed("_c0", "DepName")

    println("Join departements with their names and total population")
    groupedCom.join(dep, groupedCom("Departement") === dep("_c1"))
      .select("Departement", "Pop", "DepName")
      .show()
  }

  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s07 = spark.read
      .option("multiline", true)
      .option("delimiter", "\t")
      .csv("data/input/sample_07")
      .withColumnRenamed("_c0", "Code_07")
      .withColumnRenamed("_c1", "Description_07")
      .withColumnRenamed("_c2", "Total_Empl_07")
      .withColumnRenamed("_c3", "Salary_07")

    val s08 = spark.read
      .option("multiline", true)
      .option("delimiter", "\t")
      .csv("data/input/sample_08")
      .withColumnRenamed("_c0", "Code_08")
      .withColumnRenamed("_c1", "Description_08")
      .withColumnRenamed("_c2", "Total_Empl_08")
      .withColumnRenamed("_c3", "Salary_08")

    println("Top salaries in 2007 above 100K")

    s07.select("Description_07", "Salary_07")
      .filter(s07("Salary_07") > 100000)
      .sort($"Salary_07".desc)
      .show()

    println("Salary growth from 2007-08")

    s07.join(s08, s07("Code_07")===s08("Code_08"))
      .filter(s08("Salary_08") > s07("Salary_07"))
      .withColumn("Growth", s08("Salary_08") - s07("Salary_07"))
      .sort($"Growth".desc)
      .select("Description_07", "Salary_07", "Salary_08", "Growth")
      .show()

    println("Job Loss among the top earnings from 2007-08")

    s07.join(s08, s07("Code_07")===s08("Code_08"))
      .filter(s07("Salary_07") > 100000)
      .filter(s08("Total_Empl_08") < s07("Total_Empl_07"))
      .withColumn("Jobs Lost", s07("Total_Empl_07") - s08("Total_Empl_08"))
      .sort($"Jobs Lost".desc)
      .select("Description_07","Salary_07","Total_Empl_07", "Total_Empl_08", "Jobs Lost")
      .show()
  }

  def exec4(): Unit= {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val tours = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    println("1. Unique levels of difficulty")

    tours.select("tourDifficulty").distinct().show()

    println("2. Min/Max/Avg of tour prices")

    tours.agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))
      .show()

    println("3. Min/Max/Avg of tour prices for each level of difficulty")

    tours.groupBy("tourDifficulty")
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))
      .show()

    println("4. Min/Max/Avg of tour prices and tour durations for each level of difficulty")

    val priceByDiff = tours.groupBy("tourDifficulty")
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))

    val lengthByDiff = tours.groupBy("tourDifficulty")
      .agg(min("tourLength"), max("tourLength"), avg("tourLength"))

    priceByDiff.join(lengthByDiff, priceByDiff("tourDifficulty")===lengthByDiff("tourDifficulty"))
      .show()

    println("5. Top 10 tour tags")

    tours.select(explode($"tourTags").as("Tag"))
      .groupBy("Tag")
      .count()
      .sort($"count".desc)
      .show(10)

    println("6. Relationship between top 10 tourTags and tourDifficulty")

    tours
      .select(explode($"tourTags").as("Tag"),$"tourDifficulty")
      .groupBy("tourDifficulty", "Tag")
      .count()
      .sort($"Tag".asc,$"count".desc)
      .show(100)

    println("6bis. top tourDifficulty per tourTags")

    tours
      .select(explode($"tourTags").as("Tag"), $"tourDifficulty")
      .groupBy("tourDifficulty", "Tag")
      .count()
      .withColumn("Rank", rank().over(Window.partitionBy("Tag").orderBy($"count".desc)))
      .filter("Rank == 1")
      .sort($"count".desc)
      .show(100)

    println("7. Max/Min/Avg of price in tourTags and tourDifficulty relationship")

    tours
      .select(explode($"tourTags").as("Tag"), $"tourDifficulty", $"tourPrice")
      .groupBy("tourDifficulty", "Tag")
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))
      .sort($"avg(tourPrice)")
      .show(100)

  }
}
