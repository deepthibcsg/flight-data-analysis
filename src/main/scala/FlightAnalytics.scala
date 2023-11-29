import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object FlightAnalytics {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .appName("Flight Analytics")
            .master("local[*]") // for local testing; change as necessary
            .getOrCreate()

        val flightData = spark.read.option("header", "true").csv("resources/flightData.csv")
        val passengerData = spark.read.option("header", "true").csv("resources/passengers.csv")

        // Calling the functions for analysis and showing results
        totalFlightsPerMonth(flightData).show()
        topFrequentFlyers(flightData, passengerData).show()
        longestRunWithoutUK(flightData).show()
        passengersWithSharedFlights(flightData).show()
        passengersWithSharedFlightsWithinRange(flightData, "2023-01-01", "2023-12-31").show() // Example date range

        spark.stop()
    }

    def totalFlightsPerMonth(flightData: DataFrame): DataFrame = {
        import flightData.sparkSession.implicits._
        flightData.select($"date".substr(6, 2).alias("month"))
            .groupBy("month")
            .count()
            .orderBy("month")
    }

    def topFrequentFlyers(flightData: DataFrame, passengerData: DataFrame): DataFrame = {
        flightData.groupBy("passengerId")
            .count()
            .join(passengerData, Seq("passengerId"))
            .select("passengerId", "count", "firstName", "lastName")
            .orderBy($"count".desc)
            .limit(100)
    }

    def longestRunWithoutUK(flightData: DataFrame): DataFrame = {
        import flightData.sparkSession.implicits._
        val withSeq = flightData.withColumn("seqId", row_number().over(Window.partitionBy($"passengerId").orderBy($"date")))
        val withUKFlag = withSeq.withColumn("isUK", when($"to" === "UK", 1).otherwise(0))
        val withGroup = withUKFlag.withColumn("group", $"seqId" - sum($"isUK").over(Window.partitionBy($"passengerId").orderBy($"seqId")))
        val longestRun = withGroup.filter($"to" =!= "UK").groupBy($"passengerId", $"group").agg(max($"seqId") - min($"seqId") + 1 as "run")
        longestRun.groupBy("passengerId").agg(max("run").as("longestRun"))
    }

    def passengersWithSharedFlights(flightData: DataFrame): DataFrame = {
        flightData.alias("a")
            .join(flightData.alias("b"), col("a.flightId") === col("b.flightId") && col("a.passengerId") < col("b.passengerId"))
            .groupBy("a.passengerId", "b.passengerId")
            .count()
            .filter($"count" > 3)
    }

    def passengersWithSharedFlightsWithinRange(flightData: DataFrame, fromDate: String, toDate: String): DataFrame = {
        val filteredFlights = flightData.filter($"date".between(fromDate, toDate))
        passengersWithSharedFlights(filteredFlights)
    }
}
