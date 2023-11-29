import org.apache.spark.sql.{SparkSession, DataFrame}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class FlightAnalyticsTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("FlightAnalyticsTest").master("local[*]").getOrCreate()
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("totalFlightsPerMonth should return correct counts") {
    val sampleFlightData = Seq(
      (1, 1, "USA", "UK", "2023-01-01"),
      (2, 1, "USA", "CAN", "2023-02-01"),
      (3, 2, "UK", "CAN", "2023-01-10")
    )
    val flightData = spark.createDataFrame(sampleFlightData).toDF("passengerId", "flightId", "from", "to", "date")
    val result = FlightAnalytics.totalFlightsPerMonth(flightData)
    assert(result.collect().map(row => (row.getString(0), row.getLong(1))).toSet == Set(("01", 2), ("02", 1)))
  }

  test("topFrequentFlyers should return top flyers") {
    val sampleFlightData = Seq(
      (1, 100), (1, 101), (1, 102),
      (2, 100), (2, 103),
      (3, 104), (3, 105)
    )
    val flightData = spark.createDataFrame(sampleFlightData).toDF("passengerId", "flightId")

    val samplePassengerData = Seq(
      (1, "John", "Doe"),
      (2, "Jane", "Doe"),
      (3, "Bob", "Smith")
    )
    val passengerData = spark.createDataFrame(samplePassengerData).toDF("passengerId", "firstName", "lastName")

    val result = FlightAnalytics.topFrequentFlyers(flightData, passengerData)
    assert(result.head().getAs[String]("firstName") == "John")
  }

  test("longestRunWithoutUK should return longest run") {
    val sampleFlightData = Seq(
      (1, "UK", "2023-01-01"), (1, "FR", "2023-01-02"), (1, "DE", "2023-01-03"),
      (1, "UK", "2023-01-04"), (1, "US", "2023-01-05"),
      (2, "US", "2023-01-01"), (2, "FR", "2023-01-02")
    )
    val flightData = spark.createDataFrame(sampleFlightData).toDF("passengerId", "to", "date")

    val result = FlightAnalytics.longestRunWithoutUK(flightData)
    assert(result.filter($"passengerId" === 1).head().getAs[Long]("longestRun") == 2)
  }

  test("passengersWithSharedFlights should return shared flights") {
    val sampleFlightData = Seq(
      (1, 100), (2, 100),
      (1, 101), (3, 101),
      (2, 102), (3, 102),
      (1, 103), (2, 103), (3, 103)
    )
    val flightData = spark.createDataFrame(sampleFlightData).toDF("passengerId", "flightId")

    val result = FlightAnalytics.passengersWithSharedFlights(flightData)
    assert(result.count() > 0)
  }

  // Add more tests for passengersWithSharedFlightsWithinRange and other functions if needed


}
