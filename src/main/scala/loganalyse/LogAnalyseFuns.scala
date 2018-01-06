package loganalyse

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.time.DayOfWeek
import java.time.format.TextStyle
import java.util.Locale


object LogAnalyseFuns {



  def getParsedData(
      data: RDD[String]): (RDD[(Row, Int)], RDD[LogEntry], RDD[Row]) = {
    val parsed_logs = data.map(Utilities.parse_line)
    val access_logs = parsed_logs.filter(_._2 == 1).map(row => new LogEntry(row._1)).cache()
    val failed_logs = parsed_logs.filter(_._2 == 0).map(_._1)
    (parsed_logs, access_logs, failed_logs)
  }

  def calculateLogStatistic(data: RDD[LogEntry]): (Long, Long, Long) = {
    val contentSize = data.map[Long](f => f.getContentSize())
    (
      contentSize.min(),
      contentSize.max(),
      (contentSize.sum() / contentSize.count()).toLong
    )
  }

  /*
   * Calculate for the content size the following values:
   *
   * minimum: Minimum value
   * maximum: Maximum value
   * average: Average
   *
   * Return the following triple:
   * (min,max,avg)
   */

  def getResponseCodesAndFrequencies(data: RDD[LogEntry]): List[(Int, Int)] = {
    data.groupBy(entry => entry.getResponseCode())
      .map{ case (code, entries) => (code, entries.count(_ => true)) }
      .collect()
      .toList
  }

  /*
   * Calculate for each single response code the number of occurences
   * Return a list of tuples which contain the response code as the first
   * element and the number of occurences as the second.
   *
   */

  def get20HostsAccessedMoreThan10Times(data: RDD[LogEntry]): List[String] = {
    data
      .groupBy(row => row.getHost())
      .map [(String, Int)] {
        case (str: String, it: Iterable[LogEntry])  => (str, it.count(_ => true))
      }
      .filter { case (_ , count) => count > 20 }
      .map{ case (host, _) => host }
      .take(20)
      .toList
  }

  /*
   * Calculate 20 abitrary hosts from which the web server was accessed more than 10 times
   * Print out the result on the console (no tests take place)
   */

  def getTopTenEndpoints(data: RDD[LogEntry]): List[(String, Int)] = {
    data.groupBy(_.getRequestEndpoint())
      .map{ case (endpoint, entries) => (endpoint, entries.count(_ => true)) }
      .takeOrdered(10)(NumberOrdering)
      .toList
  }

  /*
   * Calcuclate the top ten endpoints.
   * Return a list of tuples which contain the path of the endpoint as the first element
   * and the number of accesses as the second
   * The list should be ordered by the number of accesses.
   */

  def getTopTenErrorEndpoints(data: RDD[LogEntry]): List[(String, Int)] = {
    data
      .filter(row => row.getResponseCode() != 200)
      .groupBy(row => row.getRequestEndpoint())
      .map(row => (row._1, row._2.count(k => true)))
      .takeOrdered(10)(NumberOrdering)
      .toList
  }

  /*
   * Calculate the top ten endpoint that produces error response codes (response code != 200).
   *
   * Return a list of tuples which contain the path of the endpoint as the first element
   * and the number of errors as the second.
   * The list should be ordered by the number of accesses.
   */

  def getNumberOfRequestsPerDay(data: RDD[LogEntry]): List[(Int, Int)] = {
    data.groupBy(entry => entry.getDatetime().getDayOfMonth)
      .map { case (date, entries) => (date, entries.count(_ => true)) }
      .sortBy(entry => entry._1, ascending = true)
      .collect()
      .toList
  }

  /*
   * Calculate the number of requests per day.
   * Return a list of tuples which contain the day (1..30) as the first element and the number of
   * accesses as the second.
   * The list should be ordered by the day number.
   */



  def numberOfUniqueHosts(data: RDD[LogEntry]): Long =
    data.groupBy(row => row.getHost()).count()

  /*
   * Calculate the number of hosts that accesses the web server in June 95.
   * Every hosts should only be counted once.
   */


  def numberOfUniqueDailyHosts(data: RDD[LogEntry]): List[(Int, Int)] = {
    data.groupBy(entry => entry.getDatetime().getDayOfMonth)
      .map{
        case (day, entries) => (
          day,
          entries.map(entry => entry.getHost()).toList.distinct.count(_ => true)
        )
      }
      .sortBy(entry => entry._1, ascending = true)
      .collect()
      .toList
  }


  /*
   * Calculate the number of hosts per day that accesses the web server.
   * Every host should only be counted once per day.
   * Order the list by the day number.
   */


  def averageNrOfDailyRequestsPerHost(data: RDD[LogEntry]): List[(Int, Int)] = {
    data.groupBy(entry => entry.getDatetime().getDayOfMonth)
      .map{
        case (day, entries) => {
          val hosts = entries.groupBy(entry => entry.getHost())
          (
           day,
           hosts.foldLeft(0)(_ + _._2.size) / hosts.size
        )}
      }
    .sortBy(entry => entry._1, ascending = true)
    .collect()
    .toList
  }

  /*
   * Calculate the average number of requests per host for each single day.
   * Order the list by the day number.
   */

  def top25ErrorCodeResponseHosts(data: RDD[LogEntry]): Set[(String, Int)] = {
    data.filter(e => e.getResponseCode() == 404)
      .groupBy(e => e.getHost())
      .map(e => (e._1, e._2.count(k=> true)))
      .takeOrdered(25)(NumberOrdering)
      .toSet
  }

  /*
   * Calculate the top 25 hosts that causes error codes (Response Code=404)
   * Return a set of tuples consisting the hostnames  and the number of requests
   */

  def responseErrorCodesPerDay(data: RDD[LogEntry]): List[(Int, Int)] =  {
    data.filter(e => e.getResponseCode() == 404)
      .groupBy(e => e.getDatetime().getDayOfMonth())
      .map(e => (e._1, e._2.count(k=> true)))
      .sortBy(e => e._1, ascending=true)
      .collect()
      .toList
  }

  /*
   * Calculate the number of error codes (Response Code=404) per day.
   * Return a list of tuples that contain the day as the first element and the number as the second.
   * Order the list by the day number.
   */

  def errorResponseCodeByHour(data: RDD[LogEntry]): List[(Int, Int)] = {
    data.filter(e => e.getResponseCode() == 404)
      .groupBy(e => e.getDatetime().getHour())
      .map(e => (e._1, e._2.count(k=> true)))
      .sortBy(e => e._1, ascending=true)
      .collect()
      .toList
  }

  /*
  * Calculate the error response coded for every hour of the day.
  * Return a list of tuples that contain the hour as the first element (0..23) abd the number of error codes as the second.
  * Ergebnis soll eine Liste von Tupeln sein, deren erstes Element die Stunde bestimmt (0..23) und
  * Order the list by the hour-number.
  */

  def getAvgRequestsPerWeekDay(data: RDD[LogEntry]): List[(Int, String)] = {
    data
      .groupBy(e => e.getDatetime().getDayOfWeek)
      .sortBy(_._1, true)
      .map{case (weekday, entries) => 
         val perDay = entries.groupBy(_.getDatetime().getDayOfYear).map(e => e._2.count(k => true))
         val avg = perDay.sum / perDay.size
        (avg, weekday.getDisplayName(TextStyle.FULL, Locale.ENGLISH))
       }
      .collect()
      .toList
  }


  /*
  * Calculate the number of requests per weekday (Monday, Tuesday,...).
  * Return a list of tuples that contain the number of requests as the first element and the weekday
  * (String) as the second.
  * The elements should have the following order: [Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday].
  */
  // sort by number
  object NumberOrdering extends Ordering[(String, Int)] {
    def compare(a: (String, Int), b: (String, Int)) = b._2 compare a._2
  }

  object WeekDay extends Enumeration {
    type WeekDay = Value
    val Monday, Tuesday, Wednesday, Thu, Fri, Sat, Sun = Value
  }

}
