package loganalyse

import java.sql.Date
import java.time.OffsetDateTime

import org.apache.spark.sql.Row

/* Row composition
 *
 * 0 - String: IP or name
 * 1 - String: Client ID on user machine
 * 2 - String: User name
 * 3 - OffsetDateTime: Date and time of request
 * 4 - String: Request Method
 * 5 - String: Request endpoint
 * 6 - String: Protocol
 * 7 - Integer: Response Code
 * 8 - Integer: Content size
 *
 */
@SerialVersionUID(15L)
class LogEntry (row: Row) extends Serializable{

  def getHost() : String = row.getString(0)
  def getId() : String = row.getString(1)
  def getUsername() : String = row.getString(2)
  def getDatetime() : OffsetDateTime = row.getAs[OffsetDateTime](3)
  def getRequestMethod() : String = row.getString(4)
  def getRequestEndpoint() : String = row.getString(5)
  def getProtocol() : String = row.getString(6)
  def getResponseCode() : Int = row.getInt(7)
  def getContentSize() : Int = row.getInt(8)

}