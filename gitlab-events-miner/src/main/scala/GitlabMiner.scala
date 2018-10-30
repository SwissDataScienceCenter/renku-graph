/*
This module reads the events table from the gitlab instance and publishes the push events that are in it.
 */

import java.sql.{DriverManager, ResultSet}
import scala.collection.mutable.ListBuffer

object GitlabMiner extends App {
  // Values for database connection - extract later into seperate file

  val hostip = "86.119.30.76"
  val port = 30432
  val database = "gitlabhq_production"
  val user = "event-miner"
  val pass = "efc25bd91dfbd754d264f42ecac6f3c4"



  // Connect to database
  val conn_str = "jdbc:postgresql://"+hostip+":" + port+"/"+database+"?user="+user +"&password="+pass

  Class.forName("org.postgresql.Driver")
  val conn = DriverManager.getConnection(conn_str)

  val prep = conn.prepareStatement("SELECT * FROM events LIMIT 1")
  val result = prep.executeQuery

  val all_columns_list = getColumns(result)

  def getColumns (result: ResultSet ) : List[String] = {
    var all_columns= new ListBuffer[String]()
    val column_count = result.getMetaData().getColumnCount()
    for (i <- 1 to column_count ) {
      val column_name = result.getMetaData().getColumnName(i)
      all_columns += column_name
    }
    all_columns.toList
  }

  // Continuous poll


  val allevents = new ListBuffer[List[(String, String)]]()

  for (i <- 1 to 3) {
    print(i)
    //while (1==1) {
    val id =  if (allevents.isEmpty){130} else { allevents.tail.last.head._2}
    val prep = conn.prepareStatement(s"SELECT * FROM events WHERE action=5 AND id>$id")
    val result = prep.executeQuery
    while ( result.next() ) {
      val t = for (c <- all_columns_list) yield (c.toString, result.getString(c))
      //send t to wherever
      allevents += t
      //get the push events
      val event_id = result.getString("id")
      val prep = conn.prepareStatement(s"SELECT * FROM push_event_payloads where event_id=$event_id")
      val push_result = prep.executeQuery
    }
  }
  conn.close()

}

