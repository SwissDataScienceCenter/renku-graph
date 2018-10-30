/*
This module reads the events table from the gitlab instance and publishes the push events that are in it.
 */

import java.sql.{DriverManager, ResultSet}
import scala.collection.mutable.ListBuffer

import com.typesafe.config.{Config, ConfigFactory}



object Main extends App {


  val config: Config = ConfigFactory.load().getConfig("postgresDB")
  val conn_str = config.getString("url")+"?user="+config.getString("user") +"&password="+config.getString("pass")

  Class.forName("org.postgresql.Driver")
  val conn = DriverManager.getConnection(conn_str)

  val prep = conn.prepareStatement("SELECT * FROM events LIMIT 1")
  val result = prep.executeQuery

  val all_columns_list = getColumns(result)

  def getColumns (result: ResultSet ) : List[String] = {
    var all_columns= new ListBuffer[String]()
    val column_count = result.getMetaData.getColumnCount
    for (i <- 1 to column_count ) {
      val column_name = result.getMetaData.getColumnName(i)
      all_columns += column_name
    }
    all_columns.toList
  }

  // Continuous poll


  val allevents = new ListBuffer[List[(String, String)]]()

  for (i <- 1 to 3) {
    println(i)
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

