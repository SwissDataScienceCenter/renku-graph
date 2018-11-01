/*
This module reads the events table from the gitlab instance and publishes the push events that are in it.
 */
package GitlabMiner

import java.sql.{DriverManager, ResultSet}
import scala.collection.mutable.ListBuffer

import com.typesafe.config.{Config, ConfigFactory}



object Main extends App {


  val config: Config = ConfigFactory.load().getConfig("postgresDB")
  val conn_str = config.getString("url")+"?user="+config.getString("user") +"&password="+config.getString("pass")

  Class.forName("org.postgresql.Driver")
  val conn = DriverManager.getConnection(conn_str)


  // Continuous poll
  val allevents = new ListBuffer[List[(String, String)]]()

  for (i <- 1 to 3) {
    //while (1==1) {
    val id =  if (allevents.isEmpty){3} else { allevents.tail.last.head._2}
    val prep = conn.prepareStatement(s"select * from events as e, push_event_payloads as p where p.event_id = e.id and e.id >$id;")
    val result = prep.executeQuery
    while ( result.next() ) {
      val t = for (c <- helperFunctions.all_columns_list) yield (c.toString, result.getString(c))
      //send t to wherever
      allevents += t

    }
    println(allevents)
  }
  conn.close()

}

