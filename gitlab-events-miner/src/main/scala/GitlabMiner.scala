/*
This module reads the events table from the gitlab instance and publishes the push events that are in it.
 */
package GitlabMiner

import GitlabMiner.helperFunctions.Event
import java.sql.{DriverManager, ResultSet}
import scala.collection.mutable.ListBuffer
import com.typesafe.config.{Config, ConfigFactory}



object Main extends App {


  val config: Config = ConfigFactory.load().getConfig("postgresDB")
  val conn_str = config.getString("url")+"?user="+config.getString("user") +"&password="+config.getString("pass")

  Class.forName("org.postgresql.Driver")
  val conn = DriverManager.getConnection(conn_str)


  // Continuous poll
  val allevents = new ListBuffer[Event]()

  for (i <- 1 to 3) {
    //while (1==1) {
    val id =  if (allevents.isEmpty){1} else { allevents.last.id }
    val prep = conn.prepareStatement(s"select * from events as e, push_event_payloads as p where p.event_id = e.id and e.id >$id;")
    val result = prep.executeQuery
    while ( result.next() ) {
      val t = (Event.apply _).tupled(helperFunctions.getresult(result) )

      println(t)
      //send t to wherever
      allevents += t

    }
  }
  conn.close()

}

