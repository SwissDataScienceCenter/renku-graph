/*
This module reads the events table from the gitlab instance and publishes the push events that are in it.
 */
package GitlabMiner

import GitlabMiner.helperFunctions.{Event, GitSingleCommit}
import java.sql.{DriverManager}
import scala.collection.mutable.ListBuffer
import com.typesafe.config.{Config, ConfigFactory}
import scalaj.http._
import spray.json._

object Main extends App {


  val config: Config = ConfigFactory.load().getConfig("postgresDB")
  val conn_str = config.getString("url")+"?user="+config.getString("user") +"&password="+config.getString("pass")

  Class.forName("org.postgresql.Driver")
  val conn = DriverManager.getConnection(conn_str)


  // Continuous poll
  val allevents = new ListBuffer[Event]()
  val allcommits = new ListBuffer[GitSingleCommit]()
  while (1==1) {

    val id = if (allevents.isEmpty){0} else { allevents.last.id }
    val prep = conn.prepareStatement(s"select * from events as e, push_event_payloads as p where p.event_id = e.id and e.id >$id;")
    val result = prep.executeQuery
    while ( result.next() ) {
      val event = (Event.apply _).tupled(helperFunctions.getresult(result) )
      allevents += event
      val commit =  getCommit(event.project_id, getBytes(event.commit_to), getBytes(event.commit_from), event.commit_count, 1, List.empty)
      println(commit)
      }
  }

  def getBytes(commit_hash: Option[Array[Byte]]): String = commit_hash match {
    case Some(x) => x.map("%02x".format(_)).mkString
    case None => ""
  }

  def getCommit(project_id: Int, commit_to: String, commit_from: String, commit_count: Int, count: Int, list: List[GitSingleCommit]):  List[GitSingleCommit] = {
    val response: HttpResponse[String] =
      Http("https://testing.datascience.ch/api/v4/projects/"+ project_id + "/repository/commits/" +
        commit_to).asString

    if (response.code==200) {
      val firstCommit = ((response.body).parseJson).convertTo[GitSingleCommit]

      if (commit_count == 1 ||  commit_count == count ||
        (firstCommit.parent_ids.length == 1 && commit_from == firstCommit.parent_ids.head)) {
              list:+firstCommit
      }
      else {
        firstCommit.parent_ids.length match {
          case 1 => getCommit(project_id, firstCommit.parent_ids.head, commit_from, commit_count, count+1, list:+firstCommit)
          case 2 => {
            getCommit(project_id, firstCommit.parent_ids.head, commit_from, commit_count, count+1, list:+firstCommit)
            getCommit(project_id, firstCommit.parent_ids(1), commit_from, commit_count, count+1, list:+firstCommit)
          }
        }

      }
    } else{
      println("Response code", response.code)
      list
          }
  }

  conn.close()

}

