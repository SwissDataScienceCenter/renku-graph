/*
This module reads the events table from the gitlab instance and publishes the push events that are in it.
 */
package GitlabMiner

import GitlabMiner.helperFunctions.{Event, GitSingleCommit, SingleCommit}
import java.sql.{DriverManager, ResultSet}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat
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

  for (i <- 1 to 3) {
    //while (1==1) {
    val id =  if (allevents.isEmpty){150} else { allevents.last.id }
    val prep = conn.prepareStatement(s"select * from events as e, push_event_payloads as p where p.event_id = e.id and e.id >$id;")
    val result = prep.executeQuery
    while ( result.next() ) {
      val t = (Event.apply _).tupled(helperFunctions.getresult(result) )
      // get the amount of commits: start from commit_to until commit_from loop over parents
      // count how we many commits we go back, in case of merge req

      //GET /projects/:id/repository/commits/:sha
      val response: HttpResponse[String] = Http("https://testing.datascience.ch/api/v4/projects/22/repository/commits/b8bc9bf560921c6dc027529393c57134b5619b44").asString
      val commit_count = t.commit_count

      //if commit_count = 1 get
    /*  val response: HttpResponse[Map[String,String]] = Http("https://testing.datascience.ch/api/v4/projects/22/repository/commits/b8bc9bf560921c6dc027529393c57134b5619b44").execute(parser = {inputStream =>
        Json.parse[Map[String,String]](inputStream)
      })*/

      print((response.body).parseJson)
      allevents += t

    }
  }

  def getAllCommits(project_id: Int, commit_to : String, commit_from: String, commit_count: Int, singlecommit: GitSingleCommit, count: Int, list: List[GitSingleCommit]): List[GitSingleCommit] = {
    println(commit_count)

    if (commit_count == 1 ||  commit_count == count ||
      (singlecommit.parent_ids.length == 1 && (commit_from == singlecommit.parent_ids.head)))  {
      list:+singlecommit
    }

    else {
      getCommit(project_id, singlecommit.parent_ids.head, commit_from, commit_count, count+1, list:+singlecommit)
    }
  }

  def getCommit(project_id: Int, commit_to : String, commit_from: String, commit_count: Int, count: Int, list: List[GitSingleCommit]): List[GitSingleCommit] = {
    // print(commit_to)
    val response: HttpResponse[String] =
      Http("https://testing.datascience.ch/api/v4/projects/"+ project_id + "/repository/commits/" +
        commit_to).asString
    val firstCommit = ((response.body).parseJson).convertTo[GitSingleCommit]
    return getAllCommits(project_id, commit_to : String, commit_from: String, commit_count: Int, firstCommit, count, list: List[GitSingleCommit])

  }

  implicit val gitsingleCommitFormat: RootJsonFormat[GitSingleCommit] = jsonFormat12(GitSingleCommit.apply)

  conn.close()

}

