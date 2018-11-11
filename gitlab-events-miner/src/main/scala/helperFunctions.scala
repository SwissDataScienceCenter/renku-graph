package GitlabMiner

import java.sql.ResultSet
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat
object helperFunctions {

  // Probably ok to just have the columns, if they change other stuff will break too

  val events_columns_list = List("id", "project_id", "author_id", "target_id",  "created_at",  "updated_at", "action",  "target_type")
  val push_payload_columns_list = List( "commit_count", "event_id", "action", "ref_type", "commit_from", "commit_to", "ref", "commit_title")
  val all_columns_list = events_columns_list:::push_payload_columns_list


  def getresult(result: ResultSet) = {
    (result.getInt("id"), result.getInt( "project_id"), result.getInt("author_id"), result.getInt("target_id"),
      result.getTimestamp("created_at"), result.getTimestamp("updated_at"), result.getInt("action"),
      result.getString("target_type"), result.getInt("commit_count"), result.getInt("event_id"),
      result.getInt("action"), result.getInt("ref_type"), Option(result.getBytes("commit_from")),
      Option(result.getBytes("commit_to")), result.getString("ref"), result.getString("commit_title")   )
  }

  case class Event(id: Int, project_id: Int, author_id: Int, target_id: Int,
                   created_at: java.sql.Timestamp, updated_at: java.sql.Timestamp, actionE: Int, target_type: String,
                   commit_count: Int, event_id: Int, actionP: Int, ref_type: Int, commit_from: Option[Array[Byte]],
                   commit_to : Option[Array[Byte]], ref: String, commit_title: String)

  object Event extends Function16[Int, Int, Int, Int, java.sql.Timestamp, java.sql.Timestamp, Int, String, Int, Int, Int, Int, Option[Array[Byte]], Option[Array[Byte]], String, String, Event]

  case class GitSingleCommit(id: String, short_id: String, title: String, author_name: String,
                             author_email: String, committer_name: String, committer_email: String,
                             created_at: String, message: String, committed_date: String,
                             authored_date: String, parent_ids: List[String]
                            )


  implicit val GitSingleCommitFormat: RootJsonFormat[GitSingleCommit] = jsonFormat12(GitSingleCommit.apply)

}
