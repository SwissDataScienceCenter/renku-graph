package GitlabMiner

import java.sql.ResultSet

object helperFunctions {

/*  def getColumns (table:String, conn: Connection) : List[String] = {
    val result = conn.prepareStatement(s"SELECT * FROM $table LIMIT 1").executeQuery
    var all_columns= new ListBuffer[String]()
    val column_count = result.getMetaData.getColumnCount
    for (i <- 1 to column_count ) {
      val column_name = result.getMetaData.getColumnName(i)
      all_columns += column_name
    }
    all_columns.toList
  }*/

  // Probably ok to just have the columns, if they change other stuff will break too

  val events_columns_list = List("id", "project_id", "author_id", "target_id",  "created_at",  "updated_at", "action",  "target_type")
  val push_payload_columns_list = List( "commit_count", "event_id", "action", "ref_type", "commit_from", "commit_to", "ref", "commit_title")
  val all_columns_list = events_columns_list:::push_payload_columns_list


  def getresult(result: ResultSet) = {
      (result.getInt("id"), result.getInt( "project_id"), result.getInt("author_id"), result.getInt("target_id"),
      result.getTimestamp("created_at"), result.getTimestamp("updated_at"), result.getInt("action"),
      result.getString("target_type"), result.getInt("commit_count"), result.getInt("event_id"),
      result.getInt("action"), result.getInt("ref_type"), result.getBytes("commit_from"),
      result.getBytes("commit_to"), result.getString("ref"), result.getString("commit_title")   )
  }


  //TODO is target_id needed also is it int?

  case class Event(id: Int, project_id: Int, author_id: Int, target_id: Int,
                   created_at: java.sql.Timestamp, updated_at: java.sql.Timestamp, actionE: Int, target_type: String,
                   commit_count: Int, event_id: Int, actionP: Int, ref_type: Int, commit_from:  Array[Byte],
                   commit_to : Array[Byte], ref: String, commit_title: String)
  object Event extends Function16[Int, Int, Int, Int, java.sql.Timestamp, java.sql.Timestamp, Int, String, Int, Int, Int, Int, Array[Byte], Array[Byte], String, String, Event]

  case class PushEvent(id: Int, project_id: Int, author_id: Int,
                       created_at:  java.sql.Timestamp, updated_at:  java.sql.Timestamp, commit_count: Int,
                       action: Int, ref_type: Int, commit_from:  Array[Byte],
                       commit_to : Array[Byte], ref: String, commit_title: String)

  case class SingleCommit(id: Int, short_id: Int, title: String, author_name: String,
                          author_email: String, committer_name: String, committer_email: String,
                          created_at:  java.sql.Timestamp, message: String, committed_date:  java.sql.Timestamp,
                          authored_date: java.sql.Timestamp, parent_ids: List[Int]
                         )

}
