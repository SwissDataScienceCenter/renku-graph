package GitlabMiner

import java.sql.ResultSet
import scala.collection.mutable.ListBuffer

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

}
