package persistence

import java.io.InputStream

import play.api.libs.json.{ JsResult, Json, Reads }

import scala.util.Try

object Utils {
  def loadJsonData[T : Reads]( is: InputStream ): Try[Seq[T]] = {
    val json = Json.parse( is )
    JsResult.toTry( json.validate[Seq[T]] )
  }

  def loadJsonData[T : Reads]( path: String ): Try[Seq[T]] = {
    val is = getClass.getResourceAsStream( path )
    loadJsonData( is )
  }
}
