package models

import java.time.Instant

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class Activity(
    id:    String,
    label: String,
    //    commit_sha1: String,
    endTime: Instant
)

object Activity {
  implicit lazy val format: OFormat[Activity] = (
    ( JsPath \ "id" ).format[String] and
    ( JsPath \ "label" ).format[String] and
    //      (JsPath \ "commit_sha1").format[String] and
    ( JsPath \ "endTime" ).format[Instant]
  )( Activity.apply, unlift( Activity.unapply ) )
}
