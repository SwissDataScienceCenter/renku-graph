package models

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class Entity(
    id:          String,
    path:        String,
    commit_sha1: String,
    isPlan:      Boolean
)

object Entity {
  implicit lazy val format: OFormat[Entity] = (
    ( JsPath \ "id" ).format[String] and
    ( JsPath \ "path" ).format[String] and
    ( JsPath \ "commit_sha1" ).format[String] and
    ( JsPath \ "is_plan" ).formatWithDefault[Boolean]( false )
  )( Entity.apply, unlift( Entity.unapply ) )
}
