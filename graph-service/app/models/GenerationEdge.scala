package models

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class GenerationEdge(
    // from
    entityId: String,
    // to
    activityId: String
)

object GenerationEdge {
  implicit lazy val format: OFormat[GenerationEdge] = (
    ( JsPath \ "entityId" ).format[String] and
    ( JsPath \ "activityId" ).format[String]
  )( GenerationEdge.apply, unlift( GenerationEdge.unapply ) )
}
