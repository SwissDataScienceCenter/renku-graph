package models

import play.api.libs.functional.syntax._
import play.api.libs.json.{ JsPath, OFormat }

case class UsageEdge(
    // from
    activityId: String,
    // to
    entityId: String
)

object UsageEdge {
  implicit lazy val format: OFormat[UsageEdge] = (
    ( JsPath \ "activityId" ).format[String] and
    ( JsPath \ "entityId" ).format[String]
  )( UsageEdge.apply, unlift( UsageEdge.unapply ) )
}
