package models

import play.api.libs.functional.syntax._
import play.api.libs.json.{ JsPath, OFormat }

case class AssociationEdge(
    // from
    activityId: String,
    // to
    agentId: String,
    // plan:
    planId: String
)

object AssociationEdge {
  implicit lazy val format: OFormat[AssociationEdge] = (
    ( JsPath \ "activityId" ).format[String] and
    ( JsPath \ "agentId" ).format[String] and
    ( JsPath \ "planId" ).format[String]
  )( AssociationEdge.apply, unlift( AssociationEdge.unapply ) )
}
