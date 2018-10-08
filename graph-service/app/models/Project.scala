package models

import play.api.libs.functional.syntax._
import play.api.libs.json._

case class Project(
    id: String,
    url: String,
    //gitlabId: String,
)

object Project {

  implicit val format: OFormat[Project] = (
    (JsPath \ "id").format[String] and
      (JsPath \ "url").format[String]
  )(Project.apply, unlift(Project.unapply))

}
