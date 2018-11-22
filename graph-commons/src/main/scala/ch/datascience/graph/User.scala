package ch.datascience.graph

import play.api.libs.json.{ JsPath, OFormat }
import play.api.libs.functional.syntax._

case class User(
    username: String,
    email:    String,
    gitlabId: Option[Int]
)

object User {
  implicit lazy val format: OFormat[User] = (
    ( JsPath \ 'username ).format[String] and
    ( JsPath \ 'email ).format[String] and
    ( JsPath \ 'gitlabId ).formatNullable[Int]
  )( User.apply, unlift( User.unapply ) )
}
