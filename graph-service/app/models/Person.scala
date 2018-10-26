package models

import play.api.libs.functional.syntax._
import play.api.libs.json.{ JsPath, OFormat }

case class Person(
    id:    String,
    name:  String,
    email: String
)

object Person {
  implicit lazy val format: OFormat[Person] = (
    ( JsPath \ "id" ).format[String] and
    ( JsPath \ "name" ).format[String] and
    ( JsPath \ "email" ).format[String]
  )( Person.apply, unlift( Person.unapply ) )
}
