import java.time.format.DateTimeFormatter
import java.time.{ Instant, ZoneId }

import play.api.libs.json._

import scala.util.{ Success, Try }

package object models {
  implicit lazy val InstantFormat: Format[Instant] = Format(
    new Reads[Instant] {
      override def reads( json: JsValue ): JsResult[Instant] = json match {
        case JsString( value ) =>
          Try( Instant.parse( value ) ) match {
            case Success( instant ) => JsSuccess( instant )
            case _ =>
              Try( Instant.parse( s"${value}Z" ) ) match {
                case Success( instant ) => JsSuccess( instant )
                case _                  => JsError( "expected string representation of instant" )
              }
          }
        case _ => JsError( "expected string representation of instant" )
      }
    },
    new Writes[Instant] {
      override def writes( t: Instant ): JsValue = {
        JsString(
          t.atZone( ZoneId.of( "UTC" ) ).format( DateTimeFormatter.ISO_INSTANT )
        )
      }
    }
  )
}
