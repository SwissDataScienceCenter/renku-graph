package ch.datascience.webhookservice

import ch.datascience.tinytypes.StringValue
import ch.datascience.tinytypes.constraints.NonBlank
import play.api.libs.json.{ JsError, JsValue, Json, Writes }

case class ErrorResponse( value: String ) extends StringValue with NonBlank

object ErrorResponse {

  implicit val errorResponseWrites: Writes[ErrorResponse] = Writes[ErrorResponse] {
    case ErrorResponse( message ) => Json.obj( "error" -> message )
  }

  def apply( jsError: JsError ): ErrorResponse = {
    val errorMessage = jsError.errors.foldLeft( "Json deserialization error(s):" ) {
      case ( message, ( path, pathErrors ) ) =>
        s"$message\n $path -> ${pathErrors.map( _.message ).mkString( "; " )}"
    }
    ErrorResponse( errorMessage )
  }

  implicit class ErrorResponseOps( errorResponse: ErrorResponse ) {
    lazy val toJson: JsValue = Json.toJson( errorResponse )
  }
}
