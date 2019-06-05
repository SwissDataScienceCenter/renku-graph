package ch.datascience.graphservice.lineage

import cats.MonadError
import cats.effect.{Effect, IO}
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.graphservice.graphql.{QueryRunner, UserQuery}
import ch.datascience.graphservice.lineage.queries.lineageQuerySchema
import io.circe.Json
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityDecoder, Request, Response}
import sangria.execution.QueryAnalysisError
import sangria.parser.QueryParser
import sangria.renderer.SchemaRenderer.renderSchema

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal

class LineageEndpoint[Interpretation[_]: Effect](
    queryRunner: QueryRunner[Interpretation, LineageRepository[Interpretation]]
)(implicit ME:   MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import ErrorMessage._
  import LineageEndpoint._
  import org.http4s.circe._

  def schema: Interpretation[Response[Interpretation]] =
    for {
      lineageSchema <- ME.fromTry(Try(renderSchema(lineageQuerySchema)))
      result        <- Ok(lineageSchema)
    } yield result

  def findLineage(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
    for {
      query    <- request.as[UserQuery] recoverWith badRequest
      result   <- queryRunner run query recoverWith badRequestForInvalidQuery
      response <- Ok(result)
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[UserQuery]] = {
    case NonFatal(exception) => ME.raiseError(BadRequestError(exception))
  }

  private lazy val badRequestForInvalidQuery: PartialFunction[Throwable, Interpretation[Json]] = {
    case exception: QueryAnalysisError => ME.raiseError(BadRequestError(exception))
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) => BadRequest(ErrorMessage(exception.getMessage))
    case NonFatal(exception)        => InternalServerError(ErrorMessage(exception.getMessage))
  }

  private implicit lazy val queryEntityDecoder: EntityDecoder[Interpretation, UserQuery] =
    jsonOf[Interpretation, UserQuery]
}

private object LineageEndpoint {

  import io.circe._

  implicit val queryDecoder: Decoder[UserQuery] = (cursor: HCursor) => {
    for {
      query <- cursor
                .downField("query")
                .as[String]
                .flatMap(rawQuery => Either.fromTry(QueryParser.parse(rawQuery)))
                .leftMap(exception => DecodingFailure(exception.getMessage, Nil))
      operation <- cursor.downField("operationName").as[Option[String]]
    } yield UserQuery(query, operation)
  }
}

object IOLineageEndpoint {

  def apply()(implicit executionContext: ExecutionContext): IO[LineageEndpoint[IO]] =
    for {
      lineageRepository <- IOLineageRepository()
    } yield
      new LineageEndpoint[IO](
        new QueryRunner[IO, IOLineageRepository](lineageQuerySchema, lineageRepository)
      )
}
