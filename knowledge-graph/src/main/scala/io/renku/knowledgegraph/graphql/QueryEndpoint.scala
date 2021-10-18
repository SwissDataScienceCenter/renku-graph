/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.knowledgegraph.graphql

import cats.MonadError
import cats.effect._
import cats.syntax.all._
import io.circe.Json
import io.renku.http.ErrorMessage
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.lineage.graphql.QueryFields
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityDecoder, Request, Response}
import org.typelevel.log4cats.Logger
import sangria.execution.QueryAnalysisError
import sangria.parser.QueryParser
import sangria.renderer.SchemaRenderer.renderSchema

import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.util.control.NonFatal

class QueryEndpoint[Interpretation[_]: Effect: MonadThrow](
    queryRunner: QueryRunner[Interpretation, LineageQueryContext[Interpretation]]
) extends Http4sDsl[Interpretation] {

  import ErrorMessage._
  import QueryEndpoint._
  import org.http4s.circe._

  def schema(): Interpretation[Response[Interpretation]] =
    for {
      schema <- MonadError[Interpretation, Throwable].fromTry(Try(renderSchema(queryRunner.schema)))
      result <- Ok(schema)
    } yield result

  def handleQuery(request:   Request[Interpretation],
                  maybeUser: Option[AuthUser]
  ): Interpretation[Response[Interpretation]] = {
    for {
      query <- request.as[UserQuery] recoverWith badRequest
      result <-
        queryRunner run (query, maybeUser) recoverWith badRequestForInvalidQuery
      response <- Ok(result)
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, Interpretation[UserQuery]] = { case NonFatal(exception) =>
    BadRequestError(exception).raiseError[Interpretation, UserQuery]
  }

  private lazy val badRequestForInvalidQuery: PartialFunction[Throwable, Interpretation[Json]] = {
    case exception: QueryAnalysisError => BadRequestError(exception).raiseError[Interpretation, Json]
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private lazy val httpResponse: PartialFunction[Throwable, Interpretation[Response[Interpretation]]] = {
    case BadRequestError(exception) => BadRequest(ErrorMessage(exception))
    case NonFatal(exception)        => InternalServerError(ErrorMessage(exception))
  }

  private implicit lazy val queryEntityDecoder: EntityDecoder[Interpretation, UserQuery] =
    jsonOf[Interpretation, UserQuery]
}

private object QueryEndpoint {

  import io.circe._

  implicit val queryDecoder: Decoder[UserQuery] = cursor =>
    for {
      query <- cursor
                 .downField("query")
                 .as[String]
                 .flatMap(rawQuery => Either.fromTry(QueryParser.parse(rawQuery)))
                 .leftMap(exception => DecodingFailure(exception.getMessage, Nil))
      variables <- cursor
                     .downField("variables")
                     .as[Option[Map[String, Any]]]
                     .map(_.getOrElse(Map.empty))
    } yield UserQuery(query, variables)

  private implicit lazy val variablesValueDecoder: Decoder[Any] =
    _.value match {
      case v if v.isString  => v.as[String]
      case v if v.isBoolean => v.as[Boolean]
      case v if v.isNumber  => v.as[Double]
      case v                => Left(DecodingFailure(s"Cannot find a decoder for variable with value '$v'", Nil))
    }
}

object IOQueryEndpoint {

  import io.renku.rdfstore.SparqlQueryTimeRecorder

  def apply(
      timeRecorder: SparqlQueryTimeRecorder[IO],
      logger:       Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[QueryEndpoint[IO]] = for {
    queryContext <- LineageQueryContext(timeRecorder, logger)
    querySchema = QuerySchema[IO](QueryFields())
  } yield new QueryEndpoint[IO](new QueryRunner(querySchema, queryContext))
}
