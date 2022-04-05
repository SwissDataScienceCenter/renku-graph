/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
import cats.effect._
import cats.effect.kernel.Concurrent
import cats.effect.unsafe.IORuntime
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

trait QueryEndpoint[F[_]] {
  def schema(): F[Response[F]]

  def handleQuery(request: Request[F], maybeUser: Option[AuthUser]): F[Response[F]]
}

class QueryEndpointImpl[F[_]: Concurrent](
    queryRunner: QueryRunner[F, LineageQueryContext[F]]
) extends Http4sDsl[F]
    with QueryEndpoint[F] {

  import ErrorMessage._
  import QueryEndpointImpl._
  import org.http4s.circe._

  def schema(): F[Response[F]] =
    for {
      schema <- MonadThrow[F].fromTry(Try(renderSchema(queryRunner.schema)))
      result <- Ok(schema)
    } yield result

  def handleQuery(request: Request[F], maybeUser: Option[AuthUser]): F[Response[F]] = {
    for {
      query <- request.as[UserQuery] recoverWith badRequest
      result <-
        queryRunner run (query, maybeUser) recoverWith badRequestForInvalidQuery
      response <- Ok(result)
    } yield response
  } recoverWith httpResponse

  private lazy val badRequest: PartialFunction[Throwable, F[UserQuery]] = { case NonFatal(exception) =>
    BadRequestError(exception).raiseError[F, UserQuery]
  }

  private lazy val badRequestForInvalidQuery: PartialFunction[Throwable, F[Json]] = {
    case exception: QueryAnalysisError => BadRequestError(exception).raiseError[F, Json]
  }

  private case class BadRequestError(cause: Throwable) extends Exception(cause)

  private lazy val httpResponse: PartialFunction[Throwable, F[Response[F]]] = {
    case BadRequestError(exception) => BadRequest(ErrorMessage(exception))
    case NonFatal(exception)        => InternalServerError(ErrorMessage(exception))
  }

  private implicit lazy val queryEntityDecoder: EntityDecoder[F, UserQuery] =
    jsonOf[F, UserQuery]
}

object QueryEndpointImpl {

  import io.circe._

  private implicit val queryDecoder: Decoder[UserQuery] = cursor =>
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

object QueryEndpoint {

  import io.renku.rdfstore.SparqlQueryTimeRecorder

  def apply()(implicit
      executionContext: ExecutionContext,
      runtime:          IORuntime,
      logger:           Logger[IO],
      timeRecorder:     SparqlQueryTimeRecorder[IO]
  ): IO[QueryEndpoint[IO]] = for {
    queryContext <- LineageQueryContext[IO]
    querySchema = QuerySchema[IO](QueryFields())
    queryRunner <- QueryRunner[IO, LineageQueryContext[IO]](querySchema, queryContext)
  } yield new QueryEndpointImpl[IO](queryRunner)
}
