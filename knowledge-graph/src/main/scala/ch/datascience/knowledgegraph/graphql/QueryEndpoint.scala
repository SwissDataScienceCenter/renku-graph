/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.graphql

import cats.MonadError
import cats.effect._
import cats.implicits._
import ch.datascience.controllers.ErrorMessage
import ch.datascience.knowledgegraph.{datasets, lineage}
import io.circe.Json
import org.http4s.dsl.Http4sDsl
import org.http4s.{EntityDecoder, Request, Response}
import sangria.execution.QueryAnalysisError
import sangria.parser.QueryParser
import sangria.renderer.SchemaRenderer.renderSchema
import sangria.schema.Schema

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try
import scala.util.control.NonFatal

class QueryEndpoint[Interpretation[_]: Effect](
    querySchema: Schema[QueryContext[Interpretation], Unit],
    queryRunner: QueryRunner[Interpretation, QueryContext[Interpretation]]
)(implicit ME:   MonadError[Interpretation, Throwable])
    extends Http4sDsl[Interpretation] {

  import ErrorMessage._
  import QueryEndpoint._
  import org.http4s.circe._

  def schema: Interpretation[Response[Interpretation]] =
    for {
      schema <- ME.fromTry(Try(renderSchema(querySchema)))
      result <- Ok(schema)
    } yield result

  def handleQuery(request: Request[Interpretation]): Interpretation[Response[Interpretation]] = {
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

  def apply()(implicit executionContext: ExecutionContext,
              contextShift:              ContextShift[IO],
              timer:                     Timer[IO]): IO[QueryEndpoint[IO]] =
    for {
      queryContext <- IOQueryContext()
      querySchema = QuerySchema[IO](lineage.graphql.QueryFields(), datasets.graphql.QueryFields())
    } yield new QueryEndpoint[IO](querySchema, new QueryRunner(querySchema, queryContext))
}
