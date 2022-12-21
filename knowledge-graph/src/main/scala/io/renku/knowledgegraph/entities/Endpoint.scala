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

package io.renku.knowledgegraph.entities

import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.config.renku
import io.renku.graph.config.{GitLabUrlLoader, RenkuUrlLoader}
import io.renku.graph.model._
import io.renku.http.rest.paging.{PagingHeaders, PagingResponse}
import io.renku.knowledgegraph.entities.finder.EntitiesFinder
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.io.{OptionalMultiQueryParamDecoderMatcher, OptionalValidatingQueryParamDecoderMatcher}
import org.http4s.{EntityEncoder, Header, ParseFailure, QueryParamDecoder, QueryParameterValue, Request, Response, Status}
import org.typelevel.log4cats.Logger

import java.time.LocalDate
import scala.util.control.NonFatal

trait Endpoint[F[_]] {
  def `GET /entities`(criteria: Criteria, request: Request[F]): F[Response[F]]
}

object Endpoint {

  import Criteria.Filters._

  object Decoders {
    private implicit val queryParameterDecoder: QueryParamDecoder[Query] =
      (value: QueryParameterValue) =>
        Query.from(value.value).leftMap(_ => parsingFailure(query.parameterName)).toValidatedNel

    object query extends OptionalValidatingQueryParamDecoderMatcher[Query]("query") {
      val parameterName: String = "query"
    }

    private implicit val entityTypeParameterDecoder: QueryParamDecoder[EntityType] =
      (value: QueryParameterValue) =>
        EntityType.from(value.value).leftMap(_ => parsingFailure(entityTypes.parameterName)).toValidatedNel

    object entityTypes extends OptionalMultiQueryParamDecoderMatcher[EntityType]("type") {
      val parameterName: String = "type"
    }

    private implicit val creatorNameParameterDecoder: QueryParamDecoder[persons.Name] =
      (value: QueryParameterValue) =>
        persons.Name.from(value.value).leftMap(_ => parsingFailure(creatorNames.parameterName)).toValidatedNel

    object creatorNames extends OptionalMultiQueryParamDecoderMatcher[persons.Name]("creator") {
      val parameterName: String = "creator"
    }

    private implicit val visibilityParameterDecoder: QueryParamDecoder[projects.Visibility] =
      (value: QueryParameterValue) =>
        projects.Visibility
          .from(value.value)
          .leftMap(_ => parsingFailure(visibilities.parameterName))
          .toValidatedNel

    object visibilities extends OptionalMultiQueryParamDecoderMatcher[projects.Visibility]("visibility") {
      val parameterName: String = "visibility"
    }

    private implicit val namespaceParameterDecoder: QueryParamDecoder[projects.Namespace] =
      (value: QueryParameterValue) =>
        projects.Namespace
          .from(value.value)
          .leftMap(_ => parsingFailure(namespaces.parameterName))
          .toValidatedNel

    object namespaces extends OptionalMultiQueryParamDecoderMatcher[projects.Namespace]("namespace") {
      val parameterName: String = "namespace"
    }

    private implicit val sinceParameterDecoder: QueryParamDecoder[Since] =
      (value: QueryParameterValue) =>
        Either
          .catchNonFatal(LocalDate.parse(value.value))
          .flatMap(Since.from)
          .leftMap(_ => parsingFailure(since.parameterName))
          .toValidatedNel

    object since extends OptionalValidatingQueryParamDecoderMatcher[Since]("since") {
      val parameterName: String = "since"
    }

    private implicit val untilParameterDecoder: QueryParamDecoder[Until] =
      (value: QueryParameterValue) =>
        Either
          .catchNonFatal(LocalDate.parse(value.value))
          .flatMap(Until.from)
          .leftMap(_ => parsingFailure(until.parameterName))
          .toValidatedNel

    object until extends OptionalValidatingQueryParamDecoderMatcher[Until]("until") {
      val parameterName: String = "until"
    }
  }

  private def parsingFailure(paramName: String) = ParseFailure(s"'$paramName' parameter with invalid value", "")

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder]: F[Endpoint[F]] = for {
    entitiesFinder <- EntitiesFinder[F]
    renkuUrl       <- RenkuUrlLoader()
    renkuApiUrl    <- renku.ApiUrl()
    gitLabUrl      <- GitLabUrlLoader[F]()
  } yield new EndpointImpl(entitiesFinder, renkuUrl, renkuApiUrl, gitLabUrl)
}

private class EndpointImpl[F[_]: Async: Logger](finder: EntitiesFinder[F],
                                                renkuUrl:    RenkuUrl,
                                                renkuApiUrl: renku.ApiUrl,
                                                gitLabUrl:   GitLabUrl
) extends Http4sDsl[F]
    with Endpoint[F] {

  import io.circe.syntax._
  import io.circe.{Encoder, Json}
  import io.renku.http.ErrorMessage
  import io.renku.http.ErrorMessage._
  import org.http4s.circe.jsonEncoderOf

  override def `GET /entities`(criteria: Criteria, request: Request[F]): F[Response[F]] =
    finder.findEntities(criteria) map toHttpResponse(request) recoverWith httpResult

  private def toHttpResponse(request: Request[F])(response: PagingResponse[model.Entity]): Response[F] = {
    implicit val resourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    Response[F](Status.Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response).toSeq.map(Header.ToRaw.rawToRaw): _*)
  }

  private implicit def modelEncoder: Encoder[model.Entity] = {
    import ModelEncoders._
    implicit val apiUrl: renku.ApiUrl = renkuApiUrl
    implicit val glUrl:  GitLabUrl    = gitLabUrl
    Encoder.instance {
      case project:  model.Entity.Project  => project.asJson
      case ds:       model.Entity.Dataset  => ds.asJson
      case workflow: model.Entity.Workflow => workflow.asJson
      case person:   model.Entity.Person   => person.asJson
    }
  }

  private implicit lazy val responseEntityEncoder: EntityEncoder[F, Json] = jsonEncoderOf[F, Json]

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage("Cross-entity search failed")
    Logger[F].error(exception)(errorMessage.value) >> InternalServerError(errorMessage)
  }
}
