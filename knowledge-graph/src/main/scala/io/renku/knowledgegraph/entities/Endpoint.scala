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
import io.circe.Decoder
import io.renku.config.renku
import io.renku.graph.config.{GitLabUrlLoader, RenkuUrlLoader}
import io.renku.graph.model.datasets.ImageUri
import io.renku.graph.model._
import io.renku.http.rest.Links.Href
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.{PagingHeaders, PagingRequest, PagingResponse}
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.entities.Endpoint.Criteria
import io.renku.knowledgegraph.entities.finder.EntitiesFinder
import io.renku.triplesstore.SparqlQueryTimeRecorder
import io.renku.tinytypes.constraints.{LocalDateNotInTheFuture, NonBlank}
import io.renku.tinytypes.{LocalDateTinyType, StringTinyType, TinyTypeFactory}
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

  import Criteria._

  final case class Criteria(filters:   Filters = Filters(),
                            sorting:   Sorting.By = Sorting.default,
                            paging:    PagingRequest = PagingRequest.default,
                            maybeUser: Option[AuthUser] = None
  )

  object Criteria {

    final case class Filters(maybeQuery:   Option[Filters.Query] = None,
                             entityTypes:  Set[Filters.EntityType] = Set.empty,
                             creators:     Set[persons.Name] = Set.empty,
                             visibilities: Set[projects.Visibility] = Set.empty,
                             maybeSince:   Option[Filters.Since] = None,
                             maybeUntil:   Option[Filters.Until] = None
    )

    object Filters {

      final class Query private (val value: String) extends AnyVal with StringTinyType
      object Query extends TinyTypeFactory[Query](new Query(_)) with NonBlank[Query] {
        private implicit val queryParameterDecoder: QueryParamDecoder[Query] =
          (value: QueryParameterValue) =>
            Query.from(value.value).leftMap(_ => parsingFailure(query.parameterName)).toValidatedNel

        object query extends OptionalValidatingQueryParamDecoderMatcher[Query]("query") {
          val parameterName: String = "query"
        }
      }

      sealed trait EntityType extends StringTinyType with Product with Serializable
      object EntityType extends TinyTypeFactory[EntityType](EntityTypeApply) {

        final case object Project  extends EntityType { override val value: String = "project" }
        final case object Dataset  extends EntityType { override val value: String = "dataset" }
        final case object Workflow extends EntityType { override val value: String = "workflow" }
        final case object Person   extends EntityType { override val value: String = "person" }

        val all: List[EntityType] = Project :: Dataset :: Workflow :: Person :: Nil

        import io.renku.tinytypes.json.TinyTypeDecoders.stringDecoder

        implicit val decoder: Decoder[EntityType] = stringDecoder(EntityType)

        private implicit val entityTypeParameterDecoder: QueryParamDecoder[EntityType] =
          (value: QueryParameterValue) =>
            EntityType.from(value.value).leftMap(_ => parsingFailure(entityTypes.parameterName)).toValidatedNel

        object entityTypes extends OptionalMultiQueryParamDecoderMatcher[EntityType]("type") {
          val parameterName: String = "type"
        }
      }

      object CreatorName {
        private implicit val creatorNameParameterDecoder: QueryParamDecoder[persons.Name] =
          (value: QueryParameterValue) =>
            persons.Name.from(value.value).leftMap(_ => parsingFailure(creatorNames.parameterName)).toValidatedNel

        object creatorNames extends OptionalMultiQueryParamDecoderMatcher[persons.Name]("creator") {
          val parameterName: String = "creator"
        }
      }

      object Visibility {
        private implicit val visibilityParameterDecoder: QueryParamDecoder[projects.Visibility] =
          (value: QueryParameterValue) =>
            projects.Visibility
              .from(value.value)
              .leftMap(_ => parsingFailure(visibilities.parameterName))
              .toValidatedNel

        object visibilities extends OptionalMultiQueryParamDecoderMatcher[projects.Visibility]("visibility") {
          val parameterName: String = "visibility"
        }
      }

      private object EntityTypeApply extends (String => EntityType) {
        override def apply(value: String): EntityType = EntityType.all.find(_.value == value).getOrElse {
          throw new IllegalArgumentException(s"'$value' unknown EntityType")
        }
      }

      final class Since private (val value: LocalDate) extends AnyVal with LocalDateTinyType
      object Since extends TinyTypeFactory[Since](new Since(_)) with LocalDateNotInTheFuture[Since] {
        private implicit val dateParameterDecoder: QueryParamDecoder[Since] =
          (value: QueryParameterValue) =>
            Either
              .catchNonFatal(LocalDate.parse(value.value))
              .flatMap(Since.from)
              .leftMap(_ => parsingFailure(since.parameterName))
              .toValidatedNel

        object since extends OptionalValidatingQueryParamDecoderMatcher[Since]("since") {
          val parameterName: String = "since"
        }
      }

      final class Until private (val value: LocalDate) extends AnyVal with LocalDateTinyType
      object Until extends TinyTypeFactory[Until](new Until(_)) with LocalDateNotInTheFuture[Until] {
        private implicit val dateParameterDecoder: QueryParamDecoder[Until] =
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
    }

    object Sorting extends io.renku.http.rest.SortBy {

      type PropertyType = SortProperty

      sealed trait SortProperty extends Property

      final case object ByName          extends Property("name") with SortProperty
      final case object ByMatchingScore extends Property("matchingScore") with SortProperty
      final case object ByDate          extends Property("date") with SortProperty

      lazy val default: Sorting.By = Sorting.By(ByName, Direction.Asc)

      override lazy val properties: Set[SortProperty] = Set(ByName, ByMatchingScore, ByDate)
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

  import io.circe.literal._
  import io.circe.syntax._
  import io.circe.{Encoder, Json}
  import io.renku.http.ErrorMessage
  import io.renku.http.ErrorMessage._
  import io.renku.http.rest.Links.{Link, Rel, _links}
  import io.renku.json.JsonOps._
  import io.renku.knowledgegraph.datasets.rest.DatasetEndpoint
  import io.renku.knowledgegraph.projects.rest.ProjectEndpoint
  import io.renku.tinytypes.json.TinyTypeEncoders._
  import org.http4s.circe.jsonEncoderOf

  override def `GET /entities`(criteria: Criteria, request: Request[F]): F[Response[F]] =
    finder.findEntities(criteria) map toHttpResponse(request) recoverWith httpResult

  private def toHttpResponse(request: Request[F])(response: PagingResponse[model.Entity]): Response[F] = {
    implicit val resourceUrl: renku.ResourceUrl = renku.ResourceUrl(show"$renkuUrl${request.uri}")
    Response[F](Status.Ok)
      .withEntity(response.results.asJson)
      .putHeaders(PagingHeaders.from(response).toSeq.map(Header.ToRaw.rawToRaw): _*)
  }

  private implicit lazy val modelEncoder: Encoder[model.Entity] =
    Encoder.instance {
      case project: model.Entity.Project =>
        json"""{
          "type":          ${Criteria.Filters.EntityType.Project.value},
          "matchingScore": ${project.matchingScore},
          "name":          ${project.name},
          "path":          ${project.path},
          "namespace":     ${project.path.toNamespaces.mkString("/")},
          "visibility":    ${project.visibility},
          "date":          ${project.date},
          "keywords":      ${project.keywords}
        }"""
          .addIfDefined("creator" -> project.maybeCreator)
          .addIfDefined("description" -> project.maybeDescription)
          .deepMerge(
            _links(
              Link(Rel("details") -> ProjectEndpoint.href(renkuApiUrl, project.path))
            )
          )
      case ds: model.Entity.Dataset =>
        json"""{
          "type":          ${Criteria.Filters.EntityType.Dataset.value},
          "matchingScore": ${ds.matchingScore},
          "name":          ${ds.name},
          "visibility":    ${ds.visibility},
          "date":          ${ds.date},
          "creators":      ${ds.creators},
          "keywords":      ${ds.keywords},
          "images":        ${ds.images -> ds.exemplarProjectPath}
        }"""
          .addIfDefined("description" -> ds.maybeDescription)
          .deepMerge(
            _links(
              Link(Rel("details") -> DatasetEndpoint.href(renkuApiUrl, ds.identifier))
            )
          )
      case workflow: model.Entity.Workflow =>
        json"""{
          "type":          ${Criteria.Filters.EntityType.Workflow.value},
          "matchingScore": ${workflow.matchingScore},
          "name":          ${workflow.name},
          "visibility":    ${workflow.visibility},
          "date":          ${workflow.date},
          "keywords":      ${workflow.keywords}
        }"""
          .addIfDefined("description" -> workflow.maybeDescription)
      case person: model.Entity.Person =>
        json"""{
          "type":          ${Criteria.Filters.EntityType.Person.value},
          "matchingScore": ${person.matchingScore},
          "name":          ${person.name}
        }"""
    }

  private implicit lazy val imagesEncoder: Encoder[(List[ImageUri], projects.Path)] =
    Encoder.instance[(List[ImageUri], projects.Path)] { case (imageUris, exemplarProjectPath) =>
      Json.arr(imageUris.map {
        case uri: ImageUri.Relative =>
          json"""{
            "location": $uri
          }""" deepMerge _links(
            Link(Rel("view") -> Href(gitLabUrl / exemplarProjectPath / "raw" / "master" / uri))
          )
        case uri: ImageUri.Absolute =>
          json"""{
            "location": $uri
          }""" deepMerge _links(Link(Rel("view") -> Href(uri.show)))
      }: _*)
    }

  private implicit lazy val responseEntityEncoder: EntityEncoder[F, Json] = jsonEncoderOf[F, Json]

  private lazy val httpResult: PartialFunction[Throwable, F[Response[F]]] = { case NonFatal(exception) =>
    val errorMessage = ErrorMessage("Cross-entity search failed")
    Logger[F].error(exception)(errorMessage.value) >> InternalServerError(errorMessage)
  }
}
