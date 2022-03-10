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
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.datasets.ImageUri
import io.renku.graph.model.{GitLabUrl, persons, projects}
import io.renku.http.rest.Links.Href
import io.renku.http.rest.SortBy.Direction
import io.renku.http.rest.paging.{PagingHeaders, PagingRequest, PagingResponse}
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.entities.Endpoint.Criteria
import io.renku.knowledgegraph.entities.finder.EntitiesFinder
import io.renku.rdfstore.SparqlQueryTimeRecorder
import io.renku.tinytypes.constraints.{LocalDateNotInTheFuture, NonBlank}
import io.renku.tinytypes.{LocalDateTinyType, StringTinyType, TinyTypeFactory}
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.io.OptionalValidatingQueryParamDecoderMatcher
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

    final case class Filters(maybeQuery:      Option[Filters.Query] = None,
                             maybeEntityType: Option[Filters.EntityType] = None,
                             maybeCreator:    Option[persons.Name] = None,
                             maybeVisibility: Option[projects.Visibility] = None,
                             maybeDate:       Option[Filters.Date] = None
    )

    object Filters {

      final class Query private (val value: String) extends AnyVal with StringTinyType
      object Query extends TinyTypeFactory[Query](new Query(_)) with NonBlank {
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
            EntityType.from(value.value).leftMap(_ => parsingFailure(entityType.parameterName)).toValidatedNel

        object entityType extends OptionalValidatingQueryParamDecoderMatcher[EntityType]("type") {
          val parameterName: String = "type"
        }
      }

      object CreatorName {
        private implicit val creatorNameParameterDecoder: QueryParamDecoder[persons.Name] =
          (value: QueryParameterValue) =>
            persons.Name.from(value.value).leftMap(_ => parsingFailure(creatorName.parameterName)).toValidatedNel

        object creatorName extends OptionalValidatingQueryParamDecoderMatcher[persons.Name]("creator") {
          val parameterName: String = "creator"
        }
      }

      object Visibility {
        private implicit val visibilityParameterDecoder: QueryParamDecoder[projects.Visibility] =
          (value: QueryParameterValue) =>
            projects.Visibility.from(value.value).leftMap(_ => parsingFailure(visibility.parameterName)).toValidatedNel

        object visibility extends OptionalValidatingQueryParamDecoderMatcher[projects.Visibility]("visibility") {
          val parameterName: String = "visibility"
        }
      }

      private object EntityTypeApply extends (String => EntityType) {
        override def apply(value: String): EntityType = EntityType.all.find(_.value == value).getOrElse {
          throw new IllegalArgumentException(s"'$value' unknown EntityType")
        }
      }

      final class Date private (val value: LocalDate) extends AnyVal with LocalDateTinyType
      object Date extends TinyTypeFactory[Date](new Date(_)) with LocalDateNotInTheFuture {
        private implicit val dateParameterDecoder: QueryParamDecoder[Date] =
          (value: QueryParameterValue) =>
            Either
              .catchNonFatal(LocalDate.parse(value.value))
              .flatMap(Date.from)
              .leftMap(_ => parsingFailure(date.parameterName))
              .toValidatedNel

        object date extends OptionalValidatingQueryParamDecoderMatcher[Date]("date") {
          val parameterName: String = "date"
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

  def apply[F[_]: Async: NonEmptyParallel: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[Endpoint[F]] = for {
    entitiesFinder    <- EntitiesFinder(timeRecorder)
    renkuResourcesUrl <- renku.ResourcesUrl()
    gitLabUrl         <- GitLabUrlLoader[F]()
  } yield new EndpointImpl(entitiesFinder, renkuResourcesUrl, gitLabUrl)
}

private class EndpointImpl[F[_]: Async: Logger](finder: EntitiesFinder[F],
                                                renkuResourcesUrl: renku.ResourcesUrl,
                                                gitLabUrl:         GitLabUrl
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
    implicit val resourceUrl: renku.ResourceUrl = renkuResourcesUrl / request.uri.show
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
              Link(Rel("details") -> ProjectEndpoint.href(renkuResourcesUrl, project.path))
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
              Link(Rel("details") -> DatasetEndpoint.href(renkuResourcesUrl, ds.identifier))
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
