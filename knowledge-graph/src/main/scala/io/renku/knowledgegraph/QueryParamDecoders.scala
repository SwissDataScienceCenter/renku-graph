/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph

import cats.data.{Validated, ValidatedNel}
import cats.syntax.all._
import io.renku.entities.search.Criteria.Filters._
import io.renku.entities.viewings.search.RecentEntitiesFinder
import io.renku.graph.model._
import io.renku.http.rest.paging.model.PerPage
import org.http4s.dsl.io.{OptionalMultiQueryParamDecoderMatcher, OptionalValidatingQueryParamDecoderMatcher}
import org.http4s.{ParseFailure, QueryParamDecoder, QueryParameterValue}

import java.time.LocalDate

object QueryParamDecoders {

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

  private implicit val recentlyViewedEntityTypesParamDecoder: QueryParamDecoder[RecentEntitiesFinder.EntityType] =
    (value: QueryParameterValue) =>
      RecentEntitiesFinder.EntityType
        .fromString(value.value)
        .leftMap(_ => parsingFailure(entityTypes.parameterName))
        .toValidatedNel

  object recentlyViewedEntityTypes
      extends OptionalMultiQueryParamDecoderMatcher[RecentEntitiesFinder.EntityType]("type") {}

  object LimitQueryParam {
    def unapply(
        params: Map[String, collection.Seq[String]]
    ): Some[Option[ValidatedNel[ParseFailure, Int]]] =
      Some {
        params
          .get("limit")
          .flatMap(_.headOption)
          .fold[Option[ValidatedNel[ParseFailure, Int]]](None) { s =>
            Some(QueryParamDecoder[Int].decode(QueryParameterValue(s)))
              .map(_.andThen {
                case n if n <= 0 => Validated.invalidNel(ParseFailure(s"Invalid limit value: $n", ""))
                case n if n > PerPage.max.value =>
                  Validated.invalidNel(ParseFailure(s"Invalid (too large) limit value: $n", ""))
                case n => Validated.validNel(n)
              })
          }
      }
  }

  private implicit val creatorNameParameterDecoder: QueryParamDecoder[persons.Name] =
    (value: QueryParameterValue) =>
      persons.Name.from(value.value).leftMap(_ => parsingFailure(creatorNames.parameterName)).toValidatedNel

  object creatorNames extends OptionalMultiQueryParamDecoderMatcher[persons.Name]("creator") {
    val parameterName: String = "creator"
  }

  object owned extends OptionalValidatingQueryParamDecoderMatcher[Boolean]("owned") {
    val parameterName: String = "owned"
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

  private def parsingFailure(paramName: String) = ParseFailure(s"'$paramName' parameter with invalid value", "")
}
