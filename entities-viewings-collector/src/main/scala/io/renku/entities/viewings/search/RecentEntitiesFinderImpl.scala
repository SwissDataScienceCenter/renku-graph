/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.entities.viewings.search

import cats.NonEmptyParallel
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.entities.search.model.{Entity => SearchEntity}
import io.renku.entities.viewings.search.RecentEntitiesFinder.{Criteria, EntityType}
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import io.renku.triplesstore.client.sparql.Fragment
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{SparqlQuery, TSClient}
import org.typelevel.log4cats.Logger

private[search] class RecentEntitiesFinderImpl[F[_]: Async: NonEmptyParallel: Logger](
    client: TSClient[F]
) extends RecentEntitiesFinder[F]
    with Paging[SearchEntity] /* why is this using subtyping? */ {

  def findRecentlyViewedEntities(criteria: Criteria): F[PagingResponse[SearchEntity]] = {
    implicit val resultsFinder: Paging.PagedResultsFinder[F, SearchEntity] =
      client.pagedResultsFinder[SearchEntity](makeQuery(criteria))

    makeRequest(criteria).flatMap(findPage[F])
  }

  private def makeRequest(c: Criteria) =
    perPageFromCriteria(c).map(pp => PagingRequest(Page.first, pp))

  private def perPageFromCriteria(c: Criteria): F[PerPage] =
    PerPage
      .from(c.limit)
      .fold(Async[F].raiseError(_), _.pure[F])

  def makeQuery(criteria: Criteria): SparqlQuery =
    combineQuery(List(ProjectQuery, DatasetQuery).flatMap(_.apply(criteria)))

  private def combineQuery(qs: List[SparqlQuery]): SparqlQuery =
    SparqlQuery.of(
      "recent entity search - complete query",
      qs.flatMap(_.prefixes).toSet, {
        val bodies = qs
          .map(_.body)
          .map(Fragment.apply)
          .foldSmash(Fragment("{"), Fragment("} UNION {"), Fragment("}"))

        sparql"""SELECT DISTINCT
                | ${Variables.all}
                |WHERE {
                |  $bodies
                |}
                |ORDER BY DESC(${Variables.viewedDate}) 
                |""".stripMargin
      }
    )

  private implicit lazy val searchEntityDecoder: Decoder[SearchEntity] = { implicit cursor =>
    import io.renku.triplesstore.ResultsDecoder._

    extract[EntityType]("entityType") >>= {
      case EntityType.Dataset => Variables.Dataset.decoder.tryDecode(cursor)
      case EntityType.Project => Variables.Project.decoder.tryDecode(cursor)
    }
  }
}
