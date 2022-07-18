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
package finder

import Endpoint.Criteria
import Endpoint.Criteria.Filters._
import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.{Paging, PagingResponse}
import io.renku.triplesstore.{RenkuConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import model._
import org.typelevel.log4cats.Logger

private[entities] trait EntitiesFinder[F[_]] {
  def findEntities(criteria: Criteria): F[PagingResponse[Entity]]
}

private[entities] object EntitiesFinder {
  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder]: F[EntitiesFinder[F]] =
    RenkuConnectionConfig[F]().map(new EntitiesFinderImpl(_))
}

private class EntitiesFinderImpl[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
    renkuConnectionConfig: RenkuConnectionConfig,
    entityQueries:         List[EntityQuery[Entity]] = List(ProjectsQuery, DatasetsQuery, WorkflowsQuery, PersonsQuery)
) extends TSClientImpl[F](renkuConnectionConfig)
    with EntitiesFinder[F]
    with Paging[Entity] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes

  override def findEntities(criteria: Criteria): F[PagingResponse[Entity]] = {
    implicit val resultsFinder: PagedResultsFinder[F, Entity] = pagedResultsFinder(query(criteria))
    findPage[F](criteria.paging)
  }

  private def query(criteria: Criteria) = SparqlQuery.of(
    name = "cross-entity search",
    Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema", text -> "text", xsd -> "xsd"),
    s"""|SELECT ${entityQueries.map(_.selectVariables).combineAll.mkString(" ")}
        |WHERE {
        |  ${entityQueries.flatMap(_.query(criteria)).mkString(" UNION ")}
        |}
        |${`ORDER BY`(criteria.sorting)}
        |""".stripMargin
  )

  private def `ORDER BY`(sorting: Criteria.Sorting.By): String = sorting.property match {
    case Criteria.Sorting.ByName          => s"ORDER BY ${sorting.direction}(?name)"
    case Criteria.Sorting.ByDate          => s"ORDER BY ${sorting.direction}(?date)"
    case Criteria.Sorting.ByMatchingScore => s"ORDER BY ${sorting.direction}(?matchingScore)"
  }

  private implicit lazy val recordDecoder: Decoder[Entity] = {
    import io.circe.DecodingFailure

    cursor =>
      cursor.downField("entityType").downField("value").as[EntityType] >>= { entityType =>
        entityQueries.flatMap(_.getDecoder(entityType)) match {
          case Nil            => DecodingFailure(s"No decoder for $entityType", Nil).asLeft
          case decoder :: Nil => cursor.as(decoder)
          case _              => DecodingFailure(s"Multiple decoders for $entityType", Nil).asLeft
        }
      }
  }
}
