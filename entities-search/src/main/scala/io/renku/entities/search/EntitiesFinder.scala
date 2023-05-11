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

package io.renku.entities.search

import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.entities.search.Criteria.Filters._
import io.renku.entities.search.model._
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.{Paging, PagingResponse}
import io.renku.triplesstore.client.model.OrderBy
import io.renku.triplesstore.client.sparql.SparqlEncoder
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import org.typelevel.log4cats.Logger

trait EntitiesFinder[F[_]] {
  def findEntities(criteria: Criteria): F[PagingResponse[Entity]]
}

object EntitiesFinder {
  private[search] val newFinders = List(ProjectsQuery, DatasetsQuery, WorkflowsQuery, PersonsQuery)
  private[search] val oldFinders = List(ProjectsQuery, DatasetsQueryOld, WorkflowsQuery, PersonsQuery)

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder]: F[EntitiesFinder[F]] =
    ProjectsConnectionConfig[F]().map(new EntitiesFinderImpl(_, newFinders))

  def createOld[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder]: F[EntitiesFinder[F]] =
    ProjectsConnectionConfig[F]().map(new EntitiesFinderImpl(_, oldFinders))
}

private class EntitiesFinderImpl[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
    storeConfig:   ProjectsConnectionConfig,
    entityQueries: List[EntityQuery[Entity]]
) extends TSClientImpl[F](storeConfig)
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
    Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema", text -> "text", xsd -> "xsd"),
    s"""|SELECT ${entityQueries.map(_.selectVariables).combineAll.mkString(" ")}
        |WHERE {
        |  ${entityQueries.flatMap(_.query(criteria)).mkString(" UNION ")}
        |}
        |${`ORDER BY`(criteria.sorting)}
        |""".stripMargin
  )

  private def `ORDER BY`(
      sorting: Sorting[Criteria.Sort.type]
  )(implicit encoder: SparqlEncoder[OrderBy]): String = {
    def mapPropertyName(property: Criteria.Sort.SortProperty) = property match {
      case Criteria.Sort.ByName          => OrderBy.Property("LCASE(?name)")
      case Criteria.Sort.ByDate          => OrderBy.Property("xsd:dateTime(?date)")
      case Criteria.Sort.ByMatchingScore => OrderBy.Property("?matchingScore")
    }

    encoder(sorting.toOrderBy(mapPropertyName)).sparql
  }

  private implicit def recordDecoder: Decoder[Entity] = { implicit cursor =>
    import io.circe.DecodingFailure
    import io.renku.triplesstore.ResultsDecoder._

    extract[EntityType]("entityType") >>= { entityType =>
      entityQueries.flatMap(_.getDecoder(entityType)) match {
        case Nil            => DecodingFailure(s"No decoder for $entityType", Nil).asLeft
        case decoder :: Nil => cursor.as(decoder)
        case _              => DecodingFailure(s"Multiple decoders for $entityType", Nil).asLeft
      }
    }
  }
}
