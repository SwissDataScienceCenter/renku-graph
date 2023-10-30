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

package io.renku.knowledgegraph.projects.datasets.tags

import Endpoint.Criteria
import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.projects.ResourceId
import io.renku.graph.model.views.RdfResource
import io.renku.http.rest.paging.{Paging, PagingResponse}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import org.typelevel.log4cats.Logger

private trait TagsFinder[F[_]] {
  def findTags(criteria: Criteria): F[PagingResponse[model.Tag]]
}

private object TagsFinder {
  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](implicit ru: RenkuUrl): F[TagsFinder[F]] =
    ProjectsConnectionConfig[F]().map(new TagsFinderImpl(_))
}

private class TagsFinderImpl[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
    connectionConfig: ProjectsConnectionConfig
)(implicit renkuUrl: RenkuUrl)
    extends TSClientImpl[F](connectionConfig)
    with TagsFinder[F]
    with Paging[model.Tag] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.{datasets, publicationEvents}
  import io.renku.graph.model.Schemas._
  import io.renku.http.rest.paging.Paging.PagedResultsFinder
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes

  override def findTags(criteria: Criteria): F[PagingResponse[model.Tag]] = {
    implicit val resultsFinder: PagedResultsFinder[F, model.Tag] = pagedResultsFinder(query(criteria))
    findPage[F](criteria.paging)
  }

  private def query(criteria: Criteria) = SparqlQuery.of(
    name = "project ds tags search",
    Prefixes of (renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?name ?startDate ?maybeDesc ?dsIdentifier
        |WHERE {
        |  BIND (${ResourceId(criteria.projectSlug).showAs[RdfResource]} AS ?projId)
        |  Graph ?projId {
        |    ?projId renku:hasDataset ?dsId.
        |    ?dsId renku:slug '${criteria.datasetName}';
        |          schema:identifier ?dsIdentifier.
        |    ?eventId schema:about/schema:url ?dsId;
        |             schema:name ?name;
        |             schema:startDate ?startDate.
        |    OPTIONAL { ?eventId schema:description ?maybeDesc }
        |  }
        |}
        |ORDER BY DESC(?startDate)
        |""".stripMargin
  )

  private implicit lazy val tagDecoder: Decoder[model.Tag] = { implicit cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._
    for {
      name         <- extract[publicationEvents.Name]("name")
      startDate    <- extract[publicationEvents.StartDate]("startDate")
      maybeDesc    <- extract[Option[publicationEvents.Description]]("maybeDesc")
      dsIdentifier <- extract[datasets.Identifier]("dsIdentifier")
    } yield model.Tag(name, startDate, maybeDesc, dsIdentifier)
  }
}
