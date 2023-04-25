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

package io.renku.entities.viewings.collector
package datasets

import cats.effect.Async
import cats.syntax.all._
import cats.MonadThrow
import eu.timepit.refined.auto._
import io.renku.entities.viewings.collector.persons.Dataset
import io.renku.graph.model.{datasets, projects}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait DSInfoFinder[F[_]] {
  def findDSInfo(identifier: datasets.Identifier): F[Option[DSInfo]]
}

private final case class DSInfo(projectPath: projects.Path, dataset: Dataset)

private object DSInfoFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[DSInfoFinder[F]] =
    ProjectsConnectionConfig[F]().map(TSClient[F](_)).map(new DSInfoFinderImpl[F](_))
}

private class DSInfoFinderImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends DSInfoFinder[F] {

  import io.circe.Decoder
  import io.renku.graph.model.Schemas.{renku, schema}
  import io.renku.triplesstore.{ResultsDecoder, SparqlQuery}
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.ResultsDecoder._
  import tsClient._

  override def findDSInfo(identifier: datasets.Identifier): F[Option[DSInfo]] =
    findProjects(identifier)
      .map(findOldestProject)
      .map(_.map { case (path, _, dataset) => DSInfo(path, dataset) })

  private type Row = (projects.Path, projects.DateCreated, Dataset)

  private def findProjects(identifier: datasets.Identifier): F[List[Row]] = queryExpecting(
    SparqlQuery
      .ofUnsafe(
        show"${categoryName.show.toLowerCase}: find projects",
        Prefixes of (schema -> "schema", renku -> "renku"),
        sparql"""|SELECT ?path ?dateCreated ?dsId ?dsIdentifier
                 |WHERE {
                 |  GRAPH ?projectId {
                 |    BIND (${identifier.asObject} AS ?dsIdentifier)
                 |    ?dsId a schema:Dataset;
                 |          schema:identifier ?dsIdentifier.
                 |    ?projectId a schema:Project;
                 |               renku:projectPath ?path;
                 |               schema:dateCreated ?dateCreated.
                 |  }
                 |}
                 |""".stripMargin
      )
  )(rowsDecoder)

  private lazy val rowsDecoder: Decoder[List[Row]] = ResultsDecoder[List, Row] { implicit cur =>
    import io.renku.tinytypes.json.TinyTypeDecoders._
    for {
      path         <- extract[projects.Path]("path")
      date         <- extract[projects.DateCreated]("dateCreated")
      dsId         <- extract[datasets.ResourceId]("dsId")
      dsIdentifier <- extract[datasets.Identifier]("dsIdentifier")
    } yield (path, date, Dataset(dsId, dsIdentifier))
  }

  private lazy val findOldestProject: List[Row] => Option[Row] =
    _.sortBy(_._2).headOption
}