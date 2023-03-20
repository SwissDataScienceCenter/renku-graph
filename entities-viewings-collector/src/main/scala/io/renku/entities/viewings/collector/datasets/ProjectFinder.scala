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

package io.renku.entities.viewings.collector.datasets

import cats.effect.Async
import cats.syntax.all._
import cats.MonadThrow
import eu.timepit.refined.auto._
import io.renku.graph.model.{datasets, projects}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait ProjectFinder[F[_]] {
  def findProject(identifier: datasets.Identifier): F[Option[projects.Path]]
}

private object ProjectFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ProjectFinder[F]] =
    ProjectsConnectionConfig[F]().map(TSClient[F](_)).map(new ProjectFinderImpl[F](_))
}

private class ProjectFinderImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends ProjectFinder[F] {

  import io.circe.Decoder
  import io.renku.graph.model.Schemas.{renku, schema}
  import io.renku.triplesstore.{ResultsDecoder, SparqlQuery}
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.ResultsDecoder._
  import tsClient._

  override def findProject(identifier: datasets.Identifier): F[Option[projects.Path]] =
    findProjects(identifier)
      .map(findOldestProject)

  private type Row = (projects.Path, projects.DateCreated)

  private def findProjects(identifier: datasets.Identifier): F[List[Row]] = queryExpecting(
    SparqlQuery
      .ofUnsafe(
        show"${categoryName.show.toLowerCase}: find projects",
        Prefixes of (schema -> "schema", renku -> "renku"),
        s"""|SELECT ?path ?dateCreated
            |WHERE {
            |  GRAPH ?projectId {
            |    ?id a schema:Dataset;
            |        schema:identifier ${identifier.asObject.asSparql.sparql}.
            |    ?projectId a schema:Project;
            |               renku:projectPath ?path;
            |               schema:dateCreated ?dateCreated.
            |  }
            |}
            |""".stripMargin
      )
  )(rowsDecoder)

  private lazy val rowsDecoder: Decoder[List[Row]] =
    ResultsDecoder[List, Row] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      (extract[projects.Path]("path") -> extract[projects.DateCreated]("dateCreated")).bisequence
    }

  private lazy val findOldestProject: List[Row] => Option[projects.Path] =
    _.sortBy(_._2).headOption.map(_._1)
}
