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

package io.renku.entities.searchgraphs.commands

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import org.typelevel.log4cats.Logger

private trait VisibilityFinder[F[_]] {
  def findVisibility(projectId: projects.ResourceId): F[Option[projects.Visibility]]
}

private object VisibilityFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): VisibilityFinder[F] =
    new VisibilityFinderImpl[F](connectionConfig)

  def default[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[VisibilityFinder[F]] =
    ProjectsConnectionConfig[F]().map(apply(_))
}

private class VisibilityFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig)
    extends TSClientImpl(storeConfig)
    with VisibilityFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.GraphClass
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore.client.syntax._

  override def findVisibility(projectId: projects.ResourceId): F[Option[projects.Visibility]] =
    queryExpecting[Option[projects.Visibility]](query(projectId))

  private def query(resourceId: projects.ResourceId) = SparqlQuery.of(
    name = "project visibility",
    Prefixes of (renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?visibility
        |WHERE {
        |  GRAPH ${GraphClass.Project.id(resourceId).asSparql.sparql} {
        |    ${resourceId.asEntityId.asSparql.sparql} a schema:Project;
        |                                             renku:projectVisibility ?visibility.
        |  }
        |}
        |""".stripMargin
  )

  private implicit lazy val recordsDecoder: Decoder[Option[projects.Visibility]] =
    ResultsDecoder[Option, projects.Visibility] { implicit cur =>
      extract[projects.Visibility]("visibility")
    }
}
