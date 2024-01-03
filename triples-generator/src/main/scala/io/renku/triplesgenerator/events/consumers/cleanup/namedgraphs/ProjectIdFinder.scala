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

package io.renku.triplesgenerator.events.consumers.cleanup.namedgraphs

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.entities.ProjectIdentification
import io.renku.graph.model.projects
import io.renku.triplesstore._
import io.renku.triplesstore.ResultsDecoder._
import org.typelevel.log4cats.Logger
import io.renku.graph.model.views.TinyTypeToObject._

private trait ProjectIdFinder[F[_]] {
  def findProjectId(slug: projects.Slug): F[Option[ProjectIdentification]]
}

private object ProjectIdFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): ProjectIdFinder[F] = new ProjectIdFinderImpl[F](connectionConfig)
}

private class ProjectIdFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    connectionConfig: ProjectsConnectionConfig
) extends TSClientImpl(connectionConfig)
    with ProjectIdFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore.client.syntax._

  override def findProjectId(slug: projects.Slug): F[Option[ProjectIdentification]] =
    queryExpecting[Option[projects.ResourceId]](query(slug))(encoder)
      .map(_.map(ProjectIdentification(_, slug)))

  private def query(slug: projects.Slug) =
    SparqlQuery.of(
      name = "find projectId",
      Prefixes of renku -> "renku",
      s"""
      SELECT ?projectId
      WHERE {
        GRAPH ?projectId {
          ?projectId renku:projectPath ${slug.asObject.asSparql.sparql}
        }
      }
     """
    )

  private lazy val encoder: Decoder[Option[projects.ResourceId]] =
    ResultsDecoder[Option, projects.ResourceId] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[projects.ResourceId]("projectId")
    }
}
