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

package io.renku.triplesgenerator.events.consumers.cleanup.namedgraphs

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private object ProjectHierarchyFixer {
  def relinkProjectHierarchy[F[_]: Async: Logger: SparqlQueryTimeRecorder](path: projects.Path)(implicit
      connectionConfig: ProjectsConnectionConfig
  ): F[Unit] = MonadThrow[F].catchNonFatal(new ProjectHierarchyFixer[F](path)(connectionConfig)) >>= (_.run())
}

private class ProjectHierarchyFixer[F[_]: Async: Logger: SparqlQueryTimeRecorder](projectPath: projects.Path)(
    connectionConfig: ProjectsConnectionConfig
) extends TSClientImpl(connectionConfig,
                       idleTimeoutOverride = (11 minutes).some,
                       requestTimeoutOverride = (10 minutes).some
    ) {

  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore.client.syntax._

  def run(): F[Unit] = updateWithNoResult {
    SparqlQuery.of(
      name = "project re-linking hierarchy",
      Prefixes of (renku -> "renku", prov -> "prov"),
      s"""
        DELETE { GRAPH ?childId { ?childId prov:wasDerivedFrom ?projectId } }
        INSERT { GRAPH ?childId { ?childId prov:wasDerivedFrom ?parentId } }
        WHERE {
          GRAPH ?projectGraphId {
            ?projectId renku:projectPath ${projectPath.asObject.asSparql.sparql}
          }
          OPTIONAL { GRAPH ?projectId { ?projectId prov:wasDerivedFrom ?parentId } }
          GRAPH ?childGraphId {
            ?childId prov:wasDerivedFrom ?projectId
          }
        }
        """
    )
  }
}
