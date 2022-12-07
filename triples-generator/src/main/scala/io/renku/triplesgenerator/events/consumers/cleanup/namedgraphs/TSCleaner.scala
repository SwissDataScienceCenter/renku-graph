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

package io.renku.triplesgenerator.events.consumers.cleanup.namedgraphs

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.circe.Decoder
import io.renku.graph.model.{GraphClass, projects}
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.jsonld.EntityId
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private[cleanup] trait TSCleaner[F[_]] {
  def removeTriples(path: projects.Path): F[Unit]
}

private[cleanup] object TSCleaner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
      maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
      idleTimeout:    Duration = 16 minutes,
      requestTimeout: Duration = 15 minutes
  ): F[TSCleaner[F]] = ProjectsConnectionConfig[F]().map(
    new TSCleanerImpl[F](_, retryInterval, maxRetries, idleTimeout, requestTimeout)
  )
}

private class TSCleanerImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    connectionConfig: ProjectsConnectionConfig,
    retryInterval:    FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:       Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:      Duration = 16 minutes,
    requestTimeout:   Duration = 15 minutes
) extends TSClient(connectionConfig,
                   retryInterval = retryInterval,
                   maxRetries = maxRetries,
                   idleTimeoutOverride = idleTimeout.some,
                   requestTimeoutOverride = requestTimeout.some
    )
    with TSCleaner[F] {

  import SameAsHierarchyFixer._
  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._
  import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes
  private implicit val tsConnection: ProjectsConnectionConfig = connectionConfig

  override def removeTriples(path: projects.Path): F[Unit] =
    relinkSameAsHierarchy(path) >> relinkProjectHierarchy(path) >>
      findGraphId(path) >>= removeProjectGraph

  private def relinkProjectHierarchy(projectPath: projects.Path) = updateWithNoResult {
    SparqlQuery.of(
      name = "project re-linking hierarchy",
      Prefixes of (renku -> "renku", prov -> "prov"),
      s"""
      DELETE { GRAPH ?childId { ?childId prov:wasDerivedFrom ?projectId } }
      INSERT { GRAPH ?childId { ?childId prov:wasDerivedFrom ?parentId } }
      WHERE {
        GRAPH ?projectGraphId {
          ?projectId renku:projectPath '${sparqlEncode(projectPath.show)}'
        }
        OPTIONAL { GRAPH ?projectId { ?projectId prov:wasDerivedFrom ?parentId } }
        GRAPH ?childGraphId {
          ?childId prov:wasDerivedFrom ?projectId
        }
      } 
       """
    )
  }

  private def findGraphId(projectPath: projects.Path): F[Option[EntityId]] = {
    implicit val enc: Decoder[Option[EntityId]] = ResultsDecoder[Option, EntityId] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[projects.ResourceId]("graphId").map(GraphClass.Project.id)
    }

    queryExpecting[Option[EntityId]] {
      SparqlQuery.of(
        name = "find project graphId",
        Prefixes of (renku -> "renku", prov -> "prov"),
        s"""
        SELECT ?graphId
        WHERE {
          GRAPH ?graphId {
            ?projectId renku:projectPath '${sparqlEncode(projectPath.show)}'
          }
        } 
       """
      )
    }
  }

  private def removeProjectGraph: Option[EntityId] => F[Unit] = {
    case None => ().pure[F]
    case Some(graphId) =>
      updateWithNoResult {
        SparqlQuery.of(name = "project graph removal", s"""DROP GRAPH <$graphId>""")
      }
  }
}
