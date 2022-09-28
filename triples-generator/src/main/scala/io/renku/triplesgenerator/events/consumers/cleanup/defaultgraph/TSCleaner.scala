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

package io.renku.triplesgenerator.events.consumers.cleanup
package defaultgraph

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.graph.config._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
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
  ): F[TSCleaner[F]] = for {
    renkuConnectionConfig <- RenkuConnectionConfig[F]()
    renkuUrl              <- RenkuUrlLoader[F]()
  } yield new TSCleanerImpl[F](renkuConnectionConfig, renkuUrl, retryInterval, maxRetries, idleTimeout, requestTimeout)
}

private class TSCleanerImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    renkuConnectionConfig: RenkuConnectionConfig,
    renkuUrl:              RenkuUrl,
    retryInterval:         FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:            Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:           Duration = 16 minutes,
    requestTimeout:        Duration = 15 minutes
) extends TSClientImpl(renkuConnectionConfig,
                       retryInterval = retryInterval,
                       maxRetries = maxRetries,
                       idleTimeoutOverride = idleTimeout.some,
                       requestTimeoutOverride = requestTimeout.some
    )
    with TSCleaner[F] {
  import SameAsHierarchyFixer._
  import io.renku.graph.model.Schemas._

  private implicit val baseUrl:     RenkuUrl              = renkuUrl
  private implicit val storeConfig: RenkuConnectionConfig = renkuConnectionConfig

  override def removeTriples(path: projects.Path): F[Unit] =
    relinkSameAsHierarchy(path) >>
      relinkProjectHierarchy(path) >>
      removeProjectInternalEntities(path)

  private def relinkProjectHierarchy(projectPath: projects.Path) = updateWithNoResult {
    SparqlQuery.of(
      name = "project re-linking hierarchy",
      Prefixes of (renku -> "renku", prov -> "prov", schema -> "schema"),
      s"""
      DELETE 
      WHERE { 
        ?projectId renku:projectPath '$projectPath'.
        ?s prov:wasDerivedFrom ?projectId
      } 
       """
    )
  }

  private def removeProjectInternalEntities(projectPath: projects.Path): F[Unit] = updateWithNoResult {
    SparqlQuery.of(
      name = "project removal",
      Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
      s"""
        DELETE {
          ?s ?p ?o .
        } WHERE {
          {
            SELECT ?s (count(?ss) AS ?linkedToProject)
            WHERE {
              {
                ${projects.ResourceId(projectPath).showAs[RdfResource]} (renku:hasActivity | prov:qualifiedAssociation |
                  prov:qualifiedUsage | renku:hasPlan | renku:hasArguments | renku:hasInputs | renku:hasOutputs |
                  renku:parameter | schema:valueReference |  ^prov:activity | ^prov:qualifiedGeneration | prov:agent |
                  schema:creator | schema:member | schema:sameAs | prov:wasAssociatedWith | renku:hasDataset | 
                  schema:image | schema:hasPart | prov:entity | ^schema:about | ^schema:url)* ?s .
  
                OPTIONAL {
                  ?ss (<>|!<>)* ?s.
                  ?ss a schema:Project. 
                }
              } UNION {
                # sameAs values if DS shared on other projects (forks) needs to added to raise the count above 1 
                ${projects.ResourceId(projectPath).showAs[RdfResource]} renku:hasDataset ?dsId .
                ?projectId renku:hasDataset ?dsId.
                FILTER (?projectId != ${projects.ResourceId(projectPath).showAs[RdfResource]})
                ?ss schema:sameAs/schema:url ?dsId.
                ?ss schema:sameAs ?s
              }
            }
            GROUP BY ?s 
          } FILTER (?linkedToProject <= 1)
          ?s ?p ?o .
        }"""
    )
  }
}
