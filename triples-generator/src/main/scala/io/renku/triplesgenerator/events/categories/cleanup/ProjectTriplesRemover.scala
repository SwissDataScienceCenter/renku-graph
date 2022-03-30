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

package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import io.renku.graph.config._
import io.renku.graph.model.{RenkuBaseUrl, projects}
import io.renku.http.client.RestClient.{MaxRetriesAfterConnectionTimeout, SleepAfterConnectionIssue}
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait ProjectTriplesRemover[F[_]] {
  def removeTriples(of: projects.Path): F[Unit]
}

private object ProjectTriplesRemover {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
      maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
      idleTimeout:    Duration = 6 minutes,
      requestTimeout: Duration = 5 minutes
  ): F[ProjectTriplesRemover[F]] = for {
    rdfStoreConfig <- RdfStoreConfig[F]()
    renkuBaseUrl   <- RenkuBaseUrlLoader[F]()
  } yield new ProjectTriplesRemoverImpl[F](rdfStoreConfig,
                                           renkuBaseUrl,
                                           retryInterval,
                                           maxRetries,
                                           idleTimeout,
                                           requestTimeout
  )
}
private class ProjectTriplesRemoverImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig,
    renkuBaseUrl:   RenkuBaseUrl,
    retryInterval:  FiniteDuration = SleepAfterConnectionIssue,
    maxRetries:     Int Refined NonNegative = MaxRetriesAfterConnectionTimeout,
    idleTimeout:    Duration = 6 minutes,
    requestTimeout: Duration = 5 minutes
) extends RdfStoreClientImpl(rdfStoreConfig,
                             retryInterval = retryInterval,
                             maxRetries = maxRetries,
                             idleTimeoutOverride = idleTimeout.some,
                             requestTimeoutOverride = requestTimeout.some
    )
    with ProjectTriplesRemover[F] {
  implicit val baseUrl: RenkuBaseUrl = renkuBaseUrl

  override def removeTriples(projectPath: projects.Path): F[Unit] = for {
    _ <- relinkDatasetHierarchy(projectPath)
    _ <- relinkProjectHierarchy(projectPath)
    _ <- removeProjectInternalEntities(projectPath)
  } yield ()

  private def relinkDatasetHierarchy(projectPath: projects.Path): F[Unit] = for {
    _ <- updateWithNoResult(datasetSameAsUpdate(projectPath))
    _ <- updateWithNoResult(datasetTopmostSameAsUpdate(projectPath))
  } yield ()

  private def datasetSameAsUpdate(projectPath: projects.Path): SparqlQuery = SparqlQuery.of(
    name = "CLEAN UP - dataset re-linking topmostSameAs hierarchy",
    prefixes,
    s"""
      INSERT {
        ?directDescendant schema:sameAs ?sameAs .
      }
      WHERE {
        ?dataset ^renku:hasDataset <$renkuBaseUrl/projects/$projectPath>;
                  a schema:Dataset;
                  ^schema:url/^schema:sameAs ?directDescendant ;
                  schema:sameAs ?sameAs .
        ?directDescendant schema:sameAs ?descendantSameAs .
		?descendantSameAs schema:url ?dataset .        
      }
       """
  )
  private def datasetTopmostSameAsUpdate(projectPath: projects.Path): SparqlQuery = SparqlQuery.of(
    name = "CLEAN UP - dataset re-linking topmostSameAs hierarchy",
    prefixes,
    s"""
      DELETE {
        ?directDescendant renku:topmostSameAs ?oldTopmostSameAs .
        ?descendantSameAs schema:url ?dataset .
        ?directDescendant schema:sameAs ?descendantSameAs .
        ?otherDataset renku:topmostSameAs ?dataset .
      } 
      INSERT {
        ?directDescendant renku:topmostSameAs ?newTopmostSameAs .
        ?otherDataset renku:topmostSameAs ?newTopmostSameAs .
      }
      WHERE {
        ?dataset ^renku:hasDataset <$renkuBaseUrl/projects/$projectPath>;
                  a schema:Dataset;
                  renku:topmostSameAs ?topmostSameAs ;
                  ^schema:url/^schema:sameAs ?directDescendant .
        ?directDescendant renku:topmostSameAs ?oldTopmostSameAs ;
                          schema:sameAs ?descendantSameAs .
        ?descendantSameAs schema:url ?dataset .        
        OPTIONAL { ?dataset ^renku:topmostSameAs ?otherDataset . }
        BIND(IF(?topmostSameAs != ?dataset,  ?topmostSameAs, ?directDescendant) as ?newTopmostSameAs)
      }
       """
  )

  private def relinkProjectHierarchy(projectPath: projects.Path) = updateWithNoResult(projectLinksUpdate(projectPath))

  private def projectLinksUpdate(projectPath: projects.Path) = SparqlQuery.of(
    name = "CLEAN UP - project re-linking hierarchy",
    prefixes,
    s"""
      DELETE 
      WHERE { 
       ?s prov:wasDerivedFrom <$renkuBaseUrl/projects/$projectPath> 
      } 
       """
  )

  private def removeProjectInternalEntities(projectPath: projects.Path)(implicit
      renkuBaseUrl:                                      RenkuBaseUrl
  ): F[Unit] = updateWithNoResult(projectRemovalQuery(projectPath))

  private def projectRemovalQuery(projectPath: projects.Path) = SparqlQuery.of(
    name = "CLEAN UP - project removal",
    prefixes,
    s"""
        DELETE {
          ?s ?p ?o .
        } WHERE {
          {
            SELECT ?s (count(?ss) AS ?linkedToProject)
            WHERE {
              <$renkuBaseUrl/projects/$projectPath> (renku:hasActivity |  prov:qualifiedAssociation |
              prov:qualifiedUsage | renku:hasPlan | renku:hasArguments | renku:hasInputs | renku:hasOutputs |
              renku:parameter | schema:valueReference |  ^prov:activity | ^prov:qualifiedGeneration | prov:agent |
              schema:creator | schema:member | schema:sameAs | prov:wasAssociatedWith | renku:hasDataset | 
              schema:image |schema:hasPart | prov:entity | ^schema:url | ^schema:about)* ?s .

              OPTIONAL { ?ss (<>|!<>)* ?s.
                         ?ss a schema:Project . 
              }
            } GROUP BY ?s 
          }FILTER(?linkedToProject <= 1)
          ?s ?p ?o .
        }"""
  )

  import io.renku.graph.model.Schemas._
  private val prefixes = Prefixes.of(renku -> "renku", prov -> "prov", schema -> "schema")
}
