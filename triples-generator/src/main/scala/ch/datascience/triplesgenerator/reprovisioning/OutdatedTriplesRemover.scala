/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.reprovisioning

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.rdfstore.IORdfStoreClient.RdfDelete
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait OutdatedTriplesRemover[Interpretation[_]] {
  def removeOutdatedTriples(outdatedTriples: OutdatedTriples): Interpretation[Unit]
}

private class IOOutdatedTriplesRemover(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfDelete](rdfStoreConfig, logger)
    with OutdatedTriplesRemover[IO] {

  override def removeOutdatedTriples(outdatedTriples: OutdatedTriples): IO[Unit] =
    for {
      _ <- remove(outdatedTriples)
      _ <- removeOrphanAgentTriples
    } yield ()

  private def remove(triplesToRemove: OutdatedTriples): IO[Unit] = queryWitNoResult {
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |
       |DELETE { ?s ?p ?o } 
       |WHERE {
       |  {
       |    SELECT ?subject
       |    WHERE {
       |      {
       |        ?commit dcterms:isPartOf|schema:isPartOf ${triplesToRemove.projectPath.showAs[RdfResource]} .
       |        FILTER (?commit IN (${triplesToRemove.commits.map(_.showAs[RdfResource]).mkString(",")}))
       |        ?commit ?predicate ?object
       |      }
       |      {
       |        ?commit ?p ?o .
       |        BIND (?commit as ?subject)
       |      } UNION {
       |        ?commit prov:influenced ?influencedObject .
       |        BIND (?influencedObject as ?subject)
       |      } UNION {
       |        ?activitySubject prov:activity ?object .
       |        BIND (?activitySubject as ?subject)
       |      } UNION {
       |        ?generationSubject prov:qualifiedGeneration/prov:activity ?object .
       |        BIND (?generationSubject as ?subject)
       |      } UNION {
       |        ?memberSubject prov:hadMember/prov:qualifiedGeneration/prov:activity ?object .
       |        BIND (?memberSubject as ?subject)
       |      }
       |    }
       |    GROUP BY ?subject
       |  } {
       |    ?subject ?p ?o 
       |    BIND (?subject as ?s)
       |  }
       |}""".stripMargin
  }

  private def removeOrphanAgentTriples: IO[Unit] = queryWitNoResult {
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |
       |DELETE { ?s ?p ?o } 
       |WHERE {
       |  {
       |    ?agentS rdf:type <http://purl.org/dc/terms/SoftwareAgent> .
       |    FILTER NOT EXISTS { ?tripleS prov:agent ?agentS }
       |  }
       |  {
       |    ?agentS ?p ?o .
       |    BIND (?agentS as ?s)
       |  }
       |}""".stripMargin
  }
}
