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
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait OutdatedTriplesRemover[Interpretation[_]] {
  def removeOutdatedTriples(outdatedTriples: OutdatedTriples): Interpretation[Unit]
}

private class IOOutdatedTriplesRemover(
    rdfStoreConfig:          RdfStoreConfig,
    executionTimeRecorder:   ExecutionTimeRecorder[IO],
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger)
    with OutdatedTriplesRemover[IO] {

  import cats.implicits._
  import executionTimeRecorder._

  override def removeOutdatedTriples(outdatedTriples: OutdatedTriples): IO[Unit] =
    measureExecutionTime {
      outdatedTriples.commits.toList.map(remove).parSequence.map(_ => ())
    } map logExecutionTime(withMessage = s"Removing outdated triples for '${outdatedTriples.projectResource}' finished")

  private def remove(commitIdResource: CommitIdResource): IO[Unit] = queryWitNoResult {
    val commitResource = commitIdResource.showAs[RdfResource]
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
       |    SELECT DISTINCT ?subject
       |    WHERE {
       |      {
       |        $commitResource rdf:type prov:Activity ;
       |                        ?predicate ?object .
       |      }
       |      {
       |        $commitResource ?p ?o .
       |        BIND ($commitResource as ?subject)
       |      } UNION {
       |        $commitResource prov:influenced ?influencedObject .
       |        BIND (?influencedObject as ?subject)
       |      } UNION {
       |        ?activitySubject prov:activity ?object .
       |        BIND (?activitySubject as ?subject)
       |      } UNION {
       |        ?memberSubject prov:hadMember ?object .
       |        BIND (?memberSubject as ?subject)
       |      } UNION {
       |        $commitResource prov:influenced/prov:hadMember ?memberObject .
       |        FILTER (
       |          !EXISTS { ?memberObject rdf:type <http://schema.org/Dataset> }
       |        )
       |        BIND (?memberObject as ?subject)
       |      } UNION {
       |        $commitResource prov:influenced/prov:hadMember/schema:hasPart ?partObject .
       |        FILTER (
       |          !EXISTS { ?memberObject rdf:type <http://schema.org/DigitalDocument> }
       |        )
       |        BIND (?partObject as ?subject)
       |      } UNION {
       |        $commitResource prov:influenced/prov:hadMember/prov:qualifiedGeneration ?generationObject .
       |        BIND (?generationObject as ?subject)
       |      } UNION {
       |        ?memberSubject prov:hadMember/prov:qualifiedGeneration/prov:activity ?object .
       |        BIND (?memberSubject as ?subject)
       |      }
       |    }
       |  } {
       |    ?subject ?p ?o
       |    BIND (?subject as ?s)
       |  } UNION {
       |    # selecting isPartOf dataset triples if there are no other commits linked to that dataset project
       |    $commitResource rdf:type prov:Activity ;
       |                    schema:isPartOf ?commitProject ;
       |                    prov:influenced/prov:hadMember ?dataset .
       |    ?dataset rdf:type <http://schema.org/Dataset> ;
       |             schema:isPartOf ?commitProject .
       |    FILTER (
       |      !EXISTS {
       |        ?otherCommit rdf:type prov:Activity ;
       |                     schema:isPartOf ?commitProject .
       |        FILTER (?otherCommit != $commitResource)
       |      }
       |    )
       |    BIND (?dataset as ?s)
       |    BIND (schema:isPartOf as ?p)
       |    BIND (?commitProject as ?o)
       |  } UNION {
       |    # selecting isPartOf dataset part triples for the datasets for which isPartOf triples are selected
       |    $commitResource rdf:type prov:Activity ;
       |                    schema:isPartOf ?commitProject ;
       |                    prov:influenced/prov:hadMember ?dataset .
       |    ?dataset rdf:type <http://schema.org/Dataset> ;
       |             schema:isPartOf ?commitProject .
       |    FILTER (
       |      !EXISTS {
       |        ?otherCommit rdf:type prov:Activity ;
       |                     schema:isPartOf ?commitProject .
       |        FILTER (?otherCommit != $commitResource)
       |      }
       |    )
       |    ?dataset schema:hasPart ?part .
       |    ?part rdf:type <http://schema.org/DigitalDocument> ;
       |          schema:isPartOf ?commitProject .
       |    BIND (?part as ?s)
       |    BIND (schema:isPartOf as ?p)
       |    BIND (?commitProject as ?o)
       |  }
       |}
       |""".stripMargin
  }
}
