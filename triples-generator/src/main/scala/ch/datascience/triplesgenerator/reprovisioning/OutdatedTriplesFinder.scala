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

import cats.data.OptionT
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait OutdatedTriplesFinder[Interpretation[_]] {
  def findOutdatedTriples: OptionT[Interpretation, OutdatedTriples]
}

private class IOOutdatedTriplesFinder(
    rdfStoreConfig:          RdfStoreConfig,
    executionTimeRecorder:   ExecutionTimeRecorder[IO],
    schemaVersion:           SchemaVersion,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger)
    with OutdatedTriplesFinder[IO] {

  import ch.datascience.graph.model.views.RdfResource
  import executionTimeRecorder._
  import io.circe.Decoder
  import io.circe.Decoder._

  override def findOutdatedTriples: OptionT[IO, OutdatedTriples] = OptionT {
    measureExecutionTime {
      {
        for {
          projectPath     <- maybeProjectWithOutdatedAgent orElse maybeProjectOnEntityWithOutdatedAgent orElse maybeProjectWithNoAgent
          outdatedTriples <- findOutdatedTriples(projectPath)
        } yield outdatedTriples
      }.value
    } map logExecutionTime(withMessage = "Searching for outdated triples finished")
  }

  private def maybeProjectWithOutdatedAgent = OptionT {
    queryExpecting[Option[ProjectResource]] {
      s"""
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX prov: <http://www.w3.org/ns/prov#>
         |PREFIX schema: <http://schema.org/>
         |PREFIX dcterms: <http://purl.org/dc/terms/>
         |
         |SELECT ?project
         |WHERE {
         |  ?agent rdf:type prov:SoftwareAgent ;
         |         rdfs:label ?version .
         |  FILTER (?version != "renku $schemaVersion")
         |  VALUES ?p { dcterms:isPartOf schema:isPartOf <schema:isPartOf> }
         |  ?commit prov:agent ?agent ;
         |	        rdf:type prov:Activity ;
         |	        ?p ?project .
         |}
         |LIMIT 1
         |""".stripMargin
    }
  }

  private def maybeProjectOnEntityWithOutdatedAgent = OptionT {
    queryExpecting[Option[ProjectResource]] {
      s"""
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX prov: <http://www.w3.org/ns/prov#>
         |PREFIX schema: <http://schema.org/>
         |PREFIX dcterms: <http://purl.org/dc/terms/>
         |
         |SELECT ?project
         |WHERE {
         |  ?agent rdf:type prov:SoftwareAgent ;
         |         rdfs:label ?version .
         |  FILTER (?version != "renku $schemaVersion")
         |  ?commit prov:agent ?agent ;
         |	        rdf:type prov:Activity ;
         |	        prov:influenced ?entity .
         |  VALUES ?p { dcterms:isPartOf schema:isPartOf <schema:isPartOf> }
         |  ?entity ?p ?project .
         |}
         |GROUP BY ?project
         |LIMIT 1
         |""".stripMargin
    }
  }

  private def maybeProjectWithNoAgent = OptionT {
    queryExpecting[Option[ProjectResource]] {
      """
        |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |PREFIX prov: <http://www.w3.org/ns/prov#>
        |PREFIX schema: <http://schema.org/>
        |PREFIX dcterms: <http://purl.org/dc/terms/>
        |
        |SELECT ?project
        |WHERE {
        |  VALUES ?p { dcterms:isPartOf schema:isPartOf <schema:isPartOf> }
        |  {
        |    ?commit ?p ?project ;
        |            rdf:type prov:Activity .
        |    FILTER NOT EXISTS {
        |      ?commit prov:agent ?agent .
        |      ?agent rdf:type prov:SoftwareAgent .
        |    }
        |  }
        |}
        |LIMIT 1
        |""".stripMargin
    }
  }

  private def findOutdatedTriples(projectResource: ProjectResource) = OptionT {
    queryExpecting[List[CommitIdResource]] {
      s"""
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX prov: <http://www.w3.org/ns/prov#>
         |PREFIX schema: <http://schema.org/>
         |PREFIX dcterms: <http://purl.org/dc/terms/>
         |
         |SELECT ?commit
         |WHERE {
         |  VALUES ?p { dcterms:isPartOf schema:isPartOf <schema:isPartOf> }
         |  {
         |    ?commit ?p ${projectResource.showAs[RdfResource]} ;
         |            rdf:type prov:Activity ;
         |            prov:agent ?agent .
         |    ?agent  rdf:type prov:SoftwareAgent ;
         |            rdfs:label ?version .
         |    FILTER (?version != "renku $schemaVersion")
         |  }
         |  UNION
         |  {
         |    ?commit rdf:type prov:Activity ;
         |            prov:influenced ?entity ;
         |            prov:agent ?agent .
         |    ?entity ?p ${projectResource.showAs[RdfResource]} .
         |    ?agent  rdf:type prov:SoftwareAgent ;
         |            rdfs:label ?version .
         |    FILTER (?version != "renku $schemaVersion")
         |  }
         |  UNION
         |  {
         |    ?commit ?p ${projectResource.showAs[RdfResource]} ;
         |            rdf:type prov:Activity .
         |    FILTER NOT EXISTS {
         |      ?commit prov:agent ?agent .
         |      ?agent rdf:type prov:SoftwareAgent .
         |    }
         |  }
         |}
         |LIMIT 10
         |""".stripMargin
    } map toMaybeOutdatedTriples(projectResource)
  }

  private implicit lazy val maybeProjectResourceDecoder: Decoder[Option[ProjectResource]] =
    _.downField("results")
      .downField("bindings")
      .as(decodeList(projectResources))
      .map {
        case Nil                  => None
        case projectResource +: _ => Some(projectResource)
      }

  private lazy val projectResources: Decoder[ProjectResource] =
    _.downField("project").downField("value").as[ProjectResource]

  private implicit lazy val outdatedCommitIdResources: Decoder[List[CommitIdResource]] =
    _.downField("results")
      .downField("bindings")
      .as(decodeList(commitIdResources))

  private lazy val commitIdResources: Decoder[CommitIdResource] =
    _.downField("commit").downField("value").as[CommitIdResource]

  private def toMaybeOutdatedTriples(projectPath: ProjectResource): List[CommitIdResource] => Option[OutdatedTriples] = {
    case Nil             => None
    case commitResources => Some(OutdatedTriples(projectPath, commitResources.toSet))
  }
}
