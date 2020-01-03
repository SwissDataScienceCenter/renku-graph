/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.{DateCreatedInProject, Identifier}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{FullProjectPath, ProjectPath}
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.DecodingFailure
import model._

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try

private class ProjectsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger) {

  import ProjectsFinder._

  def findProjects(identifier: Identifier): IO[List[DatasetProject]] =
    queryExpecting[List[DatasetProject]](using = query(identifier))

  private def query(identifier: Identifier): String =
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX dcterms: <http://purl.org/dc/terms/>
       |PREFIX prov: <http://www.w3.org/ns/prov#>
       |
       |SELECT DISTINCT ?isPartOf ?name ?minDateCreated ?agentEmail ?agentName
       |WHERE {
       |  {
       |    SELECT DISTINCT ?isPartOf (MIN(?dataset) as ?datasetResource) (MIN(?dateCreated) as ?minDateCreated)
       |    WHERE {
       |      ?dataset rdf:type <http://schema.org/Dataset> ;
       |               (prov:qualifiedGeneration/prov:activity) ?activity .
       |      ?activity schema:isPartOf ?isPartOf ;
       |		            prov:startedAtTime ?dateCreated .
       |      {
       |        SELECT ?dataset ?isPartOf
       |        WHERE {
       |          ?dataset schema:identifier "$identifier" ;
       |                   rdf:type <http://schema.org/Dataset> ;
       |                   dcterms:isPartOf|schema:isPartOf ?isPartOf .
       |        }
       |      }
       |    }
       |    GROUP BY ?isPartOf
       |  }
       |  {
       |    ?datasetResource rdf:type <http://schema.org/Dataset> ;
       |                     (prov:qualifiedGeneration/prov:activity) ?activity .
       |    ?activity schema:isPartOf ?isPartOf ;
       |		          prov:startedAtTime ?minDateCreated ;
       |		          prov:agent ?agentResource .
       |    ?agentResource rdf:type <http://schema.org/Person> ;
       |                   schema:email ?agentEmail ;
       |                   schema:name ?agentName .
       |    ?isPartOf rdf:type <http://schema.org/Project> ;
       |              schema:name ?name .
       |  }
       |}
       |ORDER BY ASC(?name)
       |""".stripMargin
}

private object ProjectsFinder {

  import io.circe.Decoder

  private implicit val projectsDecoder: Decoder[List[DatasetProject]] = {
    import ch.datascience.graph.model.users.{Email, Name => UserName}
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def toProjectPath(projectPath: FullProjectPath) =
      projectPath
        .as[Try, ProjectPath]
        .toEither
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))

    implicit val projectDecoder: Decoder[DatasetProject] = { cursor =>
      for {
        path        <- cursor.downField("isPartOf").downField("value").as[FullProjectPath].flatMap(toProjectPath)
        name        <- cursor.downField("name").downField("value").as[projects.Name]
        dateCreated <- cursor.downField("minDateCreated").downField("value").as[DateCreatedInProject]
        agentEmail  <- cursor.downField("agentEmail").downField("value").as[Email]
        agentName   <- cursor.downField("agentName").downField("value").as[UserName]
      } yield DatasetProject(path, name, DatasetInProjectCreation(dateCreated, DatasetAgent(agentEmail, agentName)))
    }

    _.downField("results").downField("bindings").as(decodeList[DatasetProject])
  }
}
