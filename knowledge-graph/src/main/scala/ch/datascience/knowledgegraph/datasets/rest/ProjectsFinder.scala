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

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets.{DateCreatedInProject, Identifier}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{ProjectPath, ProjectResource}
import ch.datascience.knowledgegraph.datasets.model.{AddedToProject, DatasetAgent, DatasetProject}
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.DecodingFailure

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
    s"""|PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        |PREFIX schema: <http://schema.org/>
        |PREFIX prov: <http://www.w3.org/ns/prov#>
        |
        |SELECT ?projectId ?projectName ?minDateCreated ?agentEmail ?agentName
        |WHERE {
        |  { # finding datasets having the same sameAs but not pointing to a dataset id from a renku project
        |    ?dsId rdf:type <http://schema.org/Dataset> ;
        |          schema:sameAs/schema:url ?sameAs ;
        |          schema:isPartOf ?projectId {
        |            SELECT ?sameAs ?projectId (MIN(?dateCreated) AS ?minDateCreated)
        |            WHERE {
        |              ?dsId rdf:type <http://schema.org/Dataset> ;
        |                    schema:sameAs/schema:url ?sameAs {
        |                      SELECT ?sameAs
        |                      WHERE {
        |                        ?datasetId schema:identifier "$identifier" ;
        |                                   rdf:type <http://schema.org/Dataset> ;
        |                                   schema:sameAs/schema:url ?sameAs .
        |                      }
        |                      GROUP BY ?sameAs
        |                    }
        |              FILTER NOT EXISTS {
        |                ?dsId schema:sameAs/schema:url ?dsWithoutSameAsId {
        |                  ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
        |                  FILTER NOT EXISTS { ?dsWithoutSameAsId schema:sameAs ?nonExistingSameAs } .
        |                }
        |              }
        |              ?dsId schema:isPartOf ?projectId ;
        |                    prov:qualifiedGeneration/prov:activity/prov:startedAtTime ?dateCreated
        |            }
        |            GROUP BY ?sameAs ?projectId
        |            HAVING (MIN(?dateCreated) != 0)
        |          }
        |    ?dsId prov:qualifiedGeneration/prov:activity ?activityId .
        |    ?projectId rdf:type <http://schema.org/Project> ;
        |               schema:name ?projectName .
        |    ?activityId prov:startedAtTime ?minDateCreated ;
        |                prov:agent ?agentId .
        |    ?agentId rdf:type <http://schema.org/Person> ;
        |             schema:email ?agentEmail ;
        |             schema:name ?agentName .
        |  } UNION { # finding datasets having the sameAs pointing to a dataset from a renku project
        |    SELECT ?projectId ?projectName ?minDateCreated ?agentEmail ?agentName
        |    WHERE {
        |      {
        |        ?derivedDsId schema:sameAs/schema:url ?dsId {
        |          ?dsId rdf:type <http://schema.org/Dataset> ;
        |                schema:identifier "$identifier" .
        |          FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
        |        }
        |      } {
        |        ?dsId rdf:type <http://schema.org/Dataset> ;
        |              schema:isPartOf ?projectId ;
        |              prov:qualifiedGeneration/prov:activity ?activityId .
        |        ?projectId rdf:type <http://schema.org/Project> ;
        |                   schema:name ?projectName .
        |        ?activityId prov:startedAtTime ?minDateCreated ;
        |                    prov:agent ?agentId .
        |        ?agentId rdf:type <http://schema.org/Person> ;
        |                 schema:email ?agentEmail ;
        |                 schema:name ?agentName .
        |      } UNION {
        |        ?derivedDsId rdf:type <http://schema.org/Dataset> ;
        |                     schema:sameAs/schema:url ?dsId ;
        |                     schema:isPartOf ?projectId ;
        |                     prov:qualifiedGeneration/prov:activity ?activityId .
        |        ?projectId rdf:type <http://schema.org/Project> ;
        |                   schema:name ?projectName .
        |        ?activityId prov:startedAtTime ?minDateCreated ;
        |                    prov:agent ?agentId .
        |        ?agentId rdf:type <http://schema.org/Person> ;
        |                 schema:email ?agentEmail ;
        |                 schema:name ?agentName .
        |      }
        |    }
        |  } UNION { # finding datasets having no sameAs set and not imported to another projects
        |    SELECT ?projectId ?projectName ?minDateCreated ?agentEmail ?agentName
        |    WHERE {
        |        ?dsId schema:identifier "$identifier" .
        |        FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
        |        FILTER NOT EXISTS { ?derivedDsId schema:sameAs/schema:url ?dsId } .
        |        ?dsId rdf:type <http://schema.org/Dataset> ;
        |              schema:isPartOf ?projectId ;
        |              prov:qualifiedGeneration/prov:activity ?activityId .
        |        ?projectId rdf:type <http://schema.org/Project> ;
        |                   schema:name ?projectName .
        |        ?activityId prov:startedAtTime ?minDateCreated ;
        |                prov:agent ?agentId .
        |        ?agentId rdf:type <http://schema.org/Person> ;
        |                 schema:email ?agentEmail ;
        |                 schema:name ?agentName .
        |    }
        |  }
        |}
        |GROUP BY ?projectId ?projectName ?minDateCreated ?agentEmail ?agentName
        |ORDER BY ASC(?projectName)
        |""".stripMargin
}

private object ProjectsFinder {

  import io.circe.Decoder

  private implicit val projectsDecoder: Decoder[List[DatasetProject]] = {
    import ch.datascience.graph.model.users.{Email, Name => UserName}
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def toProjectPath(projectPath: ProjectResource) =
      projectPath
        .as[Try, ProjectPath]
        .toEither
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))

    implicit val projectDecoder: Decoder[DatasetProject] = { cursor =>
      for {
        path        <- cursor.downField("projectId").downField("value").as[ProjectResource].flatMap(toProjectPath)
        name        <- cursor.downField("projectName").downField("value").as[projects.Name]
        dateCreated <- cursor.downField("minDateCreated").downField("value").as[DateCreatedInProject]
        agentEmail  <- cursor.downField("agentEmail").downField("value").as[Email]
        agentName   <- cursor.downField("agentName").downField("value").as[UserName]
      } yield DatasetProject(path, name, AddedToProject(dateCreated, DatasetAgent(agentEmail, agentName)))
    }

    _.downField("results").downField("bindings").as(decodeList[DatasetProject])
  }
}
