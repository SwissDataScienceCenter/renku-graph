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
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.DecodingFailure

import scala.concurrent.ExecutionContext
import scala.language.higherKinds
import scala.util.Try

private class ProjectsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder) {

  import ProjectsFinder._

  def findProjects(identifier: Identifier): IO[List[DatasetProject]] =
    queryExpecting[List[DatasetProject]](using = query(identifier))

  private def query(identifier: Identifier) = SparqlQuery(
    name = "ds by id - projects",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>"
    ),
    s"""|SELECT ?projectId ?projectName ?minDateCreated ?agentEmail ?agentName
        |WHERE {
        |  {
        |    SELECT ?l0 ?projectId (MIN(?dateCreated) AS ?minDateCreated)
        |    WHERE {
        |      {
        |        SELECT ?topmostSameAs
        |        WHERE {
        |          {
        |            ?l0 rdf:type <http://schema.org/Dataset>;
        |                schema:identifier "$identifier"
        |          } {
        |            {
        |              {
        |                ?l0 schema:sameAs+/schema:url ?l1.
        |                FILTER NOT EXISTS { ?l1 schema:sameAs ?l2 }
        |                BIND (?l1 AS ?topmostSameAs)
        |              } UNION {
        |                ?l0 rdf:type <http://schema.org/Dataset>.
        |                FILTER NOT EXISTS { ?l0 schema:sameAs ?l1 }
        |                BIND (?l0 AS ?topmostSameAs)
        |              }
        |            } UNION {
        |              ?l0 schema:sameAs+/schema:url ?l1.
        |              ?l1 schema:sameAs+/schema:url ?l2
        |              FILTER NOT EXISTS { ?l2 schema:sameAs ?l3 }
        |              BIND (?l2 AS ?topmostSameAs)
        |            } UNION {
        |              ?l0 schema:sameAs+/schema:url ?l1.
        |              ?l1 schema:sameAs+/schema:url ?l2.
        |              ?l2 schema:sameAs+/schema:url ?l3
        |              FILTER NOT EXISTS { ?l3 schema:sameAs ?l4 }
        |              BIND (?l3 AS ?topmostSameAs)
        |            }
        |          }
        |        }
        |        GROUP BY ?topmostSameAs
        |        HAVING (COUNT(*) > 0)
        |      } {
        |        {
        |          {
        |            ?l0 schema:sameAs+/schema:url ?topmostSameAs;
        |                schema:isPartOf ?projectId;
        |                prov:qualifiedGeneration/prov:activity/prov:startedAtTime ?dateCreated.
        |            FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l2 }
        |          } UNION {
        |            ?topmostSameAs rdf:type <http://schema.org/Dataset>;
        |                           schema:isPartOf ?projectId;
        |                           prov:qualifiedGeneration/prov:activity/prov:startedAtTime ?dateCreated.
        |            FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l1 }
        |            BIND (?topmostSameAs AS ?l0)
        |          }
        |        } UNION {
        |          ?l0 schema:sameAs+/schema:url ?l1;
        |              schema:isPartOf ?projectId;
        |              prov:qualifiedGeneration/prov:activity/prov:startedAtTime ?dateCreated.
        |          ?l1 schema:sameAs+/schema:url ?topmostSameAs
        |          FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l3 }
        |        } UNION {
        |          ?l0 schema:sameAs+/schema:url ?l1;
        |              schema:isPartOf ?projectId;
        |              prov:qualifiedGeneration/prov:activity/prov:startedAtTime ?dateCreated.
        |          ?l1 schema:sameAs+/schema:url ?l2.
        |          ?l2 schema:sameAs+/schema:url ?topmostSameAs
        |          FILTER NOT EXISTS { ?topmostSameAs schema:sameAs ?l4 }
        |        }
        |      }
        |    }
        |    GROUP BY ?l0 ?projectId
        |    HAVING (MIN(?dateCreated) != 0)
        |  } {
        |    ?l0 rdf:type <http://schema.org/Dataset> ;
        |        schema:isPartOf ?projectId;
        |        prov:qualifiedGeneration/prov:activity ?activityId .
        |    ?activityId prov:startedAtTime ?minDateCreated ;
        |                prov:agent ?agentId .
        |    ?projectId rdf:type <http://schema.org/Project> ;
        |               schema:name ?projectName .
        |    ?agentId rdf:type <http://schema.org/Person> ;
        |             schema:email ?agentEmail ;
        |             schema:name ?agentName .
        |  }
        |}
        |GROUP BY ?projectId ?projectName ?minDateCreated ?agentEmail ?agentName
        |ORDER BY ASC(?projectName)
        |""".stripMargin
  )
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
