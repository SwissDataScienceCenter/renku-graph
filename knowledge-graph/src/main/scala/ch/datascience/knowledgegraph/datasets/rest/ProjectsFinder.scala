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
import cats.syntax.all._
import ch.datascience.graph.model.datasets.{DateCreatedInProject, Identifier}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.knowledgegraph.datasets.model.{AddedToProject, DatasetAgent, DatasetProject}
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList
import io.circe.DecodingFailure

import scala.concurrent.ExecutionContext
import scala.util.Try

private class ProjectsFinder(
    rdfStoreConfig:          RdfStoreConfig,
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
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
    ),
    s"""|SELECT DISTINCT ?projectId ?projectName ?minDateCreated ?maybeAgentEmail ?agentName
        |WHERE {
        |  {
        |    SELECT ?allDsId ?projectId(MIN(?dateCreated) AS ?minDateCreated)
        |    WHERE {
        |      ?dsId rdf:type <http://schema.org/Dataset>;
        |            schema:identifier '$identifier';
        |            renku:topmostSameAs ?topmostSameAs.
        |      ?allDsId rdf:type <http://schema.org/Dataset>;
        |               renku:topmostSameAs ?topmostSameAs;
        |               schema:isPartOf ?projectId;
        |               prov:qualifiedGeneration/prov:activity ?activityId.
        |      ?activityId prov:startedAtTime ?dateCreated.
        |    }
        |    GROUP BY ?allDsId ?projectId
        |  }
        |  ?allDsId rdf:type <http://schema.org/Dataset>;
        |           schema:isPartOf ?projectId;
        |           prov:qualifiedGeneration/prov:activity ?activityId.
        |  ?activityId prov:startedAtTime ?minDateCreated;
        |              prov:wasAssociatedWith ?associationId.
        |  ?projectId rdf:type <http://schema.org/Project>;
        |             schema:name ?projectName.
        |  ?associationId rdf:type <http://schema.org/Person>;
        |                 schema:name ?agentName.
        |  OPTIONAL { ?associationId schema:email ?maybeAgentEmail }
        |}
        |ORDER BY ASC(?projectName)
        |""".stripMargin
  )
}

private object ProjectsFinder {

  import io.circe.Decoder

  private implicit val projectsDecoder: Decoder[List[DatasetProject]] = {
    import ch.datascience.graph.model.users.{Email, Name => UserName}
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def toProjectPath(projectPath: ResourceId) =
      projectPath
        .as[Try, Path]
        .toEither
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))

    implicit val projectDecoder: Decoder[DatasetProject] = { cursor =>
      for {
        path            <- cursor.downField("projectId").downField("value").as[ResourceId].flatMap(toProjectPath)
        name            <- cursor.downField("projectName").downField("value").as[projects.Name]
        dateCreated     <- cursor.downField("minDateCreated").downField("value").as[DateCreatedInProject]
        maybeAgentEmail <- cursor.downField("maybeAgentEmail").downField("value").as[Option[Email]]
        agentName       <- cursor.downField("agentName").downField("value").as[UserName]
      } yield DatasetProject(path, name, AddedToProject(dateCreated, DatasetAgent(maybeAgentEmail, agentName)))
    }

    _.downField("results").downField("bindings").as(decodeList[DatasetProject])
  }
}
