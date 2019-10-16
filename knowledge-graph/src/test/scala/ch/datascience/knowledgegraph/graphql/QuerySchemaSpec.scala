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

package ch.datascience.knowledgegraph.graphql

import cats.effect.IO
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.{FilePath, ProjectPath}
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators
import ch.datascience.knowledgegraph.datasets.graphql.ProjectDatasetsFinder
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.lineage.LineageFinder
import ch.datascience.knowledgegraph.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.lineage.model._
import ch.datascience.knowledgegraph.{datasets, lineage}
import io.circe.literal._
import io.circe.{Encoder, Json}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import sangria.ast.Document
import sangria.execution.Executor
import sangria.macros._
import sangria.marshalling.circe._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

class QuerySchemaSpec
    extends WordSpec
    with ScalaCheckPropertyChecks
    with MockFactory
    with ScalaFutures
    with IntegrationPatience {

  "query" should {

    "allow to search for lineage of a given projectPath, commitId and file" in new LineageTestCase {
      val query = graphql"""
        {
          lineage(projectPath: "namespace/project", commitId: "1234567", filePath: "directory/file") {
            nodes {
              id
              label
            }
            edges {
              source
              target
            }
          }
        }"""

      givenFindLineage(ProjectPath("namespace/project"), CommitId("1234567"), FilePath("directory/file"))
        .returning(IO.pure(Some(lineage)))

      execute(query) shouldBe json(lineage)
    }

    "allow to search for project's datasets" in new DatasetsTestCase {
      forAll(datasetsLists) { datasets =>
        val query = graphql"""
        {
          datasets(projectPath: "namespace/project") {
            identifier
            name
            description
            published { datePublished creator { email name } }
            hasPart { name atLocation }
            isPartOf { path name created { dateCreated agent { email name } } }
          }
        }"""

        givenFindDatasets(ProjectPath("namespace/project"))
          .returning(IO.pure(datasets))

        execute(query) shouldBe json(datasets)
      }
    }
  }

  private trait TestCase {
    val lineageFinder  = mock[LineageFinder[IO]]
    val datasetsFinder = mock[ProjectDatasetsFinder[IO]]

    def execute(query: Document): Json =
      Executor
        .execute(
          QuerySchema[IO](lineage.graphql.QueryFields(), datasets.graphql.QueryFields()),
          query,
          new QueryContext[IO](lineageFinder, datasetsFinder)
        )
        .futureValue
  }

  private trait LineageTestCase extends TestCase {

    def givenFindLineage(
        projectPath: ProjectPath,
        commitId:    CommitId,
        filePath:    FilePath
    ) = new {
      def returning(result: IO[Option[Lineage]]) =
        (lineageFinder
          .findLineage(_: ProjectPath, _: CommitId, _: FilePath))
          .expects(projectPath, commitId, filePath)
          .returning(result)
    }

    private val sourceNode = SourceNode(NodeId("node-1"), NodeLabel("node-1-label"))
    private val targetNode = TargetNode(NodeId("node-2"), NodeLabel("node-2-label"))
    lazy val lineage       = Lineage(edges = Set(Edge(sourceNode, targetNode)), nodes = Set(sourceNode, targetNode))

    def json(lineage: Lineage) = json"""
        {
          "data" : {
            "lineage" : {
              "nodes" : ${Json.arr(lineage.nodes.map(toJson).to[List]: _*)},
              "edges" : ${Json.arr(lineage.edges.map(toJson).to[List]: _*)}
            }
          }
        }"""

    private def toJson(node: Node) = json"""
        {
          "id" : ${node.id.value},
          "label" : ${node.label.value}
        }"""

    private def toJson(edge: Edge) = json"""
        {
          "source" : ${edge.source.id.value},
          "target" : ${edge.target.id.value}
        }"""
  }

  private trait DatasetsTestCase extends TestCase {

    def givenFindDatasets(projectPath: ProjectPath) = new {
      def returning(result: IO[List[Dataset]]) =
        (datasetsFinder
          .findDatasets(_: ProjectPath))
          .expects(projectPath)
          .returning(result)
    }

    lazy val datasetsLists = nonEmptyList(DatasetsGenerators.datasets).map(_.toList)

    def json(datasets: List[Dataset]) = json"""
        {
          "data" : {
            "datasets" : ${Json.arr(datasets.map(toJson): _*)}
          }
        }"""

    // format: off
    private def toJson(dataset: Dataset): Json = json"""
      {
        "identifier": ${dataset.id.value},
        "name": ${dataset.name.value},
        "description": ${dataset.maybeDescription.map(_.value).map(Json.fromString).getOrElse(Json.Null)},
        "published": {
          "datePublished": ${dataset.published.maybeDate.map(_.toString).map(Json.fromString).getOrElse(Json.Null)},
          "creator": ${dataset.published.creators.toList}
        },
        "hasPart": ${dataset.part},
        "isPartOf": ${dataset.project}
      }"""
    // format: on

    private implicit lazy val creatorEncoder: Encoder[DatasetCreator] = Encoder.instance[DatasetCreator] { creator =>
      json"""{
        "email": ${creator.maybeEmail.map(_.toString).map(Json.fromString).getOrElse(Json.Null)},
        "name": ${creator.name.value}
      }"""
    }

    private implicit lazy val partEncoder: Encoder[DatasetPart] = Encoder.instance[DatasetPart] { part =>
      json"""{
        "name": ${part.name.value},
        "atLocation": ${part.atLocation.value}
      }"""
    }

    private implicit lazy val projectEncoder: Encoder[DatasetProject] = Encoder.instance[DatasetProject] { project =>
      json"""{
        "path": ${project.path.value},
        "name": ${project.name.value},
        "created": {
          "dateCreated": ${project.created.date.value},
          "agent": {
            "email": ${project.created.agent.email.value},
            "name": ${project.created.agent.name.value}
          }
        }
      }"""
    }
  }
}
