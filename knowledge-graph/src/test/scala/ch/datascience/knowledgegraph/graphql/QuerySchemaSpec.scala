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
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.knowledgegraph.graphql.datasets.DataSetsFinder
import ch.datascience.knowledgegraph.graphql.datasets.model.{DataSet, DataSetCreator, DataSetPart}
import ch.datascience.knowledgegraph.graphql.lineage.LineageFinder
import ch.datascience.knowledgegraph.graphql.lineage.QueryFields.FilePath
import ch.datascience.knowledgegraph.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.graphql.lineage.model._
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

    "allow to search for project's dataSets" in new DataSetsTestCase {
      forAll(dataSetsLists) { dataSets =>
        val query = graphql"""
        {
          dataSets(projectPath: "namespace/project") {
            identifier
            name
            description
            created { dateCreated agent { email name } }
            published { datePublished creator { email name } }
            hasPart { name atLocation dateCreated }
            project { name }
          }
        }"""

        givenFindDataSets(ProjectPath("namespace/project"))
          .returning(IO.pure(dataSets))

        execute(query) shouldBe json(dataSets)
      }
    }
  }

  private trait TestCase {
    val lineageFinder  = mock[LineageFinder[IO]]
    val dataSetsFinder = mock[DataSetsFinder[IO]]

    def execute(query: Document): Json =
      Executor
        .execute(
          QuerySchema[IO](lineage.QueryFields(), datasets.QueryFields()),
          query,
          new QueryContext[IO](lineageFinder, dataSetsFinder)
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

  private trait DataSetsTestCase extends TestCase {

    import ch.datascience.knowledgegraph.graphql.datasets.DataSetsGenerators._

    def givenFindDataSets(projectPath: ProjectPath) = new {
      def returning(result: IO[List[DataSet]]) =
        (dataSetsFinder
          .findDataSets(_: ProjectPath))
          .expects(projectPath)
          .returning(result)
    }

    lazy val dataSetsLists = nonEmptyList(dataSets).map(_.toList)

    def json(dataSets: List[DataSet]) = json"""
        {
          "data" : {
            "dataSets" : ${Json.arr(dataSets.map(toJson): _*)}
          }
        }"""

    // format: off
    private def toJson(dataSet: DataSet): Json = json"""
      {
        "identifier": ${dataSet.id.value},
        "name": ${dataSet.name.value},
        "description": ${dataSet.maybeDescription.map(_.value).map(Json.fromString).getOrElse(Json.Null)},
        "created": {
          "dateCreated": ${dataSet.created.date.value},
          "agent": {
            "email": ${dataSet.created.agent.email.value},
            "name": ${dataSet.created.agent.name.value}
          }
        },
        "published": {
          "datePublished": ${dataSet.published.maybeDate.map(_.toString).map(Json.fromString).getOrElse(Json.Null)},
          "creator": ${dataSet.published.creators.toList}
        },
        "hasPart": ${dataSet.part},
        "project": {
          "name": ${dataSet.project.name.value}
        }
      }"""
    // format: on

    private implicit lazy val creatorEncoder: Encoder[DataSetCreator] = Encoder.instance[DataSetCreator] { creator =>
      json"""{
        "email": ${creator.maybeEmail.map(_.toString).map(Json.fromString).getOrElse(Json.Null)},
        "name": ${creator.name.value}
      }"""
    }

    private implicit lazy val partEncoder: Encoder[DataSetPart] = Encoder.instance[DataSetPart] { part =>
      json"""{
        "name": ${part.name.value},
        "atLocation": ${part.atLocation.value},
        "dateCreated": ${part.dateCreated.value}
      }"""
    }
  }
}
