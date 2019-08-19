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

import cats.Order
import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.knowledgegraph.graphql.datasets.DataSetsFinder
import ch.datascience.knowledgegraph.graphql.datasets.model.DataSet
import ch.datascience.knowledgegraph.graphql.lineage.LineageFinder
import ch.datascience.knowledgegraph.graphql.lineage.QueryFields.FilePath
import ch.datascience.knowledgegraph.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.graphql.lineage.model._
import io.circe.Json
import io.circe.literal._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import sangria.ast.Document
import sangria.execution.Executor
import sangria.macros._
import sangria.marshalling.circe._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

class QuerySchemaSpec extends WordSpec with MockFactory with ScalaFutures with IntegrationPatience {

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
      val query = graphql"""
        {
          dataSets(projectPath: "namespace/project") {
            id
            name
            created {date}
          }
        }"""

      givenFindDataSets(ProjectPath("namespace/project"))
        .returning(IO.pure(dataSetsSet))

      execute(query) shouldBe json(dataSetsSet)
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
      def returning(result: IO[Set[DataSet]]) =
        (dataSetsFinder
          .findDataSets(_: ProjectPath))
          .expects(projectPath)
          .returning(result)
    }

    implicit val dataSetOrder: Order[DataSet] = Order.from[DataSet] {
      case (item1, item2) => item1.name.value.length - item2.name.value.length
    }
    lazy val dataSetsSet = nonEmptySet(dataSets).generateOne.toSortedSet

    def json(dataSets: Set[DataSet]) = json"""
        {
          "data" : {
            "dataSets" : ${Json.arr(dataSetsSet.map(toJson).to[List]: _*)}
          }
        }"""

    private def toJson(dataSet: DataSet) =
      json"""
        {
          "id": ${dataSet.id.value},
          "name": ${dataSet.name.value},
          "created": {
            "date": ${dataSet.created.date.value}
          }
        }"""
  }
}
