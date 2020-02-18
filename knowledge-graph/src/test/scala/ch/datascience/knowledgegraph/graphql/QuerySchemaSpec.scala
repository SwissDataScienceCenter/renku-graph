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

package ch.datascience.knowledgegraph.graphql

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.{FilePath, ProjectPath}
import ch.datascience.knowledgegraph.lineage
import ch.datascience.knowledgegraph.lineage.LineageFinder
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model._
import io.circe.Json
import io.circe.literal._
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
              type
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
  }

  private trait TestCase {
    val lineageFinder = mock[LineageFinder[IO]]

    def execute(query: Document): Json =
      Executor
        .execute(
          QuerySchema[IO](lineage.graphql.QueryFields()),
          query,
          new QueryContext[IO](lineageFinder)
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

    private val sourceNode = nodes.generateOne
    private val targetNode = nodes.generateOne
    lazy val lineage       = Lineage(edges = Set(Edge(sourceNode.id, targetNode.id)), nodes = Set(sourceNode, targetNode))

    def json(lineage: Lineage) =
      json"""
      {
        "data": {
          "lineage": {
            "nodes": ${Json.arr(lineage.nodes.map(toJson).to[List]: _*)},
            "edges": ${Json.arr(lineage.edges.map(toJson).to[List]: _*)}
          }
        }
      }"""

    private def toJson(node: Node) =
      json"""
      {
        "id": ${node.id.value},
        "label": ${node.label.value},
        "type": ${node.singleWordType.name}
      }"""

    private def toJson(edge: Edge) =
      json"""
      {
        "source" : ${edge.source.value},
        "target" : ${edge.target.value}
      }"""
  }
}
