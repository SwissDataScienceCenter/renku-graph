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

package ch.datascience.graphservice.graphql

import cats.effect.IO
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.graphservice.graphql.lineage.QueryFields.FilePath
import ch.datascience.graphservice.graphql.lineage.model.Edge.SourceEdge
import ch.datascience.graphservice.graphql.lineage.model.Node.SourceNode
import ch.datascience.graphservice.graphql.lineage.model.{Edge, Lineage, Node}
import io.circe.Json
import io.circe.literal._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.ScalaFutures
import sangria.ast.Document
import sangria.execution.Executor
import sangria.macros._
import sangria.marshalling.circe._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

class QuerySchemaSpec extends WordSpec with ScalaFutures with MockFactory {

  "query" should {

    "allow to search for lineage of a given projectId" in new LineageTestCase {
      val query = graphql"""
        {
          lineage(projectPath: "namespace/project") {
            nodes {
              id
              label
            }
            edges {
              id
            }
          }
        }"""

      givenFindLineage(ProjectPath("namespace/project"), None, None)
        .returning(IO.pure(Some(lineage)))

      execute(query) shouldBe json(lineage)
    }

    "allow to search for lineage of a given projectId and commitId" in new LineageTestCase {
      val query = graphql"""
        {
          lineage(projectPath: "namespace/project", commitId: "1234567") {
            nodes {
              id
              label
            }
            edges {
              id
            }
          }
        }"""

      givenFindLineage(ProjectPath("namespace/project"), Some(CommitId("1234567")), None)
        .returning(IO.pure(Some(lineage)))

      execute(query) shouldBe json(lineage)
    }

    "allow to search for lineage of a given projectId, commitId and file" in new LineageTestCase {
      val query = graphql"""
        {
          lineage(projectPath: "namespace/project", commitId: "1234567", filePath: "directory/file") {
            nodes {
              id
              label
            }
            edges {
              id
            }
          }
        }"""

      givenFindLineage(ProjectPath("namespace/project"), Some(CommitId("1234567")), Some(FilePath("directory/file")))
        .returning(IO.pure(Some(lineage)))

      execute(query) shouldBe json(lineage)
    }
  }

  private trait TestCase {
    val lineageFinder = mock[IOLineageFinder]

    def execute(query: Document): Json =
      Executor
        .execute(
          QuerySchema[IO](lineage.QueryFields()),
          query,
          new QueryContext[IO](lineageFinder)
        )
        .futureValue
  }

  private trait LineageTestCase extends TestCase {

    def givenFindLineage(
        projectPath:   ProjectPath,
        maybeCommitId: Option[CommitId],
        maybeFilePath: Option[FilePath]
    ) = new {
      def returning(result: IO[Option[Lineage]]) =
        (lineageFinder
          .findLineage(_: ProjectPath, _: Option[CommitId], _: Option[FilePath]))
          .expects(projectPath, maybeCommitId, maybeFilePath)
          .returning(result)
    }

    lazy val lineage = Lineage(
      nodes = List(SourceNode("node-id", "node-label")),
      edges = List(SourceEdge("edge-id"))
    )

    def json(lineage: Lineage) = json"""
        {
          "data" : {
            "lineage" : {
              "nodes" : ${Json.arr(lineage.nodes.map(toJson): _*)},
              "edges" : ${Json.arr(lineage.edges.map(toJson): _*)}
            }
          }
        }"""

    private def toJson(node: Node) = json"""
        {
          "id" : ${node.id},
          "label" : ${node.label}
        }"""

    private def toJson(edge: Edge) = json"""
        {
          "id" : ${edge.id}
        }"""
  }
}
