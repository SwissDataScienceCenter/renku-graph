/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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
import ch.datascience.generators.CommonGraphGenerators.authUsers
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.projects.Path
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.knowledgegraph.lineage
import ch.datascience.knowledgegraph.lineage.LineageFinder
import ch.datascience.knowledgegraph.lineage.LineageGenerators._
import ch.datascience.knowledgegraph.lineage.model.Node.Location
import ch.datascience.knowledgegraph.lineage.model._
import io.circe.Json
import io.circe.literal._
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import sangria.ast.Document
import sangria.execution.Executor
import sangria.macros._
import sangria.marshalling.circe._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.reflectiveCalls

class QuerySchemaSpec
    extends AnyWordSpec
    with ScalaCheckPropertyChecks
    with MockFactory
    with ScalaFutures
    with IntegrationPatience
    with should.Matchers {

  "query" should {

    "allow to search for lineage of a given projectPath, commitId and file" in new LineageTestCase {
      val query = graphql"""
        {
          lineage(projectPath: "namespace/project", filePath: "directory/file") {
            nodes {
              id
              location
              label
              type
            }
            edges {
              source
              target
            }
          }
        }"""

      givenFindLineage(Path("namespace/project"), Location("directory/file"))
        .returning(IO.pure(Some(lineage)))

      execute(query) shouldBe json(lineage)
    }
  }

  private trait TestCase {
    val lineageFinder = mock[LineageFinder[IO]]

    val maybeAuthUser = authUsers.generateOption
    def execute(query: Document): Json =
      Executor
        .execute(
          QuerySchema[IO](lineage.graphql.QueryFields()),
          query,
          new LineageQueryContext[IO](lineageFinder, maybeAuthUser)
        )
        .futureValue
  }

  private trait LineageTestCase extends TestCase {

    def givenFindLineage(projectPath: Path, location: Location) = new {
      def returning(result: IO[Option[Lineage]]) =
        (lineageFinder
          .find(_: Path, _: Location, _: Option[AuthUser]))
          .expects(projectPath, location, maybeAuthUser)
          .returning(result)
    }

    private val sourceNode = entityNodes.generateOne
    private val targetNode = processRunNodes.generateOne
    lazy val lineage = Lineage(
      edges = Set(Edge(sourceNode.location, targetNode.location)),
      nodes = Set(sourceNode, targetNode)
    )

    def json(lineage: Lineage) =
      json"""
      {
        "data": {
          "lineage": {
            "nodes": ${Json.arr(lineage.nodes.map(toJson).toList: _*)},
            "edges": ${Json.arr(lineage.edges.map(toJson).toList: _*)}
          }
        }
      }"""

    private def toJson(node: Node) =
      json"""
      {
        "id": ${node.location.value},
        "location": ${node.location.value},
        "label": ${node.label.value},
        "type": ${node.singleWordType.fold(throw _, identity).name}
      }"""

    private def toJson(edge: Edge) =
      json"""
      {
        "source" : ${edge.source.value},
        "target" : ${edge.target.value}
      }"""
  }
}
