/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.lineage

import cats.effect.IO
import cats.syntax.all._
import io.circe.{Decoder, Json}
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.Schemas.prov
import io.renku.http.ErrorMessage.ErrorMessage
import io.renku.http.InfoMessage.InfoMessage
import io.renku.http.server.EndpointTester.errorMessageEntityDecoder
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.lineage.LineageGenerators._
import io.renku.knowledgegraph.lineage.model.Node.{Label, Location, Type}
import io.renku.knowledgegraph.lineage.model.{Edge, Lineage, Node}
import io.renku.testtools.IOSpec
import org.http4s.MediaType.application
import org.http4s.Status.{InternalServerError, NotFound, Ok}
import org.http4s.circe.jsonDecoder
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EndpointSpec extends AnyWordSpec with should.Matchers with MockFactory with IOSpec {
  "GET /lineage" should {
    "respond with OK and the lineage" in new TestCase {
      val lineage = lineages.generateOne

      (lineageFinder.find _)
        .expects(projectPath, location, maybeUser)
        .returning(IO.pure(lineage.some))

      val response      = endpoint.`GET /lineage`(projectPath, location, maybeUser).unsafeRunSync()
      val Right(result) = response.as[Json].unsafeRunSync().as[Lineage]

      response.status      shouldBe Ok
      response.contentType shouldBe Some(`Content-Type`(application.json))
      result               shouldBe makeNodesWithOneType(lineage)
    }

    "respond with NotFound if the lineage isn't returned from the finder" in new TestCase {

      (lineageFinder.find _)
        .expects(projectPath, location, maybeUser)
        .returning(IO.pure(None))

      val response = endpoint.`GET /lineage`(projectPath, location, maybeUser).unsafeRunSync()
      val result   = response.as[InfoMessage].unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(application.json))
      result               shouldBe InfoMessage(show"No lineage for project: $projectPath file: $location")
    }

    "respond with InternalServerError if the lineage is returned from the finder but the encoder fails" in new TestCase {
      val lineage   = lineages.generateOne
      val exception = exceptions.generateOne

      (lineageFinder.find _)
        .expects(projectPath, location, maybeUser)
        .returning(exception.raiseError[IO, Option[Lineage]])

      val response = endpoint.`GET /lineage`(projectPath, location, maybeUser).unsafeRunSync()
      val result   = response.as[Json].unsafeRunSync()

      response.status                           shouldBe InternalServerError
      response.contentType                      shouldBe Some(`Content-Type`(application.json))
      response.as[ErrorMessage].unsafeRunSync() shouldBe ErrorMessage("Lineage generation failed")

    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne
    val location    = nodeLocations.generateOne
    val maybeUser   = authUsers.generateOption

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val lineageFinder = mock[LineageFinder[IO]]
    val endpoint      = new EndpointImpl[IO](lineageFinder)

    implicit val lineageEncoder: Decoder[Lineage] = Decoder.instance { cursor =>
      for {
        nodes <- cursor.downField("nodes").as[Set[Node]]
        edges <- cursor.downField("edges").as[Set[Edge]]
      } yield Lineage(edges, nodes)
    }

    implicit val edgeEncoder: Decoder[Edge] = Decoder.instance { cursor =>
      for {
        source <- cursor.downField("source").as[String].map(Location)
        target <- cursor.downField("target").as[String].map(Location)
      } yield Edge(source, target)
    }

    implicit val nodeEncoder: Decoder[Node] = Decoder.instance { cursor =>
      for {
        location <- cursor.downField("location").as[String].map(Location)
        label    <- cursor.downField("label").as[String].map(Label)
        types    <- cursor.downField("type").as[String].map(Type)
      } yield Node(location, label, Set(types))
    }

    def makeNodesWithOneType(lineage: Lineage): Lineage =
      lineage.copy(nodes = lineage.nodes.map(node => node.copy(types = Set(Type(node.singleWordType)))))

    private implicit class NodeOps(node: Node) {

      private lazy val FileTypes = Set(Type((prov / "Entity").show))

      lazy val singleWordType: String = node.types match {
        case types if types contains Type((prov / "Activity").show)   => "ProcessRun"
        case types if types contains Type((prov / "Collection").show) => "Directory"
        case FileTypes                                                => "File"
      }
    }
  }
}
