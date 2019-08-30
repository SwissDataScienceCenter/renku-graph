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

package ch.datascience.knowledgegraph.datasets.rest

import cats.MonadError
import cats.effect.IO
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.dataSets.{Identifier, Name}
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectDataSetsEndpointSpec extends WordSpec with MockFactory with ScalaCheckPropertyChecks {

  "getProjectDataSets" should {

    "respond with OK and the found data-sets" in new TestCase {

      forAll(nonEmptyList(dataSetBasicInfos).map(_.toList)) { dataSetsList =>
        (projectDataSetsFinder
          .findProjectDataSets(_: ProjectPath))
          .expects(projectPath)
          .returning(context.pure(dataSetsList))

        val response = getProjectDataSets(projectPath).unsafeRunSync()

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

        response.as[List[Json]].unsafeRunSync should contain theSameElementsAs (dataSetsList map toJson)

        logger.expectNoLogs()
      }
    }

    "respond with NOT_FOUND if no data-sets found" in new TestCase {

      (projectDataSetsFinder
        .findProjectDataSets(_: ProjectPath))
        .expects(projectPath)
        .returning(context.pure(Nil))

      val response = getProjectDataSets(projectPath).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync shouldBe InfoMessage(s"No data-sets found for '$projectPath'").asJson

      logger.expectNoLogs()
    }

    "respond with INTERNAL_SERVER_ERROR if finding data-sets fails" in new TestCase {

      val exception = exceptions.generateOne
      (projectDataSetsFinder
        .findProjectDataSets(_: ProjectPath))
        .expects(projectPath)
        .returning(context.raiseError(exception))

      val response = getProjectDataSets(projectPath).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync shouldBe ErrorMessage(s"Finding $projectPath's data-sets failed").asJson

      logger.loggedOnly(Error(s"Finding $projectPath's data-sets failed", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val projectPath = projectPaths.generateOne

    val projectDataSetsFinder = mock[ProjectDataSetsFinder[IO]]
    val renkuResourcesUrl     = renkuResourcesUrls.generateOne
    val logger                = TestLogger[IO]()
    val getProjectDataSets = new ProjectDataSetsEndpoint[IO](
      projectDataSetsFinder,
      renkuResourcesUrl,
      logger
    ).getProjectDataSets _

    lazy val toJson: ((Identifier, Name)) => Json = {
      case (id, name) =>
        json"""{
          "identifier": ${id.value},
          "name": ${name.value},
          "_links": [{
            "rel": "details",
            "href": ${(renkuResourcesUrl / "data-sets" / id).value}
          }]
        }"""
    }
  }

  private implicit val dataSetBasicInfos: Gen[(Identifier, Name)] = for {
    id   <- dataSetIds
    name <- dataSetNames
  } yield (id, name)
}
