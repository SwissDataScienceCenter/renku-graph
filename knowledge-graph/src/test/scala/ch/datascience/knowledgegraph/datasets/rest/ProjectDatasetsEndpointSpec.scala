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
import ch.datascience.graph.model.datasets.{Identifier, Name}
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Warn}
import ch.datascience.logging.TestExecutionTimeRecorder
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

class ProjectDatasetsEndpointSpec extends WordSpec with MockFactory with ScalaCheckPropertyChecks {

  "getProjectDatasets" should {

    "respond with OK and the found datasets" in new TestCase {

      forAll(nonEmptyList(datasetBasicDetails).map(_.toList)) { datasetsList =>
        (projectDatasetsFinder
          .findProjectDatasets(_: ProjectPath))
          .expects(projectPath)
          .returning(context.pure(datasetsList))

        val response = getProjectDatasets(projectPath).unsafeRunSync()

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

        response.as[List[Json]].unsafeRunSync should contain theSameElementsAs (datasetsList map toJson)

        logger.loggedOnly(
          Warn(s"Finding '$projectPath' datasets finished${executionTimeRecorder.executionTimeInfo}")
        )
        logger.reset()
      }
    }

    "respond with NOT_FOUND if no datasets found" in new TestCase {

      (projectDatasetsFinder
        .findProjectDatasets(_: ProjectPath))
        .expects(projectPath)
        .returning(context.pure(Nil))

      val response = getProjectDatasets(projectPath).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync shouldBe InfoMessage(s"No datasets found for '$projectPath'").asJson

      logger.loggedOnly(
        Warn(s"Finding '$projectPath' datasets finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "respond with INTERNAL_SERVER_ERROR if finding datasets fails" in new TestCase {

      val exception = exceptions.generateOne
      (projectDatasetsFinder
        .findProjectDatasets(_: ProjectPath))
        .expects(projectPath)
        .returning(context.raiseError(exception))

      val response = getProjectDatasets(projectPath).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync shouldBe ErrorMessage(s"Finding $projectPath's datasets failed").asJson

      logger.loggedOnly(Error(s"Finding $projectPath's datasets failed", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val projectPath = projectPaths.generateOne

    val projectDatasetsFinder = mock[ProjectDatasetsFinder[IO]]
    val renkuResourcesUrl     = renkuResourcesUrls.generateOne
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val getProjectDatasets = new ProjectDatasetsEndpoint[IO](
      projectDatasetsFinder,
      renkuResourcesUrl,
      executionTimeRecorder,
      logger
    ).getProjectDatasets _

    lazy val toJson: ((Identifier, Name)) => Json = {
      case (id, name) =>
        json"""{
          "identifier": ${id.value},
          "name": ${name.value},
          "_links": [{
            "rel": "details",
            "href": ${(renkuResourcesUrl / "datasets" / id).value}
          }]
        }"""
    }
  }

  private implicit val datasetBasicDetails: Gen[(Identifier, Name)] = for {
    id   <- datasetIds
    name <- datasetNames
  } yield (id, name)
}
