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

package io.renku.knowledgegraph.datasets.rest

import cats.effect.IO
import cats.syntax.all._
import io.circe.literal._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets.{Identifier, ImageUri, InitialVersion, Name, Title}
import io.renku.graph.model.projects.Path
import io.renku.http.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeEncoders
import org.http4s.Status._
import org.http4s._
import org.http4s.headers.`Content-Type`
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectDatasetsEndpointSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with TinyTypeEncoders
    with IOSpec {

  import ProjectDatasetsFinder._

  "getProjectDatasets" should {

    "respond with OK and the found datasets" in new TestCase {

      forAll(nonEmptyList(datasetBasicDetails).map(_.toList)) { datasetsList =>
        (projectDatasetsFinder
          .findProjectDatasets(_: Path))
          .expects(projectPath)
          .returning(datasetsList.pure[IO])

        val response = endpoint.getProjectDatasets(projectPath).unsafeRunSync()

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

        response.as[List[Json]].unsafeRunSync() should contain theSameElementsAs (datasetsList map toJson)

        logger.loggedOnly(
          Warn(s"Finding '$projectPath' datasets finished${executionTimeRecorder.executionTimeInfo}")
        )
        logger.reset()
      }
    }

    "respond with OK an empty JSON array if no datasets found" in new TestCase {

      (projectDatasetsFinder
        .findProjectDatasets(_: Path))
        .expects(projectPath)
        .returning(List.empty[ProjectDataset].pure[IO])

      val response = endpoint.getProjectDatasets(projectPath).unsafeRunSync()

      response.status                         shouldBe Ok
      response.contentType                    shouldBe Some(`Content-Type`(MediaType.application.json))
      response.as[List[Json]].unsafeRunSync() shouldBe List.empty

      logger.loggedOnly(
        Warn(s"Finding '$projectPath' datasets finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "respond with INTERNAL_SERVER_ERROR if finding datasets fails" in new TestCase {

      val exception = exceptions.generateOne
      (projectDatasetsFinder
        .findProjectDatasets(_: Path))
        .expects(projectPath)
        .returning(exception.raiseError[IO, List[ProjectDataset]])

      val response = endpoint.getProjectDatasets(projectPath).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(s"Finding $projectPath's datasets failed").asJson

      logger.loggedOnly(Error(s"Finding $projectPath's datasets failed", exception))
    }
  }

  private trait TestCase {
    val projectPath = projectPaths.generateOne

    val projectDatasetsFinder = mock[ProjectDatasetsFinder[IO]]
    val renkuResourcesUrl     = renkuResourcesUrls.generateOne
    val gitLabUrl             = gitLabUrls.generateOne
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    val endpoint =
      new ProjectDatasetsEndpointImpl[IO](projectDatasetsFinder, renkuResourcesUrl, gitLabUrl, executionTimeRecorder)

    lazy val toJson: ((Identifier, InitialVersion, Title, Name, SameAsOrDerived, List[ImageUri])) => Json = {
      case (id, initialVersion, title, name, Left(sameAs), images) =>
        json"""{
          "identifier": $id,
          "versions": {
            "initial": $initialVersion
          },
          "title": $title,
          "name": $name,
          "sameAs": $sameAs,
          "images": $images,
          "_links": [{
            "rel": "details",
            "href": ${renkuResourcesUrl / "datasets" / id}
          }, {
            "rel": "initial-version",
            "href": ${renkuResourcesUrl / "datasets" / initialVersion}
          }]
        }"""
      case (id, initialVersion, title, name, Right(derivedFrom), images) =>
        json"""{
          "identifier": $id,
          "versions" : {
            "initial": $initialVersion
          },
          "title": $title,
          "name": $name,
          "derivedFrom": $derivedFrom,
          "images": $images,
          "_links": [{
            "rel": "details",
            "href": ${renkuResourcesUrl / "datasets" / id}
          }, {
            "rel": "initial-version",
            "href": ${renkuResourcesUrl / "datasets" / initialVersion}
          }]
        }"""
    }

    private implicit lazy val imagesEncoder: Encoder[List[ImageUri]] = Encoder.instance[List[ImageUri]] { images =>
      Json.arr(images.map {
        case uri: ImageUri.Relative => json"""{
            "location": $uri,
            "_links": [{
              "rel": "view",
              "href": ${s"$gitLabUrl/$projectPath/raw/master/$uri"}
            }]
          }"""
        case uri: ImageUri.Absolute => json"""{
            "location": $uri,
            "_links": [{
              "rel": "view",
              "href": $uri
            }]
          }"""
      }: _*)
    }
  }

  private implicit lazy val datasetBasicDetails: Gen[ProjectDataset] = for {
    id                      <- datasetIdentifiers
    initialVersion          <- datasetInitialVersions
    title                   <- datasetTitles
    name                    <- datasetNames
    sameAsEitherDerivedFrom <- Gen.oneOf(datasetSameAs map (Left(_)), datasetDerivedFroms map (Right(_)))
    images                  <- listOf(datasetImageUris)
  } yield (id, initialVersion, title, name, sameAsEitherDerivedFrom, images)
}
