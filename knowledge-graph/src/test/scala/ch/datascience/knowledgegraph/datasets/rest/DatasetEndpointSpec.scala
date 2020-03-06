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

package ch.datascience.knowledgegraph.datasets.rest

import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.controllers.InfoMessage._
import ch.datascience.controllers.{ErrorMessage, InfoMessage}
import ch.datascience.generators.CommonGraphGenerators.renkuResourcesUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.model.users.{Affiliation, Email, Name => UserName}
import ch.datascience.http.rest.Links
import ch.datascience.http.rest.Links.Rel.Self
import ch.datascience.http.rest.Links.{Href, Rel}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Warn}
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.circe.Decoder._
import io.circe.syntax._
import io.circe.{Decoder, HCursor, Json}
import org.http4s.Status._
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetEndpointSpec extends WordSpec with MockFactory with ScalaCheckPropertyChecks {

  "getDataset" should {

    "respond with OK and the found dataset" in new TestCase {
      forAll { dataset: Dataset =>
        (datasetsFinder
          .findDataset(_: Identifier))
          .expects(dataset.id)
          .returning(context.pure(Some(dataset)))

        val response = getDataset(dataset.id).unsafeRunSync()

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

        response.as[Dataset].unsafeRunSync shouldBe dataset
        response.as[Json].unsafeRunSync._links shouldBe Right(
          Links.of(Self -> Href(renkuResourcesUrl / "datasets" / dataset.id))
        )
        val Right(projectsJsons) = response.as[Json].unsafeRunSync.hcursor.downField("isPartOf").as[List[Json]]
        projectsJsons should have size dataset.projects.size
        projectsJsons.foreach { json =>
          (json.hcursor.downField("path").as[Path], json._links)
            .mapN {
              case (path, links) =>
                links shouldBe Links.of(Rel("project-details") -> Href(renkuResourcesUrl / "projects" / path))
            }
            .getOrElse(fail("No 'path' or 'project-details' links on the 'isPartOf' elements"))
        }

        logger.loggedOnly(Warn(s"Finding '${dataset.id}' dataset finished${executionTimeRecorder.executionTimeInfo}"))
        logger.reset()
      }
    }

    "respond with NOT_FOUND if there is no dataset with the given id" in new TestCase {

      val identifier = datasetIdentifiers.generateOne

      (datasetsFinder
        .findDataset(_: Identifier))
        .expects(identifier)
        .returning(context.pure(None))

      val response = getDataset(identifier).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync shouldBe InfoMessage(s"No dataset with '$identifier' id found").asJson

      logger.loggedOnly(Warn(s"Finding '$identifier' dataset finished${executionTimeRecorder.executionTimeInfo}"))
    }

    "respond with INTERNAL_SERVER_ERROR if finding the dataset fails" in new TestCase {

      val identifier = datasetIdentifiers.generateOne

      val exception = exceptions.generateOne
      (datasetsFinder
        .findDataset(_: Identifier))
        .expects(identifier)
        .returning(context.raiseError(exception))

      val response = getDataset(identifier).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync shouldBe ErrorMessage(s"Finding dataset with '$identifier' id failed").asJson

      logger.loggedOnly(Error(s"Finding dataset with '$identifier' id failed", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val datasetsFinder        = mock[DatasetFinder[IO]]
    val renkuResourcesUrl     = renkuResourcesUrls.generateOne
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val getDataset = new DatasetEndpoint[IO](
      datasetsFinder,
      renkuResourcesUrl,
      executionTimeRecorder,
      logger
    ).getDataset _
  }

  private implicit val datasetEntityDecoder: EntityDecoder[IO, Dataset] = jsonOf[IO, Dataset]

  private implicit lazy val datasetDecoder: Decoder[Dataset] = (cursor: HCursor) =>
    for {
      id               <- cursor.downField("identifier").as[Identifier]
      name             <- cursor.downField("name").as[Name]
      sameAs           <- cursor.downField("sameAs").as[SameAs]
      maybeUrl         <- cursor.downField("url").as[Option[Url]]
      maybeDescription <- cursor.downField("description").as[Option[Description]]
      published        <- cursor.downField("published").as[DatasetPublishing]
      parts            <- cursor.downField("hasPart").as[List[DatasetPart]]
      projects         <- cursor.downField("isPartOf").as[List[DatasetProject]]
    } yield Dataset(id, name, sameAs, maybeUrl, maybeDescription, published, parts, projects)

  private implicit lazy val datasetPublishingDecoder: Decoder[DatasetPublishing] = (cursor: HCursor) =>
    for {
      maybeDate <- cursor.downField("datePublished").as[Option[PublishedDate]]
      creators  <- cursor.downField("creator").as[List[DatasetCreator]].map(_.toSet)
    } yield DatasetPublishing(maybeDate, creators)

  private implicit lazy val datasetCreatorDecoder: Decoder[DatasetCreator] = (cursor: HCursor) =>
    for {
      name             <- cursor.downField("name").as[UserName]
      maybeEmail       <- cursor.downField("email").as[Option[Email]]
      maybeAffiliation <- cursor.downField("affiliation").as[Option[Affiliation]]
    } yield DatasetCreator(maybeEmail, name, maybeAffiliation)

  private implicit lazy val datasetPartDecoder: Decoder[DatasetPart] = (cursor: HCursor) =>
    for {
      name     <- cursor.downField("name").as[PartName]
      location <- cursor.downField("atLocation").as[PartLocation]
    } yield DatasetPart(name, location)

  private implicit lazy val datasetProjectDecoder: Decoder[DatasetProject] = (cursor: HCursor) =>
    for {
      path    <- cursor.downField("path").as[Path]
      name    <- cursor.downField("name").as[projects.Name]
      created <- cursor.downField("created").as[AddedToProject]
    } yield DatasetProject(path, name, created)

  private implicit lazy val datasetInProjectCreationDecoder: Decoder[AddedToProject] = (cursor: HCursor) =>
    for {
      date       <- cursor.downField("dateCreated").as[DateCreatedInProject]
      agentEmail <- cursor.downField("agent").downField("email").as[Email]
      agentName  <- cursor.downField("agent").downField("name").as[UserName]
    } yield AddedToProject(date, DatasetAgent(agentEmail, agentName))
}
