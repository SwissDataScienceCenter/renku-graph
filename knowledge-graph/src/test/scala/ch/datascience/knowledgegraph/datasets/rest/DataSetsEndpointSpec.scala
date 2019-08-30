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
import ch.datascience.generators.CommonGraphGenerators.renkuResourcesUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.dataSets._
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.http.rest.Links
import ch.datascience.http.rest.Links.{Href, Rel}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.Error
import ch.datascience.knowledgegraph.datasets.DataSetsGenerators._
import ch.datascience.knowledgegraph.datasets.model._
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

class DataSetsEndpointSpec extends WordSpec with MockFactory with ScalaCheckPropertyChecks {

  "getDataSet" should {

    "respond with OK and the found data-set" in new TestCase {
      forAll { dataSet: DataSet =>
        (dataSetsFinder
          .findDataSet(_: Identifier))
          .expects(dataSet.id)
          .returning(context.pure(Some(dataSet)))

        val response = getDataSet(dataSet.id).unsafeRunSync()

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

        response.as[DataSet].unsafeRunSync shouldBe dataSet
        response.as[Json].unsafeRunSync._links shouldBe Right(
          Links(Rel.Self, Href(renkuResourcesUrl / "data-sets" / dataSet.id))
        )

        logger.expectNoLogs()
      }
    }

    "respond with NOT_FOUND if there is no data-set with the given id" in new TestCase {

      val identifier = dataSetIds.generateOne

      (dataSetsFinder
        .findDataSet(_: Identifier))
        .expects(identifier)
        .returning(context.pure(None))

      val response = getDataSet(identifier).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync shouldBe InfoMessage(s"No data-set with '$identifier' id found").asJson

      logger.expectNoLogs()
    }

    "respond with INTERNAL_SERVER_ERROR if finding the data-set fails" in new TestCase {

      val identifier = dataSetIds.generateOne

      val exception = exceptions.generateOne
      (dataSetsFinder
        .findDataSet(_: Identifier))
        .expects(identifier)
        .returning(context.raiseError(exception))

      val response = getDataSet(identifier).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync shouldBe ErrorMessage(s"Finding data-set with '$identifier' id failed").asJson

      logger.loggedOnly(Error(s"Finding data-set with '$identifier' id failed", exception))
    }
  }

  private trait TestCase {
    val context = MonadError[IO, Throwable]

    val dataSetsFinder    = mock[DataSetFinder[IO]]
    val renkuResourcesUrl = renkuResourcesUrls.generateOne
    val logger            = TestLogger[IO]()
    val getDataSet        = new DataSetsEndpoint[IO](dataSetsFinder, renkuResourcesUrl, logger).getDataSet _
  }

  private implicit val dataSetEntityDecoder: EntityDecoder[IO, DataSet] = jsonOf[IO, DataSet]

  private implicit lazy val dataSetDecoder: Decoder[DataSet] = (cursor: HCursor) =>
    for {
      id               <- cursor.downField("identifier").as[Identifier]
      name             <- cursor.downField("name").as[Name]
      maybeDescription <- cursor.downField("description").as[Option[Description]]
      created          <- cursor.downField("created").as[DataSetCreation]
      published        <- cursor.downField("published").as[DataSetPublishing]
      parts            <- cursor.downField("hasPart").as[List[DataSetPart]]
      projects         <- cursor.downField("isPartOf").as[List[DataSetProject]]
    } yield DataSet(id, name, maybeDescription, created, published, parts, projects)

  private implicit lazy val dataSetCreationDecoder: Decoder[DataSetCreation] = (cursor: HCursor) =>
    for {
      date       <- cursor.downField("dateCreated").as[DateCreated]
      agentEmail <- cursor.downField("agent").downField("email").as[Email]
      agentName  <- cursor.downField("agent").downField("name").as[UserName]
    } yield DataSetCreation(date, DataSetAgent(agentEmail, agentName))

  private implicit lazy val dataSetPublishingDecoder: Decoder[DataSetPublishing] = (cursor: HCursor) =>
    for {
      maybeDate <- cursor.downField("datePublished").as[Option[PublishedDate]]
      creators  <- cursor.downField("creator").as[List[DataSetCreator]].map(_.toSet)
    } yield DataSetPublishing(maybeDate, creators)

  private implicit lazy val dataSetCreatorDecoder: Decoder[DataSetCreator] = (cursor: HCursor) =>
    for {
      name       <- cursor.downField("name").as[UserName]
      maybeEmail <- cursor.downField("email").as[Option[Email]]
    } yield DataSetCreator(maybeEmail, name)

  private implicit lazy val dataSetPartDecoder: Decoder[DataSetPart] = (cursor: HCursor) =>
    for {
      name        <- cursor.downField("name").as[PartName]
      location    <- cursor.downField("atLocation").as[PartLocation]
      dateCreated <- cursor.downField("dateCreated").as[PartDateCreated]
    } yield DataSetPart(name, location, dateCreated)

  private implicit lazy val dataSetProjectDecoder: Decoder[DataSetProject] = (cursor: HCursor) =>
    for {
      name <- cursor.downField("name").as[ProjectPath]
    } yield DataSetProject(name)
}
