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

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import io.circe.Decoder._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Json}
import io.renku.generators.CommonGraphGenerators.{authContexts, renkuApiUrls}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.datasets._
import io.renku.graph.model.projects.Path
import io.renku.graph.model.testentities._
import io.renku.graph.model.persons.{Affiliation, Email, Name => UserName}
import io.renku.graph.model.{RenkuUrl, projects}
import io.renku.http.InfoMessage._
import io.renku.http.rest.Links
import io.renku.http.rest.Links.Rel.Self
import io.renku.http.rest.Links.{Href, Rel}
import io.renku.http.server.EndpointTester._
import io.renku.http.{ErrorMessage, InfoMessage}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.knowledgegraph.datasets.model
import io.renku.knowledgegraph.datasets.model._
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeDecoders._
import org.http4s.Status._
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.headers.`Content-Type`
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetEndpointSpec
    extends AnyWordSpec
    with MockFactory
    with ScalaCheckPropertyChecks
    with should.Matchers
    with IOSpec {

  "getDataset" should {

    "respond with OK and the found NonModifiedDataset" in new TestCase {
      forAll(
        Gen.oneOf(
          anyRenkuProjectEntities
            .addDataset(datasetEntities(provenanceImportedExternal))
            .map((importedExternalToNonModified _).tupled),
          anyRenkuProjectEntities
            .addDatasetAndModification(datasetEntities(provenanceInternal))
            .map { case (_ ::~ modified, project) => modified -> project }
            .map((modifiedToModified _).tupled)
        )
      ) { dataset =>
        val authContext = authContexts(fixed(dataset.id)).generateOne

        (datasetsFinder
          .findDataset(_: Identifier, _: AuthContext[Identifier]))
          .expects(dataset.id, authContext)
          .returning(dataset.some.pure[IO])

        val response = endpoint.getDataset(dataset.id, authContext).unsafeRunSync()

        response.status                            shouldBe Ok
        response.contentType                       shouldBe Some(`Content-Type`(MediaType.application.json))
        response.as[model.Dataset].unsafeRunSync() shouldBe dataset
        response.as[Json].unsafeRunSync()._links shouldBe Links
          .of(
            Self                   -> Href(renkuApiUrl / "datasets" / dataset.id),
            Rel("initial-version") -> Href(renkuApiUrl / "datasets" / dataset.versions.initial)
          )
          .asRight

        val responseCursor = response.as[Json].unsafeRunSync().hcursor

        val Right(mainProject) = responseCursor.downField("project").as[Json]
        (mainProject.hcursor.downField("path").as[Path], mainProject._links)
          .mapN { case (path, links) =>
            links shouldBe Links.of(Rel("project-details") -> Href(renkuApiUrl / "projects" / path))
          }
          .getOrElse(fail("No 'path' or 'project-details' links on the 'project' element"))

        val Right(usedInJsons) = responseCursor.downField("usedIn").as[List[Json]]
        usedInJsons should have size dataset.usedIn.size
        usedInJsons.foreach { json =>
          (json.hcursor.downField("path").as[Path], json._links)
            .mapN { case (path, links) =>
              links shouldBe Links.of(Rel("project-details") -> Href(renkuApiUrl / "projects" / path))
            }
            .getOrElse(fail("No 'path' or 'project-details' links on the 'usedIn' elements"))
        }

        val Right(imagesJsons) = responseCursor.downField("images").as[List[Json]]
        imagesJsons should have size dataset.images.size
        imagesJsons.foreach { json =>
          (json.hcursor.downField("location").as[ImageUri], json._links)
            .mapN {
              case (uri: ImageUri.Relative, links) =>
                links shouldBe Links.of(Rel("view") -> Href(gitLabUrl / dataset.project.path / "raw" / "master" / uri))
              case (uri: ImageUri.Absolute, links) =>
                links shouldBe Links.of(Rel("view") -> Href(uri.show))
              case (uri, links) => fail(s"$uri 'location' or $links 'view' links of unknown shape")
            }
            .getOrElse(fail("No 'location' or 'view' links on the 'images' elements"))
        }

        logger.loggedOnly(Warn(s"Finding '${dataset.id}' dataset finished${executionTimeRecorder.executionTimeInfo}"))
        logger.reset()
      }
    }

    "respond with NOT_FOUND if there is no dataset with the given id" in new TestCase {

      val identifier  = datasetIdentifiers.generateOne
      val authContext = authContexts(fixed(identifier)).generateOne

      (datasetsFinder
        .findDataset(_: Identifier, _: AuthContext[Identifier]))
        .expects(identifier, authContext)
        .returning(Option.empty[model.Dataset].pure[IO])

      val response = endpoint.getDataset(identifier, authContext).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync() shouldBe InfoMessage(s"No dataset with '$identifier' id found").asJson

      logger.loggedOnly(Warn(s"Finding '$identifier' dataset finished${executionTimeRecorder.executionTimeInfo}"))
    }

    "respond with INTERNAL_SERVER_ERROR if finding the dataset fails" in new TestCase {

      val identifier  = datasetIdentifiers.generateOne
      val authContext = authContexts(fixed(identifier)).generateOne

      val exception = exceptions.generateOne
      (datasetsFinder
        .findDataset(_: Identifier, _: AuthContext[Identifier]))
        .expects(identifier, authContext)
        .returning(exception.raiseError[IO, Option[model.Dataset]])

      val response = endpoint.getDataset(identifier, authContext).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(MediaType.application.json))

      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(s"Finding dataset with '$identifier' id failed").asJson

      logger.loggedOnly(Error(s"Finding dataset with '$identifier' id failed", exception))
    }
  }

  private trait TestCase {
    implicit val renkuUrl: RenkuUrl = renkuUrls.generateOne
    val gitLabUrl = gitLabUrls.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val datasetsFinder        = mock[DatasetFinder[IO]]
    val renkuApiUrl           = renkuApiUrls.generateOne
    val executionTimeRecorder = TestExecutionTimeRecorder[IO]()
    val endpoint = new DatasetEndpointImpl[IO](datasetsFinder, renkuApiUrl, gitLabUrl, executionTimeRecorder)
  }

  private implicit val datasetEntityDecoder: EntityDecoder[IO, model.Dataset] = jsonOf[IO, model.Dataset]

  private implicit lazy val datasetDecoder: Decoder[model.Dataset] = cursor =>
    for {
      id               <- cursor.downField("identifier").as[Identifier]
      title            <- cursor.downField("title").as[Title]
      name             <- cursor.downField("name").as[Name]
      resourceId       <- cursor.downField("url").as[ResourceId]
      maybeDescription <- cursor.downField("description").as[Option[Description]]
      published        <- cursor.downField("published").as[(NonEmptyList[DatasetCreator], Option[DatePublished])]
      maybeDateCreated <- cursor.downField("created").as[Option[DateCreated]]
      parts            <- cursor.downField("hasPart").as[List[model.DatasetPart]]
      project          <- cursor.downField("project").as[DatasetProject]
      usedIn           <- cursor.downField("usedIn").as[List[DatasetProject]]
      keywords         <- cursor.downField("keywords").as[List[Keyword]]
      maybeSameAs      <- cursor.downField("sameAs").as[Option[SameAs]]
      maybeDerivedFrom <- cursor.downField("derivedFrom").as[Option[DerivedFrom]]
      versions         <- cursor.downField("versions").as[DatasetVersions]
      images           <- cursor.downField("images").as[List[ImageUri]](decodeList(imageUriDecoder))
      date <-
        maybeDateCreated
          .orElse(published._2)
          .widen[Date]
          .map(_.asRight)
          .getOrElse(DecodingFailure("No date found", Nil).asLeft)
    } yield (maybeSameAs, maybeDateCreated, maybeDerivedFrom) match {
      case (Some(sameAs), _, None) =>
        NonModifiedDataset(resourceId,
                           id,
                           title,
                           name,
                           sameAs,
                           versions,
                           maybeDescription,
                           published._1.toList,
                           date,
                           parts,
                           project,
                           usedIn,
                           keywords,
                           images
        )
      case (None, Some(dateCreated), Some(derivedFrom)) =>
        ModifiedDataset(resourceId,
                        id,
                        title,
                        name,
                        derivedFrom,
                        versions,
                        maybeDescription,
                        published._1.toList,
                        dateCreated,
                        parts,
                        project,
                        usedIn,
                        keywords,
                        images
        )
      case _ => fail("Cannot decode payload as Dataset")
    }

  private implicit lazy val publishingDecoder: Decoder[(NonEmptyList[DatasetCreator], Option[DatePublished])] = {
    cursor =>
      val failIfNil: List[DatasetCreator] => Either[DecodingFailure, NonEmptyList[DatasetCreator]] = {
        case Nil          => DecodingFailure("No creators on DS", Nil).asLeft
        case head :: tail => NonEmptyList.of(head, tail: _*).asRight
      }

      for {
        maybeDate <- cursor.downField("datePublished").as[Option[DatePublished]]
        creators  <- cursor.downField("creator").as[List[DatasetCreator]].flatMap(failIfNil).map(_.sortBy(_.name))
      } yield creators -> maybeDate
  }

  private implicit lazy val creatorDecoder: Decoder[DatasetCreator] = cursor =>
    for {
      name             <- cursor.downField("name").as[UserName]
      maybeEmail       <- cursor.downField("email").as[Option[Email]]
      maybeAffiliation <- cursor.downField("affiliation").as[Option[Affiliation]]
    } yield DatasetCreator(maybeEmail, name, maybeAffiliation)

  private implicit lazy val partDecoder: Decoder[model.DatasetPart] = cursor =>
    for {
      location <- cursor.downField("atLocation").as[PartLocation]
    } yield model.DatasetPart(location)

  private implicit lazy val projectDecoder: Decoder[DatasetProject] = cursor =>
    for {
      path <- cursor.downField("path").as[Path]
      name <- cursor.downField("name").as[projects.Name]
    } yield DatasetProject(path, name)

  private implicit lazy val versionsDecoder: Decoder[DatasetVersions] = cursor =>
    for {
      initial <- cursor.downField("initial").as[OriginalIdentifier]
    } yield DatasetVersions(initial)

  private lazy val imageUriDecoder: Decoder[ImageUri] = _.downField("location").as[ImageUri]
}
