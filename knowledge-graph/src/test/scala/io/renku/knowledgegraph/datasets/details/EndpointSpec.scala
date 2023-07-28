/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.datasets
package details

import Dataset.{DatasetProject, DatasetVersions, ModifiedDataset, NonModifiedDataset, Tag}
import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import io.circe.Decoder._
import io.circe.{Decoder, DecodingFailure, HCursor, Json}
import io.renku.config.renku
import io.renku.data.Message
import io.renku.generators.CommonGraphGenerators.{authContexts, renkuApiUrls}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.datasets._
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.persons.{Affiliation, Email, Name => UserName}
import io.renku.graph.model.projects.Slug
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.http.rest.Links
import io.renku.http.rest.Links.Rel.Self
import io.renku.http.rest.Links.{Href, Rel}
import io.renku.http.server.EndpointTester._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Warn}
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeDecoders._
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.DatasetViewedEvent
import org.http4s.Status._
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class EndpointSpec
    extends AnyWordSpec
    with should.Matchers
    with EitherValues
    with MockFactory
    with ScalaCheckPropertyChecks
    with EntitiesGenerators
    with IOSpec {

  "GET /datasets/:id" should {

    forAll(
      Table(
        ("DS type", "DS"),
        ("non-modified",
         anyRenkuProjectEntities
           .addDataset(datasetEntities(provenanceImportedExternal))
           .map((importedExternalToNonModified _).tupled)
           .generateOne
        ),
        ("modified",
         anyRenkuProjectEntities
           .addDatasetAndModification(datasetEntities(provenanceInternal))
           .map { case (original -> modified, project) =>
             modifiedToModified(modified, original.provenance.date, project)
           }
           .generateOne
        )
      )
    ) { (dsType, dataset) =>
      s"respond with OK and sends a DATASET_VIEWED event in case a $dsType DS was found" in new TestCase {

        val requestedDataset =
          dataset.fold(ds => RequestedDataset(ds.sameAs), ds => RequestedDataset(ds.project.datasetIdentifier))

        val authContext = authContexts(fixed(requestedDataset)).generateOne

        givenDatasetFinding(requestedDataset, authContext, returning = dataset.some.pure[IO])

        givenDatasetViewedEventSent(dataset.project.datasetIdentifier, authContext, returning = ().pure[IO])

        val response = endpoint.`GET /datasets/:id`(requestedDataset, authContext).unsafeRunSync()

        response.status                      shouldBe Ok
        response.contentType                 shouldBe `Content-Type`(MediaType.application.json).some
        response.as[Dataset].unsafeRunSync() shouldBe dataset

        verifyLinks(response, dataset, requestedDataset)
        val responseCursor = response.as[Json].unsafeRunSync().hcursor
        verifyProject(responseCursor)
        verifyUsedIn(responseCursor, dataset)
        verifyImages(responseCursor, dataset)

        logger.loggedOnly(
          Warn(show"Finding '$requestedDataset' dataset finished${executionTimeRecorder.executionTimeInfo}")
        )
      }
    }

    "fetch DS by sameAs if given in the request instead of identifier" in new TestCase {

      val ds = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedExternal))
        .map((importedExternalToNonModified _).tupled)
        .generateOne

      val requestedDataset = RequestedDataset(ds.sameAs)

      val authContext = authContexts(fixed(requestedDataset)).generateOne

      givenDatasetFinding(requestedDataset, authContext, returning = ds.some.pure[IO])

      givenDatasetViewedEventSent(ds.project.datasetIdentifier, authContext, returning = ().pure[IO])

      endpoint.`GET /datasets/:id`(requestedDataset, authContext).unsafeRunSync().status shouldBe Ok
    }

    "respond with NOT_FOUND if there is no dataset with the given id" in new TestCase {

      val requestedDataset = RequestedDataset(datasetIdentifiers.generateOne)
      val authContext      = authContexts(fixed(requestedDataset)).generateOne

      givenDatasetFinding(requestedDataset, authContext, returning = Option.empty[Dataset].pure[IO])

      val response = endpoint.`GET /datasets/:id`(requestedDataset, authContext).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe `Content-Type`(MediaType.application.json).some

      response.as[Message].unsafeRunSync() shouldBe Message.Info.unsafeApply(show"No '$requestedDataset' dataset found")

      logger.loggedOnly(
        Warn(show"Finding '$requestedDataset' dataset finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "respond with INTERNAL_SERVER_ERROR if finding the dataset fails" in new TestCase {

      val requestedDataset = RequestedDataset(datasetIdentifiers.generateOne)
      val authContext      = authContexts(fixed(requestedDataset)).generateOne

      val exception = exceptions.generateOne
      givenDatasetFinding(requestedDataset, authContext, returning = exception.raiseError[IO, Option[Dataset]])

      val response = endpoint.`GET /datasets/:id`(requestedDataset, authContext).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe `Content-Type`(MediaType.application.json).some

      response.as[Message].unsafeRunSync() shouldBe Message.Error.unsafeApply(
        show"Finding dataset '$requestedDataset' failed"
      )

      logger.loggedOnly(Error(show"Finding dataset '$requestedDataset' failed", exception))
    }

    "do not fail if sending PROJECT_VIEWED event fails" in new TestCase {

      val ds = anyRenkuProjectEntities
        .addDataset(datasetEntities(provenanceImportedExternal))
        .map((importedExternalToNonModified _).tupled)
        .generateOne

      val requestedDataset =
        ds.fold(ds => RequestedDataset(ds.sameAs), ds => RequestedDataset(ds.project.datasetIdentifier))

      val authContext = authContexts(fixed(requestedDataset)).generateOne

      givenDatasetFinding(requestedDataset, authContext, returning = ds.some.pure[IO])

      val exception = exceptions.generateOne
      givenDatasetViewedEventSent(ds.project.datasetIdentifier, authContext, returning = exception.raiseError[IO, Unit])

      endpoint.`GET /datasets/:id`(requestedDataset, authContext).unsafeRunSync().status shouldBe Ok

      logger.logged(Error(show"sending ${DatasetViewedEvent.categoryName} event failed", exception))
    }
  }

  private trait TestCase {

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val datasetsFinder = mock[DatasetFinder[IO]]
    private val tgClient       = mock[triplesgenerator.api.events.Client[IO]]
    val executionTimeRecorder  = TestExecutionTimeRecorder[IO]()
    private implicit val renkuApiUrl: renku.ApiUrl = renkuApiUrls.generateOne
    private implicit val renkuUrl:    RenkuUrl     = renkuUrls.generateOne
    private implicit val gitLabUrl:   GitLabUrl    = gitLabUrls.generateOne
    private val currentTime = Instant.now()
    private val now         = mockFunction[Instant]
    now.expects().returning(currentTime).anyNumberOfTimes()
    val endpoint = new EndpointImpl[IO](datasetsFinder, tgClient, executionTimeRecorder, now)

    def givenDatasetViewedEventSent(identifier:  datasets.Identifier,
                                    authContext: AuthContext[RequestedDataset],
                                    returning:   IO[Unit]
    ) = (tgClient
      .send(_: DatasetViewedEvent))
      .expects(DatasetViewedEvent.forDataset(identifier, authContext.maybeAuthUser.map(_.id), now))
      .returning(returning)

    def givenDatasetFinding(id:          RequestedDataset,
                            authContext: AuthContext[RequestedDataset],
                            returning:   IO[Option[Dataset]]
    ) = (datasetsFinder
      .findDataset(_: RequestedDataset, _: AuthContext[RequestedDataset]))
      .expects(id, authContext)
      .returning(returning)

    def verifyLinks(response: Response[IO], dataset: Dataset, requestedDataset: RequestedDataset) =
      response.as[Json].unsafeRunSync()._links.value shouldBe Links
        .of(
          Self                   -> Href(Uri.unsafeFromString(renkuApiUrl.show) / "datasets" / requestedDataset),
          Rel("initial-version") -> Href(renkuApiUrl / "datasets" / dataset.versions.initial),
          Rel("tags") -> Href(renkuApiUrl / "projects" / dataset.project.slug / "datasets" / dataset.name / "tags")
        )

    def verifyProject(cursor: HCursor) = {
      val mainProject = cursor.downField("project").as[Json].value
      (mainProject.hcursor.downField("slug").as[Slug], mainProject._links)
        .mapN { case (slug, links) =>
          links shouldBe Links.of(Rel("project-details") -> Href(renkuApiUrl / "projects" / slug))
        }
        .getOrElse(fail("No 'slug' or 'project-details' links on the 'project' element"))
    }

    def verifyUsedIn(cursor: HCursor, dataset: Dataset): Unit = {

      val usedInJsons = cursor.downField("usedIn").as[List[Json]].value

      usedInJsons should have size dataset.usedIn.size
      usedInJsons foreach { json =>
        (json.hcursor.downField("slug").as[Slug], json._links)
          .mapN { case (path, links) =>
            links shouldBe Links.of(Rel("project-details") -> Href(renkuApiUrl / "projects" / path))
          }
          .getOrElse(fail("No 'slug' or 'project-details' links on the 'usedIn' elements"))
      }
    }

    def verifyImages(cursor: HCursor, dataset: Dataset): Unit = {

      val imagesJsons = cursor.downField("images").as[List[Json]].value

      imagesJsons should have size dataset.images.size
      imagesJsons foreach { json =>
        (json.hcursor.downField("location").as[ImageUri], json._links)
          .mapN {
            case (uri: ImageUri.Relative, links) =>
              links shouldBe Links.of(Rel("view") -> Href(gitLabUrl / dataset.project.slug / "raw" / "master" / uri))
            case (uri: ImageUri.Absolute, links) =>
              links shouldBe Links.of(Rel("view") -> Href(uri.show))
            case (uri, links) => fail(s"$uri 'location' or $links 'view' links of unknown shape")
          }
          .getOrElse(fail("No 'location' or 'view' links on the 'images' elements"))
      }
    }
  }

  private implicit val datasetEntityDecoder: EntityDecoder[IO, Dataset] = jsonOf[IO, Dataset]

  private implicit lazy val datasetDecoder: Decoder[Dataset] = cursor =>
    for {
      title             <- cursor.downField("title").as[Title]
      name              <- cursor.downField("name").as[Name]
      resourceId        <- cursor.downField("url").as[ResourceId]
      maybeDescription  <- cursor.downField("description").as[Option[Description]]
      published         <- cursor.downField("published").as[(NonEmptyList[DatasetCreator], Option[DatePublished])]
      maybeDateCreated  <- cursor.downField("created").as[Option[DateCreated]]
      maybeDateModified <- cursor.downField("dateModified").as[Option[DateModified]]
      parts             <- cursor.downField("hasPart").as[List[Dataset.DatasetPart]]
      project           <- cursor.downField("project").as[DatasetProject]
      usedIn            <- cursor.downField("usedIn").as[List[DatasetProject]]
      keywords          <- cursor.downField("keywords").as[List[Keyword]]
      maybeSameAs       <- cursor.downField("sameAs").as[Option[SameAs]]
      maybeDerivedFrom  <- cursor.downField("derivedFrom").as[Option[DerivedFrom]]
      versions          <- cursor.downField("versions").as[DatasetVersions]
      maybeInitialTag   <- cursor.downField("tags").downField("initial").as[Option[Tag]]
      images            <- cursor.downField("images").as[List[ImageUri]](decodeList(imageUriDecoder))
      date <-
        maybeDateCreated
          .orElse(published._2)
          .widen[CreatedOrPublished]
          .map(_.asRight)
          .getOrElse(DecodingFailure("No date found", Nil).asLeft)
    } yield (maybeSameAs, maybeDateCreated, maybeDerivedFrom, maybeDateModified) match {
      case (Some(sameAs), _, None, _) =>
        NonModifiedDataset(
          resourceId,
          title,
          name,
          sameAs,
          versions,
          maybeInitialTag,
          maybeDescription,
          published._1.toList,
          date,
          parts,
          project,
          usedIn,
          keywords,
          images
        )
      case (None, Some(dateCreated), Some(derivedFrom), Some(dateModified)) =>
        ModifiedDataset(
          resourceId,
          title,
          name,
          derivedFrom,
          versions,
          maybeInitialTag,
          maybeDescription,
          published._1.toList,
          dateCreated,
          dateModified,
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

  private implicit lazy val partDecoder: Decoder[Dataset.DatasetPart] = cursor =>
    for {
      location <- cursor.downField("atLocation").as[PartLocation]
    } yield Dataset.DatasetPart(location)

  private implicit lazy val projectDecoder: Decoder[DatasetProject] = cursor =>
    for {
      slug         <- cursor.downField("slug").as[projects.Slug]
      name         <- cursor.downField("name").as[projects.Name]
      visibility   <- cursor.downField("visibility").as[projects.Visibility]
      dsIdentifier <- cursor.downField("dataset").downField("identifier").as[datasets.Identifier]
    } yield DatasetProject(projects.ResourceId(slug), slug, name, visibility, dsIdentifier)

  private implicit lazy val versionsDecoder: Decoder[DatasetVersions] = cursor =>
    for {
      initial <- cursor.downField("initial").as[OriginalIdentifier]
    } yield DatasetVersions(initial)

  private implicit lazy val tagDecoder: Decoder[Tag] = cursor =>
    for {
      name      <- cursor.downField("name").as[publicationEvents.Name]
      maybeDesc <- cursor.downField("description").as[Option[publicationEvents.Description]]
    } yield Tag(name, maybeDesc)

  private lazy val imageUriDecoder: Decoder[ImageUri] = _.downField("location").as[ImageUri]
}
