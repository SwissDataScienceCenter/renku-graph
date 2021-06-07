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

package ch.datascience.knowledgegraph.projects.rest

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.renkuResourcesUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.projects._
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.http.InfoMessage._
import ch.datascience.http.rest.Links
import ch.datascience.http.rest.Links.{Href, Rel}
import ch.datascience.http.server.EndpointTester._
import ch.datascience.http.{ErrorMessage, InfoMessage}
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Warn}
import ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.model.Forking.ForksCount
import ch.datascience.knowledgegraph.projects.model.Permissions.{AccessLevel, GroupAccessLevel, ProjectAccessLevel}
import ch.datascience.knowledgegraph.projects.model.Project._
import ch.datascience.knowledgegraph.projects.model.Statistics.{CommitsCount, JobArtifactsSize, LsfObjectsSize, RepositorySize, StorageSize}
import ch.datascience.knowledgegraph.projects.model.Urls._
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Json}
import org.http4s.MediaType._
import org.http4s.Status._
import org.http4s._
import org.http4s.circe.jsonOf
import org.http4s.headers.`Content-Type`
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectEndpointSpec extends AnyWordSpec with MockFactory with ScalaCheckPropertyChecks with should.Matchers {

  "getProject" should {

    "respond with OK and the found project details" in new TestCase {
      forAll { project: Project =>
        val maybeAuthUser = authUsers.generateOption
        (projectFinder.findProject _)
          .expects(project.path, maybeAuthUser)
          .returning(project.some.pure[IO])

        val response = getProject(project.path, maybeAuthUser).unsafeRunSync()

        response.status      shouldBe Ok
        response.contentType shouldBe Some(`Content-Type`(application.json))

        response.as[Project].unsafeRunSync() shouldBe project
        response.as[Json].unsafeRunSync()._links shouldBe Right(
          Links.of(
            Rel.Self        -> Href(renkuResourcesUrl / "projects" / project.path),
            Rel("datasets") -> Href(renkuResourcesUrl / "projects" / project.path / "datasets")
          )
        )

        logger.loggedOnly(
          Warn(s"Finding '${project.path}' details finished${executionTimeRecorder.executionTimeInfo}")
        )
        logger.reset()
      }
    }

    "respond with NOT_FOUND if there is no project with the given path" in new TestCase {

      val path          = projectPaths.generateOne
      val maybeAuthUser = authUsers.generateOption

      (projectFinder.findProject _)
        .expects(path, maybeAuthUser)
        .returning(None.pure[IO])

      val response = getProject(path, maybeAuthUser).unsafeRunSync()

      response.status      shouldBe NotFound
      response.contentType shouldBe Some(`Content-Type`(application.json))

      response.as[Json].unsafeRunSync() shouldBe InfoMessage(s"No '$path' project found").asJson

      logger.loggedOnly(
        Warn(s"Finding '$path' details finished${executionTimeRecorder.executionTimeInfo}")
      )
    }

    "respond with INTERNAL_SERVER_ERROR if finding project details fails" in new TestCase {

      val path          = projectPaths.generateOne
      val maybeAuthUser = authUsers.generateOption
      val exception     = exceptions.generateOne
      (projectFinder.findProject _)
        .expects(path, maybeAuthUser)
        .returning(exception.raiseError[IO, Option[Project]])

      val response = getProject(path, maybeAuthUser).unsafeRunSync()

      response.status      shouldBe InternalServerError
      response.contentType shouldBe Some(`Content-Type`(application.json))

      response.as[Json].unsafeRunSync() shouldBe ErrorMessage(s"Finding '$path' project failed").asJson

      logger.loggedOnly(Error(s"Finding '$path' project failed", exception))
    }
  }

  private trait TestCase {
    val projectFinder         = mock[ProjectFinder[IO]]
    val renkuResourcesUrl     = renkuResourcesUrls.generateOne
    val logger                = TestLogger[IO]()
    val executionTimeRecorder = TestExecutionTimeRecorder[IO](logger)
    val getProject = new ProjectEndpointImpl[IO](
      projectFinder,
      renkuResourcesUrl,
      executionTimeRecorder,
      logger
    ).getProject _
  }

  private implicit val projectEntityDecoder: EntityDecoder[IO, Project] = jsonOf[IO, Project]

  private implicit lazy val projectDecoder: Decoder[Project] = cursor =>
    for {
      id               <- cursor.downField("identifier").as[Id]
      path             <- cursor.downField("path").as[Path]
      name             <- cursor.downField("name").as[Name]
      maybeDescription <- cursor.downField("description").as[Option[Description]]
      visibility       <- cursor.downField("visibility").as[Visibility]
      created          <- cursor.downField("created").as[Creation]
      updatedAt        <- cursor.downField("updatedAt").as[DateUpdated]
      urls             <- cursor.downField("urls").as[Urls]
      forks            <- cursor.downField("forking").as[Forking]
      tags             <- cursor.downField("tags").as[List[Tag]].map(_.toSet)
      starsCount       <- cursor.downField("starsCount").as[StarsCount]
      permissions      <- cursor.downField("permissions").as[Permissions]
      statistics       <- cursor.downField("statistics").as[Statistics]
      version          <- cursor.downField("version").as[SchemaVersion]
    } yield Project(id,
                    path,
                    name,
                    maybeDescription,
                    visibility,
                    created,
                    updatedAt,
                    urls,
                    forks,
                    tags,
                    starsCount,
                    permissions,
                    statistics,
                    version
    )

  private implicit lazy val createdDecoder: Decoder[Creation] = cursor =>
    for {
      date    <- cursor.downField("dateCreated").as[DateCreated]
      creator <- cursor.downField("creator").as[Option[Creator]]
    } yield Creation(date, creator)

  private implicit lazy val creatorDecoder: Decoder[Creator] = cursor =>
    for {
      name       <- cursor.downField("name").as[UserName]
      maybeEmail <- cursor.downField("email").as[Option[Email]]
    } yield Creator(maybeEmail, name)

  private implicit lazy val forkingDecoder: Decoder[Forking] = cursor =>
    for {
      count       <- cursor.downField("forksCount").as[ForksCount]
      maybeParent <- cursor.downField("parent").as[Option[ParentProject]]
    } yield Forking(count, maybeParent)

  private implicit lazy val parentDecoder: Decoder[ParentProject] = cursor =>
    for {
      path    <- cursor.downField("path").as[Path]
      name    <- cursor.downField("name").as[Name]
      created <- cursor.downField("created").as[Creation]
    } yield ParentProject(path, name, created)

  private implicit lazy val urlsDecoder: Decoder[Urls] = cursor =>
    for {
      ssh         <- cursor.downField("ssh").as[SshUrl]
      http        <- cursor.downField("http").as[HttpUrl]
      web         <- cursor.downField("web").as[WebUrl]
      maybeReadme <- cursor.downField("readme").as[Option[ReadmeUrl]]
    } yield Urls(ssh, http, web, maybeReadme)

  private implicit lazy val permissionsDecoder: Decoder[Permissions] = cursor => {
    def maybeAccessLevel(name: String) = cursor.downField(name).as[Option[AccessLevel]]

    for {
      maybeProjectAccessLevel <- maybeAccessLevel("projectAccess").map(_.map(ProjectAccessLevel))
      maybeGroupAccessLevel   <- maybeAccessLevel("groupAccess").map(_.map(GroupAccessLevel))
      permissions <- (maybeProjectAccessLevel, maybeGroupAccessLevel) match {
                       case (Some(project), Some(group)) => Right(Permissions(project, group))
                       case (Some(project), None)        => Right(Permissions(project))
                       case (None, Some(group))          => Right(Permissions(group))
                       case _                            => Left(DecodingFailure("Neither projectAccess nor groupAccess", Nil))
                     }
    } yield permissions
  }

  private implicit lazy val accessLevelDecoder: Decoder[AccessLevel] = cursor =>
    for {
      name <- cursor.downField("level").downField("name").as[String]
      accessLevel <- cursor
                       .downField("level")
                       .downField("value")
                       .as[Int]
                       .flatMap(AccessLevel.from)
                       .leftMap(exception => DecodingFailure(exception.getMessage, Nil))
    } yield
      if (accessLevel.name.value == name) accessLevel
      else throw new Exception(s"$name does not match $accessLevel")

  private implicit lazy val statisticsDecoder: Decoder[Statistics] = cursor =>
    for {
      commitsCount     <- cursor.downField("commitsCount").as[CommitsCount]
      storageSize      <- cursor.downField("storageSize").as[StorageSize]
      repositorySize   <- cursor.downField("repositorySize").as[RepositorySize]
      lfsSize          <- cursor.downField("lfsObjectsSize").as[LsfObjectsSize]
      jobArtifactsSize <- cursor.downField("jobArtifactsSize").as[JobArtifactsSize]
    } yield Statistics(commitsCount, storageSize, repositorySize, lfsSize, jobArtifactsSize)
}
