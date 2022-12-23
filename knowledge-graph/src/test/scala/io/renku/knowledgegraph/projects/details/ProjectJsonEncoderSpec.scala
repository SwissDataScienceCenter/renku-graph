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

package io.renku.knowledgegraph.projects.details

import ProjectsGenerators._
import cats.syntax.all._
import io.circe.Decoder._
import io.circe.{Decoder, DecodingFailure}
import io.renku.config.renku
import io.renku.generators.CommonGraphGenerators.renkuApiUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{GitLabUrl, SchemaVersion}
import io.renku.graph.model.persons.{Affiliation, Email, Name => UserName}
import io.renku.graph.model.projects._
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.http.rest.Links
import io.renku.http.rest.Links.{Href, Rel}
import io.renku.http.server.EndpointTester._
import io.renku.tinytypes.json.TinyTypeDecoders._
import model.Forking.ForksCount
import model.Permissions.{AccessLevel, GroupAccessLevel, ProjectAccessLevel}
import model.Project.{DateUpdated, ImageLinks, StarsCount}
import model.Statistics.{CommitsCount, JobArtifactsSize, LsfObjectsSize, RepositorySize, StorageSize}
import model.Urls.{HttpUrl, ReadmeUrl, SshUrl, WebUrl}
import model._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectJsonEncoderSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {
  implicit val gitLabUrl: GitLabUrl = EntitiesGenerators.gitLabUrl

  "encode" should {

    "convert the model.Project object to Json" in {
      forAll { project: Project =>
        val json = encoder encode project

        json.as(decoder(project)) shouldBe project.asRight
        json._links shouldBe Right(
          Links.of(
            Rel.Self        -> Href(renkuApiUrl / "projects" / project.path),
            Rel("datasets") -> Href(renkuApiUrl / "projects" / project.path / "datasets")
          )
        )

      }
    }
  }

  private lazy val renkuApiUrl: renku.ApiUrl = renkuApiUrls.generateOne
  private lazy val encoder = new ProjectJsonEncoderImpl(renkuApiUrl)

  private def decoder(project: Project): Decoder[Project] = cursor =>
    for {
      id               <- cursor.downField("identifier").as[GitLabId]
      path             <- cursor.downField("path").as[Path]
      name             <- cursor.downField("name").as[Name]
      maybeDescription <- cursor.downField("description").as[Option[Description]]
      visibility       <- cursor.downField("visibility").as[Visibility]
      created          <- cursor.downField("created").as[Creation](creationDecoder(project.created))
      updatedAt        <- cursor.downField("updatedAt").as[DateUpdated]
      urls             <- cursor.downField("urls").as[Urls]
      forks            <- cursor.downField("forking").as(forkingDecoder(project.forking))
      keywords         <- cursor.downField("keywords").as[Set[Keyword]]
      starsCount       <- cursor.downField("starsCount").as[StarsCount]
      permissions      <- cursor.downField("permissions").as[Permissions]
      statistics       <- cursor.downField("statistics").as[Statistics]
      maybeVersion     <- cursor.downField("version").as[Option[SchemaVersion]]
      images           <- cursor.downField("images").as[List[ImageLinks]]
    } yield Project(
      project.resourceId,
      id,
      path,
      name,
      maybeDescription,
      visibility,
      created,
      updatedAt,
      urls,
      forks,
      keywords,
      starsCount,
      permissions,
      statistics,
      maybeVersion,
      images.map(_.location)
    )

  private def creationDecoder(creation: Creation): Decoder[Creation] = cursor =>
    for {
      date    <- cursor.downField("dateCreated").as[DateCreated]
      creator <- cursor.downField("creator").as(decodeOption(creatorDecoder(creation.maybeCreator)))
    } yield Creation(date, creator)

  private def creatorDecoder(maybeCreator: Option[Creator]): Decoder[Creator] = cursor =>
    for {
      name             <- cursor.downField("name").as[UserName]
      maybeEmail       <- cursor.downField("email").as[Option[Email]]
      maybeAffiliation <- cursor.downField("affiliation").as[Option[Affiliation]]
      creator          <- maybeCreator.toRight(DecodingFailure(show"'$name' creator expected but found None", Nil))
    } yield Creator(creator.resourceId, name, maybeEmail, maybeAffiliation)

  private def forkingDecoder(forking: Forking): Decoder[Forking] = cursor =>
    for {
      count       <- cursor.downField("forksCount").as[Forking.ForksCount]
      maybeParent <- cursor.downField("parent").as(decodeOption(parentDecoder(forking.maybeParent)))
    } yield Forking(count, maybeParent)

  private def parentDecoder(maybeParent: Option[ParentProject]): Decoder[ParentProject] = cursor =>
    for {
      path    <- cursor.downField("path").as[Path]
      name    <- cursor.downField("name").as[Name]
      parent  <- maybeParent.toRight(DecodingFailure(show"'$path' parent project expected but found None", Nil))
      created <- cursor.downField("created").as(creationDecoder(parent.created))
    } yield ParentProject(parent.resourceId, path, name, created)

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
                       case _ => Left(DecodingFailure("Neither projectAccess nor groupAccess", Nil))
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
