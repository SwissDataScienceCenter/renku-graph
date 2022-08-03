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

package io.renku.knowledgegraph.projectdetails

import ProjectsGenerators._
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.config.renku
import io.renku.generators.CommonGraphGenerators.renkuApiUrls
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.SchemaVersion
import io.renku.graph.model.persons.{Email, Name => UserName}
import io.renku.graph.model.projects._
import io.renku.http.rest.Links
import io.renku.http.rest.Links.{Href, Rel}
import io.renku.http.server.EndpointTester._
import io.renku.tinytypes.json.TinyTypeDecoders._
import model.Forking.ForksCount
import model.Permissions.{AccessLevel, GroupAccessLevel, ProjectAccessLevel}
import model.Project._
import model.Statistics.{CommitsCount, JobArtifactsSize, LsfObjectsSize, RepositorySize, StorageSize}
import model.Urls._
import model._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class JsonEncoderSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "encode" should {

    "convert the model.Project object to Json" in {
      forAll { project: Project =>
        val json = encoder encode project

        json.as[Project] shouldBe project.asRight
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
  private lazy val encoder = new JsonEncoderImpl(renkuApiUrl)

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
      keywords         <- cursor.downField("keywords").as[Set[Keyword]]
      starsCount       <- cursor.downField("starsCount").as[StarsCount]
      permissions      <- cursor.downField("permissions").as[Permissions]
      statistics       <- cursor.downField("statistics").as[Statistics]
      maybeVersion     <- cursor.downField("version").as[Option[SchemaVersion]]
    } yield Project(id,
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
                    maybeVersion
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
