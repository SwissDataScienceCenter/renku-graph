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

package io.renku.knowledgegraph.projects.rest

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{httpUrls => urls, _}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.projects.Path
import io.renku.graph.model.projects.ResourceId._
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.knowledgegraph.projects.model.Forking.ForksCount
import io.renku.knowledgegraph.projects.model.Permissions.{apply => _, _}
import io.renku.knowledgegraph.projects.model.Project.{DateUpdated, StarsCount}
import io.renku.knowledgegraph.projects.model.Statistics._
import io.renku.knowledgegraph.projects.model.Urls.{HttpUrl, ReadmeUrl, SshUrl, WebUrl}
import io.renku.knowledgegraph.projects.model._
import io.renku.knowledgegraph.projects.rest.Converters._
import io.renku.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import io.renku.knowledgegraph.projects.rest.KGProjectFinder.KGProject
import org.scalacheck.Gen

private object ProjectsGenerators {

  implicit val projects: Gen[Project] = for {
    kgProject     <- anyProjectEntities.map(_.to[KGProject])
    gitLabProject <- gitLabProjects
  } yield Project(
    id = gitLabProject.id,
    path = kgProject.path,
    name = kgProject.name,
    maybeDescription = kgProject.maybeDescription,
    visibility = kgProject.visibility,
    created = Creation(
      date = kgProject.created.date,
      maybeCreator = kgProject.created.maybeCreator.map(creator => Creator(creator.maybeEmail, creator.name))
    ),
    updatedAt = gitLabProject.updatedAt,
    urls = gitLabProject.urls,
    forking = Forking(
      gitLabProject.forksCount,
      kgProject.maybeParent.map { parent =>
        ParentProject(
          parent.resourceId.toUnsafe[Path],
          parent.name,
          Creation(parent.created.date,
                   parent.created.maybeCreator.map(creator => Creator(creator.maybeEmail, creator.name))
          )
        )
      }
    ),
    keywords = kgProject.keywords,
    starsCount = gitLabProject.starsCount,
    permissions = gitLabProject.permissions,
    statistics = gitLabProject.statistics,
    maybeVersion = kgProject.maybeVersion
  )

  implicit lazy val gitLabProjects: Gen[GitLabProject] = for {
    id          <- projectIds
    visibility  <- projectVisibilities
    urls        <- urlsObjects
    forksCount  <- forksCounts
    starsCount  <- starsCounts
    updatedAt   <- updatedAts
    permissions <- permissionsObjects
    statistics  <- statisticsObjects
  } yield GitLabProject(id, visibility, urls, forksCount, starsCount, updatedAt, permissions, statistics)

  implicit lazy val urlsObjects: Gen[Urls] = for {
    sshUrl         <- sshUrls
    httpUrl        <- httpUrls
    webUrl         <- webUrls
    maybeReadmeUrl <- readmeUrls.toGeneratorOfOptions
  } yield Urls(sshUrl, httpUrl, webUrl, maybeReadmeUrl)

  private implicit lazy val forksCounts: Gen[ForksCount] = nonNegativeInts() map (v => ForksCount(v.value))
  implicit lazy val starsCounts:         Gen[StarsCount] = nonNegativeInts() map (v => StarsCount(v.value))

  private implicit lazy val sshUrls: Gen[SshUrl] = for {
    hostParts   <- nonEmptyList(nonBlankStrings())
    projectPath <- projectPaths
  } yield SshUrl(s"git@${hostParts.toList.mkString(".")}:$projectPath.git")

  private implicit lazy val httpUrls: Gen[HttpUrl] = for {
    url         <- urls()
    projectPath <- projectPaths
  } yield HttpUrl(s"$url/$projectPath.git")

  private implicit lazy val readmeUrls: Gen[ReadmeUrl] = for {
    url         <- urls()
    projectPath <- projectPaths
  } yield ReadmeUrl(s"$url/$projectPath/blob/master/README.md")

  private implicit lazy val webUrls:    Gen[WebUrl]      = urls() map WebUrl.apply
  private implicit lazy val updatedAts: Gen[DateUpdated] = timestampsNotInTheFuture map DateUpdated.apply

  implicit lazy val permissionsObjects: Gen[Permissions] =
    Gen.oneOf(
      accessLevels map ProjectAccessLevel.apply map ProjectPermissions.apply,
      accessLevels map GroupAccessLevel.apply map GroupPermissions.apply,
      for {
        project <- accessLevels map ProjectAccessLevel.apply
        group   <- accessLevels map GroupAccessLevel.apply
      } yield ProjectAndGroupPermissions(project, group)
    )

  implicit lazy val accessLevels: Gen[AccessLevel] = Gen.oneOf(AccessLevel.all.toList)

  implicit lazy val statisticsObjects: Gen[Statistics] = for {
    commitsCount     <- nonNegativeInts() map (v => CommitsCount(v.value))
    storageSize      <- nonNegativeInts() map (v => StorageSize(v.value))
    repositorySize   <- nonNegativeInts() map (v => RepositorySize(v.value))
    lfsSize          <- nonNegativeInts() map (v => LsfObjectsSize(v.value))
    jobArtifactsSize <- nonNegativeInts() map (v => JobArtifactsSize(v.value))
  } yield Statistics(commitsCount, storageSize, repositorySize, lfsSize, jobArtifactsSize)
}
