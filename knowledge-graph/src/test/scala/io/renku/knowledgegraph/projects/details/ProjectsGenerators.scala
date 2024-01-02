/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import Converters._
import GitLabProjectFinder.GitLabProject
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{nonBlankStrings, nonEmptyList, nonNegativeInts, httpUrls => urls}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs, projectVisibilities}
import io.renku.graph.model.testentities.{Project => _, _}
import model.Forking.ForksCount
import model.Permissions._
import model.Project.StarsCount
import model.Statistics.{CommitsCount, JobArtifactsSize, LsfObjectsSize, RepositorySize, StorageSize}
import model.Urls.{ReadmeUrl, SshUrl, WebUrl}
import model._
import org.scalacheck.Gen

private object ProjectsGenerators {

  implicit val resourceProjects: Gen[Project] = for {
    kgProject     <- anyProjectEntities.map(_.to(kgProjectConverter))
    gitLabProject <- gitLabProjects
  } yield Project(
    resourceId = kgProject.resourceId,
    id = gitLabProject.id,
    slug = kgProject.slug,
    name = kgProject.name,
    maybeDescription = kgProject.maybeDescription,
    visibility = kgProject.visibility,
    created = Creation(
      date = kgProject.created.date,
      kgProject.created.maybeCreator.map(toModelCreator)
    ),
    dateModified = kgProject.dateModified,
    urls = gitLabProject.urls,
    forking = Forking(
      gitLabProject.forksCount,
      kgProject.maybeParent.map { parent =>
        ParentProject(
          parent.resourceId,
          parent.slug,
          parent.name,
          Creation(
            parent.created.date,
            parent.created.maybeCreator.map(toModelCreator)
          )
        )
      }
    ),
    keywords = kgProject.keywords,
    starsCount = gitLabProject.starsCount,
    permissions = gitLabProject.permissions,
    statistics = gitLabProject.statistics,
    maybeVersion = kgProject.maybeVersion,
    images = kgProject.images
  )

  implicit lazy val gitLabProjects: Gen[GitLabProject] = for {
    id          <- projectIds
    visibility  <- projectVisibilities
    urls        <- urlsObjects
    forksCount  <- forksCounts
    starsCount  <- starsCounts
    permissions <- permissionsObjects
    statistics  <- statisticsObjects
  } yield GitLabProject(id, visibility, urls, forksCount, starsCount, permissions, statistics)

  implicit lazy val urlsObjects: Gen[Urls] = for {
    sshUrl         <- sshUrls
    httpUrl        <- projectGitHttpUrls
    webUrl         <- webUrls
    maybeReadmeUrl <- readmeUrls.toGeneratorOfOptions
  } yield Urls(sshUrl, httpUrl, webUrl, maybeReadmeUrl)

  private implicit lazy val forksCounts: Gen[ForksCount] = nonNegativeInts() map (v => ForksCount(v.value))
  implicit lazy val starsCounts:         Gen[StarsCount] = nonNegativeInts() map (v => StarsCount(v.value))

  private implicit lazy val sshUrls: Gen[SshUrl] = for {
    hostParts   <- nonEmptyList(nonBlankStrings())
    projectSlug <- projectSlugs
  } yield SshUrl(s"git@${hostParts.toList.mkString(".")}:$projectSlug.git")

  private implicit lazy val readmeUrls: Gen[ReadmeUrl] = for {
    url         <- urls()
    projectSlug <- projectSlugs
  } yield ReadmeUrl(s"$url/$projectSlug/blob/master/README.md")

  private implicit lazy val webUrls: Gen[WebUrl] = urls() map WebUrl.apply

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
