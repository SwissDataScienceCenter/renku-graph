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

package io.renku.graph.acceptancetests

import cats.syntax.all._
import io.renku.config.renku
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{httpUrls => urls, _}
import io.renku.graph.acceptancetests.data.Project.Permissions._
import io.renku.graph.acceptancetests.data.Project.Statistics._
import io.renku.graph.acceptancetests.data.Project.Urls._
import io.renku.graph.acceptancetests.data.Project._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.gitlab.GitLabUser
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.versions.CliVersion
import org.scalacheck.Gen

package object data extends TSData with ProjectFunctions {

  implicit val cliVersion: CliVersion   = currentVersionPair.cliVersion
  val renkuApiUrl:         renku.ApiUrl = renku.ApiUrl("http://localhost:9004/knowledge-graph")

  def dataProjects(
      projectGen:   Gen[testentities.RenkuProject],
      commitsCount: CommitsCount = CommitsCount.one
  ): Gen[Project] = for {
    project     <- projectGen
    id          <- projectIds
    members     <- EntitiesGenerators.gitLabMemberGen().toGeneratorOfNonEmptyList(max = 3)
    urls        <- urlsObjects
    starsCount  <- starsCounts
    permissions <- permissionsObjects
    statistics  <- statisticsObjects.map(_.copy(commitsCount = commitsCount))
    _ = if (project.members.nonEmpty)
          throw new Exception(show"Test project should not have members")
    _ = if (project.maybeCreator.flatMap(_.maybeGitLabId).nonEmpty)
          throw new Exception(show"Test project creator with GitLab id")
  } yield Project(project,
                  id,
                  maybeCreator = project.maybeCreator.map(_.to[GitLabUser]),
                  members,
                  urls,
                  starsCount,
                  permissions,
                  statistics
  )

  private implicit lazy val testPersonToProjectMember: testentities.Person => GitLabUser = { p =>
    GitLabUser(p.name, persons.Username(p.name.value), personGitLabIds.generateOne, p.maybeEmail)
  }

  def dataProjects(project: testentities.RenkuProject): Gen[Project] = dataProjects(fixed(project))

  implicit lazy val urlsObjects: Gen[Urls] = for {
    sshUrl         <- sshUrls
    httpUrl        <- httpUrls
    webUrl         <- webUrls
    maybeReadmeUrl <- readmeUrls.toGeneratorOfOptions
  } yield Urls(sshUrl, httpUrl, webUrl, maybeReadmeUrl)

  private lazy val starsCounts: Gen[StarsCount] = nonNegativeInts() map (v => StarsCount(v.value))
  private lazy val sshUrls: Gen[SshUrl] = for {
    hostParts   <- nonEmptyList(nonBlankStrings())
    projectSlug <- projectSlugs
  } yield SshUrl(s"git@${hostParts.toList.mkString(".")}:$projectSlug.git")

  private lazy val httpUrls: Gen[HttpUrl] = for {
    url         <- urls()
    projectSlug <- projectSlugs
  } yield HttpUrl(s"$url/$projectSlug.git")

  private lazy val readmeUrls: Gen[ReadmeUrl] = for {
    url         <- urls()
    projectSlug <- projectSlugs
  } yield ReadmeUrl(s"$url/$projectSlug/blob/master/README.md")

  private lazy val webUrls: Gen[WebUrl] = urls() map WebUrl.apply

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
