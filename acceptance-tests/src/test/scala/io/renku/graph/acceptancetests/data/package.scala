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

package io.renku.graph.acceptancetests

import io.renku.config.renku
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{httpUrls => urls, _}
import io.renku.graph.acceptancetests.data.Project.Permissions._
import io.renku.graph.acceptancetests.data.Project.Statistics._
import io.renku.graph.acceptancetests.data.Project.Urls._
import io.renku.graph.acceptancetests.data.Project._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import org.scalacheck.Gen

import java.time.Instant.now

package object data extends RdfStoreData {

  implicit val cliVersion: CliVersion         = currentVersionPair.cliVersion
  val renkuResourcesUrl:   renku.ResourcesUrl = renku.ResourcesUrl("http://localhost:9004/knowledge-graph")

  def dataProjects(
      projectGen:   Gen[testentities.Project],
      commitsCount: CommitsCount = CommitsCount.one
  ): Gen[Project] = for {
    project     <- projectGen
    id          <- projectIds
    updatedAt   <- timestamps(min = project.dateCreated.value, max = now).toGeneratorOf[DateUpdated]
    urls        <- urlsObjects
    tags        <- tagsObjects.toGeneratorOfSet()
    starsCount  <- starsCounts
    permissions <- permissionsObjects
    statistics  <- statisticsObjects.map(_.copy(commitsCount = commitsCount))
  } yield Project(project, id, updatedAt, urls, tags, starsCount, permissions, statistics)

  def dataProjects(project: testentities.Project): Gen[Project] = dataProjects(fixed(project))

  implicit lazy val urlsObjects: Gen[Urls] = for {
    sshUrl         <- sshUrls
    httpUrl        <- httpUrls
    webUrl         <- webUrls
    maybeReadmeUrl <- readmeUrls.toGeneratorOfOptions
  } yield Urls(sshUrl, httpUrl, webUrl, maybeReadmeUrl)

  private lazy val starsCounts: Gen[StarsCount] = nonNegativeInts() map (v => StarsCount(v.value))
  private lazy val tagsObjects: Gen[Tag]        = nonBlankStrings() map (v => Tag(v.value))
  private lazy val sshUrls: Gen[SshUrl] = for {
    hostParts   <- nonEmptyList(nonBlankStrings())
    projectPath <- projectPaths
  } yield SshUrl(s"git@${hostParts.toList.mkString(".")}:$projectPath.git")

  private lazy val httpUrls: Gen[HttpUrl] = for {
    url         <- urls()
    projectPath <- projectPaths
  } yield HttpUrl(s"$url/$projectPath.git")

  private lazy val readmeUrls: Gen[ReadmeUrl] = for {
    url         <- urls()
    projectPath <- projectPaths
  } yield ReadmeUrl(s"$url/$projectPath/blob/master/README.md")

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
