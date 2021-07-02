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

package ch.datascience.graph.acceptancetests

import ch.datascience.config.renku
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{httpUrls => urls, _}
import ch.datascience.graph.acceptancetests.data.Project.Permissions._
import ch.datascience.graph.acceptancetests.data.Project.Statistics._
import ch.datascience.graph.acceptancetests.data.Project.Urls._
import ch.datascience.graph.acceptancetests.data.Project._
import ch.datascience.graph.config.RenkuBaseUrlLoader
import ch.datascience.graph.model
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model._
import ch.datascience.graph.model.testentities.EntitiesGenerators._
import org.scalacheck.Gen

import java.time.Instant.now
import scala.util.Try

package object data {
  val currentVersionPair:    RenkuVersionPair   = RenkuVersionPair(CliVersion("0.12.2"), SchemaVersion("8"))
  implicit val cliVersion:   CliVersion         = currentVersionPair.cliVersion
  val renkuResourcesUrl:     renku.ResourcesUrl = renku.ResourcesUrl("http://localhost:9004/knowledge-graph")
  implicit val renkuBaseUrl: RenkuBaseUrl       = RenkuBaseUrlLoader[Try]().fold(throw _, identity)

  def dataProjects[FC <: model.projects.ForksCount](
      projectGen: Gen[testentities.Project[FC]]
  ): Gen[Project[FC]] = for {
    project          <- projectGen
    id               <- projectIds
    maybeDescription <- projectDescriptions.toGeneratorOfOptions
    updatedAt        <- timestamps(min = project.dateCreated.value, max = now).toGeneratorOf[DateUpdated]
    urls             <- urlsObjects
    tags             <- tagsObjects.toGeneratorOfSet()
    starsCount       <- starsCounts
    permissions      <- permissionsObjects
    statistics       <- statisticsObjects
  } yield Project(project, id, maybeDescription, updatedAt, urls, tags, starsCount, permissions, statistics)

  def dataProjects[FC <: model.projects.ForksCount](
      project: testentities.Project[FC]
  ): Gen[Project[FC]] = dataProjects(fixed(project))

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
