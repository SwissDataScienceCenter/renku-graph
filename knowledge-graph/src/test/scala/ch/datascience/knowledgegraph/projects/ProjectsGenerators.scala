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

package ch.datascience.knowledgegraph.projects

import java.time.temporal.ChronoUnit.DAYS
import ch.datascience.generators.CommonGraphGenerators.renkuBaseUrls
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{httpUrls => urls, _}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.{DateCreated, Path}
import ch.datascience.knowledgegraph.projects.model.Forking.ForksCount
import ch.datascience.knowledgegraph.projects.model.Permissions._
import ch.datascience.knowledgegraph.projects.model.Project.{DateUpdated, StarsCount, Tag}
import ch.datascience.knowledgegraph.projects.model.Statistics.{CommitsCount, JobArtifactsSize, LsfObjectsSize, RepositorySize, StorageSize}
import ch.datascience.knowledgegraph.projects.model.Urls.{HttpUrl, ReadmeUrl, SshUrl, WebUrl}
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder._
import org.scalacheck.Gen

import java.time.Instant.now

object ProjectsGenerators {

  private implicit lazy val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne

  implicit val projects: Gen[Project] = for {
    kgProject     <- kgProjects
    gitLabProject <- gitLabProjects
  } yield Project(
    id = gitLabProject.id,
    path = kgProject.path,
    name = kgProject.name,
    maybeDescription = gitLabProject.maybeDescription,
    visibility = gitLabProject.visibility,
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
    tags = gitLabProject.tags,
    starsCount = gitLabProject.starsCount,
    permissions = gitLabProject.permissions,
    statistics = gitLabProject.statistics,
    version = kgProject.version
  )

  implicit lazy val kgProjects: Gen[KGProject] = kgProjects(parents.toGeneratorOfOptions)

  def kgProjects(parentsGen: Gen[Option[Parent]]): Gen[KGProject] =
    for {
      id          <- projectPaths
      name        <- projectNames
      created     <- projectCreations
      maybeParent <- parentsGen
      version     <- schemaVersions
    } yield KGProject(id, name, created, maybeParent, version).copy(
      maybeParent = maybeParent.map { parent =>
        parent.copy(
          created = parent.created.copy(
            date = DateCreated {
              val newDate = created.date.value.plus(2, DAYS)
              if ((newDate compareTo now) < 0) newDate else now
            }
          )
        )
      }
    )

  implicit lazy val gitLabProjects: Gen[GitLabProject] = for {
    id               <- projectIds
    maybeDescription <- projectDescriptions.toGeneratorOfOptions
    visibility       <- projectVisibilities
    urls             <- urlsObjects
    forksCount       <- forksCounts
    tags             <- setOf(tagsObjects)
    starsCount       <- starsCounts
    updatedAt        <- updatedAts
    permissions      <- permissionsObjects
    statistics       <- statisticsObjects
  } yield GitLabProject(id,
                        maybeDescription,
                        visibility,
                        urls,
                        forksCount,
                        tags,
                        starsCount,
                        updatedAt,
                        permissions,
                        statistics
  )

  private implicit lazy val urlsObjects: Gen[Urls] = for {
    sshUrl         <- sshUrls
    httpUrl        <- httpUrls
    webUrl         <- webUrls
    maybeReadmeUrl <- readmeUrls.toGeneratorOfOptions
  } yield Urls(sshUrl, httpUrl, webUrl, maybeReadmeUrl)

  private implicit lazy val forksCounts: Gen[ForksCount] = nonNegativeInts() map (v => ForksCount.apply(v.value))

  implicit def parents(implicit renkuBaseUrl: RenkuBaseUrl): Gen[Parent] =
    for {
      resourceId <- projectResourceIds()
      name       <- projectNames
      created    <- projectCreations
    } yield Parent(resourceId, name, created)

  private implicit lazy val starsCounts: Gen[StarsCount] = nonNegativeInts() map (v => StarsCount.apply(v.value))
  private implicit lazy val tagsObjects: Gen[Tag]        = nonBlankStrings() map (v => Tag(v.value))

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

  private implicit lazy val webUrls: Gen[WebUrl] = urls() map WebUrl.apply

  implicit lazy val projectCreations: Gen[ProjectCreation] = for {
    created      <- projectCreatedDates
    maybeCreator <- projectCreators.toGeneratorOfOptions
  } yield ProjectCreation(created, maybeCreator)

  implicit lazy val projectCreators: Gen[ProjectCreator] = for {
    maybeEmail <- userEmails.toGeneratorOfOptions
    name       <- userNames
  } yield ProjectCreator(maybeEmail, name)

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

  private implicit lazy val statisticsObjects: Gen[Statistics] = for {
    commitsCount     <- nonNegativeInts() map (v => CommitsCount(v.value))
    storageSize      <- nonNegativeInts() map (v => StorageSize(v.value))
    repositorySize   <- nonNegativeInts() map (v => RepositorySize(v.value))
    lfsSize          <- nonNegativeInts() map (v => LsfObjectsSize(v.value))
    jobArtifactsSize <- nonNegativeInts() map (v => JobArtifactsSize(v.value))
  } yield Statistics(commitsCount, storageSize, repositorySize, lfsSize, jobArtifactsSize)
}
