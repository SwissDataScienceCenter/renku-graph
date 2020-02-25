/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import ch.datascience.generators.CommonGraphGenerators.{emails, names}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{httpUrls => urls, _}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.knowledgegraph.projects.model.RepoUrls.{HttpUrl, ReadmeUrl, SshUrl, WebUrl}
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.{DateUpdated, ForksCount, GitLabProject, ProjectUrls, StarsCount}
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder._
import org.scalacheck.Gen

object ProjectsGenerators {

  implicit val projects: Gen[Project] = for {
    kgProject     <- kgProjects
    gitLabProject <- gitLabProjects
    urls = gitLabProject.urls
  } yield Project(
    id               = gitLabProject.id,
    path             = kgProject.path,
    name             = kgProject.name,
    maybeDescription = gitLabProject.maybeDescription,
    visibility       = gitLabProject.visibility,
    created = Creation(
      date    = kgProject.created.date,
      creator = Creator(email = kgProject.created.creator.email, name = kgProject.created.creator.name)
    ),
    repoUrls   = RepoUrls(urls.ssh, urls.http, urls.web, urls.readme),
    forksCount = gitLabProject.forksCount,
    starsCount = gitLabProject.starsCount,
    updatedAt  = gitLabProject.updatedAt
  )

  implicit lazy val kgProjects: Gen[KGProject] = for {
    id      <- projectPaths
    name    <- projectNames
    created <- projectCreations
  } yield KGProject(id, name, created)

  implicit lazy val gitLabProjects: Gen[GitLabProject] = for {
    id               <- projectIds
    maybeDescription <- projectDescriptions.toGeneratorOfOptions
    visibility       <- projectVisibilities
    urls             <- projectUrlObjects
    forksCount       <- forksCounts
    starsCount       <- starsCounts
    updatedAt        <- updatedAts
  } yield GitLabProject(id, maybeDescription, visibility, urls, forksCount, starsCount, updatedAt)

  private implicit lazy val projectUrlObjects: Gen[ProjectUrls] = for {
    sshUrl    <- sshUrls
    httpUrl   <- httpUrls
    webUrl    <- webUrls
    readmeUrl <- readmeUrls
  } yield ProjectUrls(httpUrl, sshUrl, webUrl, readmeUrl)

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

  private implicit lazy val webUrls:     Gen[WebUrl]      = urls() map WebUrl.apply
  private implicit lazy val forksCounts: Gen[ForksCount]  = nonNegativeInts() map (v => ForksCount.apply(v.value))
  private implicit lazy val starsCounts: Gen[StarsCount]  = nonNegativeInts() map (v => StarsCount.apply(v.value))
  private implicit lazy val updatedAts:  Gen[DateUpdated] = timestampsNotInTheFuture map DateUpdated.apply

  private implicit lazy val projectCreations: Gen[ProjectCreation] = for {
    created <- projectCreatedDates
    creator <- projectCreators
  } yield ProjectCreation(created, creator)

  implicit lazy val projectCreators: Gen[ProjectCreator] = for {
    email <- emails
    name  <- names
  } yield ProjectCreator(email, name)
}
