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
import ch.datascience.generators.Generators.{httpUrls => urls, _}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.knowledgegraph.projects.model.RepoUrls.{HttpUrl, SshUrl}
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.{ForksCount, GitLabProject, ProjectUrls, StarsCount}
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder._
import org.scalacheck.Gen

object ProjectsGenerators {

  implicit val projects: Gen[Project] = for {
    kgProject     <- kgProjects
    gitLabProject <- gitLabProjects
  } yield Project(
    id         = gitLabProject.id,
    path       = kgProject.path,
    name       = kgProject.name,
    visibility = gitLabProject.visibility,
    created = Creation(
      date    = kgProject.created.date,
      creator = Creator(email = kgProject.created.creator.email, name = kgProject.created.creator.name)
    ),
    repoUrls   = RepoUrls(ssh = gitLabProject.urls.ssh, http = gitLabProject.urls.http),
    forksCount = gitLabProject.forksCount,
    starsCount = gitLabProject.starsCount
  )

  implicit lazy val kgProjects: Gen[KGProject] = for {
    id      <- projectPaths
    name    <- projectNames
    created <- projectCreations
  } yield KGProject(id, name, created)

  implicit lazy val gitLabProjects: Gen[GitLabProject] = for {
    id         <- projectIds
    visibility <- projectVisibilities
    urls       <- projectUrlObjects
    forksCount <- forksCounts
    starsCount <- starsCounts
  } yield GitLabProject(id, visibility, urls, forksCount, starsCount)

  private implicit lazy val projectUrlObjects: Gen[ProjectUrls] = for {
    sshUrl  <- sshUrls
    httpUrl <- httpUrls
  } yield ProjectUrls(httpUrl, sshUrl)

  private implicit lazy val sshUrls: Gen[SshUrl] = for {
    hostParts   <- nonEmptyList(nonBlankStrings())
    projectPath <- projectPaths
  } yield SshUrl(s"git@${hostParts.toList.mkString(".")}:$projectPath.git")

  private implicit lazy val forksCounts: Gen[ForksCount] = nonNegativeInts() map (v => ForksCount.apply(v.value))
  private implicit lazy val starsCounts: Gen[StarsCount] = nonNegativeInts() map (v => StarsCount.apply(v.value))

  private implicit lazy val httpUrls: Gen[HttpUrl] = for {
    url         <- urls()
    projectPath <- projectPaths
  } yield HttpUrl(s"$url/$projectPath.git")

  private implicit lazy val projectCreations: Gen[ProjectCreation] = for {
    created <- projectCreatedDates
    creator <- projectCreators
  } yield ProjectCreation(created, creator)

  implicit lazy val projectCreators: Gen[ProjectCreator] = for {
    email <- emails
    name  <- names
  } yield ProjectCreator(email, name)
}
