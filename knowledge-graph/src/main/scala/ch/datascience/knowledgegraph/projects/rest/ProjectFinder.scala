/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.projects.rest

import cats.MonadError
import cats.implicits._
import ch.datascience.control.Throttler
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import IOAccessTokenFinder._
import cats.data.OptionT
import ch.datascience.knowledgegraph.config.GitLab
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder.KGProject

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class ProjectFinder[Interpretation[_]](
    kgProjectFinder:     KGProjectFinder[Interpretation],
    gitLabProjectFinder: GitLabProjectFinder[Interpretation],
    accessTokenFinder:   AccessTokenFinder[Interpretation]
)(implicit ME:           MonadError[Interpretation, Throwable]) {

  import accessTokenFinder._
  import gitLabProjectFinder.{findProject => findInGitLab}
  import kgProjectFinder.{findProject => findInKG}

  def findProject(path: ProjectPath): Interpretation[Option[Project]] = {
    OptionT(findInKG(path)) semiflatMap { kgProject =>
      for {
        accessToken   <- OptionT(findAccessToken(path)) getOrElseF raiseError(s"No access token for $path")
        gitLabProject <- findInGitLab(path, Some(accessToken)) getOrElseF raiseError(s"No GitLab project for $path")
      } yield merge(path, kgProject, gitLabProject)
    }
  }.value

  private def raiseError[Out](message: String) = new Exception(message).raiseError[Interpretation, Out]

  private def merge(path: ProjectPath, kgProject: KGProject, gitLabProject: GitLabProject) =
    Project(
      path = path,
      name = kgProject.name,
      created = Creation(
        date    = kgProject.created.date,
        creator = Creator(kgProject.created.creator.email, kgProject.created.creator.name)
      ),
      repoUrls = RepoUrls(gitLabProject.urls.ssh, gitLabProject.urls.http)
    )
}

private object IOProjectFinder {
  import cats.effect.{ContextShift, IO, Timer}
  import com.typesafe.config.{Config, ConfigFactory}
  import io.chrisdavenport.log4cats.Logger

  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO],
      config:          Config = ConfigFactory.load()
  )(implicit ME:       MonadError[IO, Throwable],
    executionContext:  ExecutionContext,
    contextShift:      ContextShift[IO],
    timer:             Timer[IO]): IO[ProjectFinder[IO]] =
    for {
      kgProjectFinder     <- IOKGProjectFinder(logger = logger)
      gitLabProjectFinder <- IOGitLabProjectFinder(gitLabThrottler, logger)
      accessTokenFinder   <- IOAccessTokenFinder(logger)
    } yield new ProjectFinder[IO](kgProjectFinder, gitLabProjectFinder, accessTokenFinder)
}
