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

import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.graph.tokenrepository.AccessTokenFinder
import ch.datascience.graph.tokenrepository.IOAccessTokenFinder.projectPathToPath
import ch.datascience.http.client.AccessToken
import ch.datascience.knowledgegraph.projects.ProjectsGenerators._
import ch.datascience.knowledgegraph.projects.model._
import ch.datascience.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder.KGProject
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.concurrent.ExecutionContext

class ProjectFinderSpec extends WordSpec with MockFactory {

  "findProject" should {

    "merge the project metadata found in the KG and in GitLab" in new TestCase {

      val kgProject = kgProjects.generateOne.copy(path = path)
      (kgProjectFinder
        .findProject(_: ProjectPath))
        .expects(path)
        .returning(Some(kgProject).pure[IO])

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectPath)(_: ProjectPath => String))
        .expects(path, projectPathToPath)
        .returning(Some(accessToken).pure[IO])

      val gitLabProject = gitLabProjects.generateOne
      (gitLabProjectFinder
        .findProject(_: ProjectPath, _: Option[AccessToken]))
        .expects(path, Some(accessToken))
        .returning(OptionT.some[IO](gitLabProject))

      projectFinder.findProject(path).unsafeRunSync() shouldBe Some(projectFrom(kgProject, gitLabProject))
    }

    "return None if there's no project for the path in the KG" in new TestCase {

      (kgProjectFinder
        .findProject(_: ProjectPath))
        .expects(path)
        .returning(Option.empty[KGProject].pure[IO])

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectPath)(_: ProjectPath => String))
        .expects(path, projectPathToPath)
        .returning(Some(accessToken).pure[IO])

      val gitLabProject = gitLabProjects.generateOne
      (gitLabProjectFinder
        .findProject(_: ProjectPath, _: Option[AccessToken]))
        .expects(path, Some(accessToken))
        .returning(OptionT.some[IO](gitLabProject))

      projectFinder.findProject(path).unsafeRunSync() shouldBe None
    }

    "return None if there's no project for the path in GitLab" in new TestCase {

      val kgProject = kgProjects.generateOne.copy(path = path)
      (kgProjectFinder
        .findProject(_: ProjectPath))
        .expects(path)
        .returning(Some(kgProject).pure[IO])

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectPath)(_: ProjectPath => String))
        .expects(path, projectPathToPath)
        .returning(Some(accessToken).pure[IO])

      (gitLabProjectFinder
        .findProject(_: ProjectPath, _: Option[AccessToken]))
        .expects(path, Some(accessToken))
        .returning(OptionT.none[IO, GitLabProject])

      projectFinder.findProject(path).unsafeRunSync() shouldBe None
    }

    "fail if finding project in the KG failed" in new TestCase {

      val exception = exceptions.generateOne
      (kgProjectFinder
        .findProject(_: ProjectPath))
        .expects(path)
        .returning(exception.raiseError[IO, Option[KGProject]])

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectPath)(_: ProjectPath => String))
        .expects(path, projectPathToPath)
        .returning(Some(accessToken).pure[IO])

      (gitLabProjectFinder
        .findProject(_: ProjectPath, _: Option[AccessToken]))
        .expects(path, Some(accessToken))
        .returning(OptionT.none[IO, GitLabProject])

      intercept[Exception] {
        projectFinder.findProject(path).unsafeRunSync()
      } shouldBe exception
    }

    "fail if no access token can be found for the given project path" in new TestCase {

      val kgProject = kgProjects.generateOne.copy(path = path)
      (kgProjectFinder
        .findProject(_: ProjectPath))
        .expects(path)
        .returning(Some(kgProject).pure[IO])

      (accessTokenFinder
        .findAccessToken(_: ProjectPath)(_: ProjectPath => String))
        .expects(path, projectPathToPath)
        .returning(Option.empty[AccessToken].pure[IO])

      intercept[Exception] {
        projectFinder.findProject(path).unsafeRunSync()
      }.getMessage shouldBe s"No access token for $path"
    }

    "fail if finding access token failed" in new TestCase {

      val kgProject = kgProjects.generateOne.copy(path = path)
      (kgProjectFinder
        .findProject(_: ProjectPath))
        .expects(path)
        .returning(Some(kgProject).pure[IO])

      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectPath)(_: ProjectPath => String))
        .expects(path, projectPathToPath)
        .returning(exception.raiseError[IO, Option[AccessToken]])

      intercept[Exception] {
        projectFinder.findProject(path).unsafeRunSync()
      } shouldBe exception
    }

    "fail if finding project in GitLab failed" in new TestCase {

      val kgProject = kgProjects.generateOne.copy(path = path)
      (kgProjectFinder
        .findProject(_: ProjectPath))
        .expects(path)
        .returning(Some(kgProject).pure[IO])

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: ProjectPath)(_: ProjectPath => String))
        .expects(path, projectPathToPath)
        .returning(Some(accessToken).pure[IO])

      val exception = exceptions.generateOne
      (gitLabProjectFinder
        .findProject(_: ProjectPath, _: Option[AccessToken]))
        .expects(path, Some(accessToken))
        .returning(OptionT.liftF(exception.raiseError[IO, GitLabProject]))

      intercept[Exception] {
        projectFinder.findProject(path).unsafeRunSync()
      } shouldBe exception
    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private trait TestCase {
    val path = projectPaths.generateOne

    val kgProjectFinder     = mock[KGProjectFinder[IO]]
    val accessTokenFinder   = mock[AccessTokenFinder[IO]]
    val gitLabProjectFinder = mock[GitLabProjectFinder[IO]]
    val projectFinder       = new IOProjectFinder(kgProjectFinder, gitLabProjectFinder, accessTokenFinder)
  }

  private def projectFrom(kgProject: KGProject, gitLabProject: GitLabProject) = Project(
    path = kgProject.path,
    name = kgProject.name,
    created = Creation(
      date    = kgProject.created.date,
      creator = Creator(email = kgProject.created.creator.email, name = kgProject.created.creator.name)
    ),
    repoUrls = RepoUrls(ssh = gitLabProject.urls.ssh, http = gitLabProject.urls.http)
  )
}
