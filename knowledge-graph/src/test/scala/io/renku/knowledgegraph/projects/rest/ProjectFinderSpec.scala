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

package io.renku.knowledgegraph.projects.rest

import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.projects.Path
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.projectPathToPath
import io.renku.http.client.AccessToken
import io.renku.knowledgegraph.projects.model
import io.renku.knowledgegraph.projects.rest.Converters._
import io.renku.knowledgegraph.projects.rest.GitLabProjectFinder.GitLabProject
import io.renku.knowledgegraph.projects.rest.KGProjectFinder.KGProject
import io.renku.knowledgegraph.projects.rest.ProjectsGenerators._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectFinderSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "findProject" should {

    "merge the project metadata found in the KG and in GitLab" in new TestCase {
      val kgProject = anyProjectEntities.generateOne.to[KGProject]
      (kgProjectFinder
        .findProject(_: Path))
        .expects(kgProject.path)
        .returning(kgProject.some.pure[IO])

      val maybeAuthUser = authUsers.generateSome
      val gitLabProject = gitLabProjects.generateOne
      (gitLabProjectFinder
        .findProject(_: Path, _: Option[AccessToken]))
        .expects(kgProject.path, maybeAuthUser.map(_.accessToken))
        .returning(OptionT.some[IO](gitLabProject))

      projectFinder.findProject(kgProject.path, maybeAuthUser).unsafeRunSync() shouldBe Some(
        projectFrom(kgProject, gitLabProject)
      )
    }

    "merge the project metadata found in the KG and in GitLab - case when no AuthUser given" in new TestCase {

      val kgProject = anyProjectEntities.generateOne.to[KGProject]
      (kgProjectFinder
        .findProject(_: Path))
        .expects(kgProject.path)
        .returning(kgProject.some.pure[IO])

      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(kgProject.path, projectPathToPath)
        .returning(Some(accessToken).pure[IO])

      val gitLabProject = gitLabProjects.generateOne
      (gitLabProjectFinder
        .findProject(_: Path, _: Option[AccessToken]))
        .expects(kgProject.path, Some(accessToken))
        .returning(OptionT.some[IO](gitLabProject))

      projectFinder.findProject(kgProject.path, maybeAuthUser = None).unsafeRunSync() shouldBe Some(
        projectFrom(kgProject, gitLabProject)
      )
    }

    "return None if there's no project for the path in the KG" in new TestCase {

      val projectPath = projectPaths.generateOne
      (kgProjectFinder
        .findProject(_: Path))
        .expects(projectPath)
        .returning(Option.empty[KGProject].pure[IO])

      val maybeAuthUser = authUsers.generateSome
      val gitLabProject = gitLabProjects.generateOne
      (gitLabProjectFinder
        .findProject(_: Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAuthUser.map(_.accessToken))
        .returning(OptionT.some[IO](gitLabProject))

      projectFinder.findProject(projectPath, maybeAuthUser).unsafeRunSync() shouldBe None
    }

    "return None if there's no project for the path in GitLab" in new TestCase {

      val kgProject = anyProjectEntities.generateOne.to[KGProject]
      (kgProjectFinder
        .findProject(_: Path))
        .expects(kgProject.path)
        .returning(kgProject.some.pure[IO])

      val maybeAuthUser = authUsers.generateSome
      (gitLabProjectFinder
        .findProject(_: Path, _: Option[AccessToken]))
        .expects(kgProject.path, maybeAuthUser.map(_.accessToken))
        .returning(OptionT.none[IO, GitLabProject])

      projectFinder.findProject(kgProject.path, maybeAuthUser).unsafeRunSync() shouldBe None
    }

    "return None if no access token can be found for the given project path" in new TestCase {

      val kgProject = anyProjectEntities.generateOne.to[KGProject]
      (kgProjectFinder
        .findProject(_: Path))
        .expects(kgProject.path)
        .returning(kgProject.some.pure[IO])

      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(kgProject.path, projectPathToPath)
        .returning(Option.empty[AccessToken].pure[IO])

      projectFinder.findProject(kgProject.path, maybeAuthUser = None).unsafeRunSync() shouldBe None
    }

    "fail if finding project in the KG failed" in new TestCase {

      val projectPath = projectPaths.generateOne
      val exception   = exceptions.generateOne
      (kgProjectFinder
        .findProject(_: Path))
        .expects(projectPath)
        .returning(exception.raiseError[IO, Option[KGProject]])

      val maybeAuthUser = authUsers.generateSome
      (gitLabProjectFinder
        .findProject(_: Path, _: Option[AccessToken]))
        .expects(projectPath, maybeAuthUser.map(_.accessToken))
        .returning(OptionT.none[IO, GitLabProject])
        .noMoreThanOnce()

      intercept[Exception] {
        projectFinder.findProject(projectPath, maybeAuthUser).unsafeRunSync()
      } shouldBe exception
    }

    "fail if finding access token failed" in new TestCase {

      val kgProject = anyProjectEntities.generateOne.to[KGProject]
      (kgProjectFinder
        .findProject(_: Path))
        .expects(kgProject.path)
        .returning(Some(kgProject).pure[IO])

      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: Path)(_: Path => String))
        .expects(kgProject.path, projectPathToPath)
        .returning(exception.raiseError[IO, Option[AccessToken]])

      intercept[Exception] {
        projectFinder.findProject(kgProject.path, maybeAuthUser = None).unsafeRunSync()
      } shouldBe exception
    }

    "fail if finding project in GitLab failed" in new TestCase {

      val kgProject = anyProjectEntities.generateOne.to[KGProject]
      (kgProjectFinder
        .findProject(_: Path))
        .expects(kgProject.path)
        .returning(Some(kgProject).pure[IO])

      val maybeAuthUser = authUsers.generateSome
      val exception     = exceptions.generateOne
      (gitLabProjectFinder
        .findProject(_: Path, _: Option[AccessToken]))
        .expects(kgProject.path, maybeAuthUser.map(_.accessToken))
        .returning(OptionT.liftF(exception.raiseError[IO, GitLabProject]))

      intercept[Exception] {
        projectFinder.findProject(kgProject.path, maybeAuthUser).unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    val kgProjectFinder     = mock[KGProjectFinder[IO]]
    val accessTokenFinder   = mock[AccessTokenFinder[IO]]
    val gitLabProjectFinder = mock[GitLabProjectFinder[IO]]
    val projectFinder       = new ProjectFinderImpl[IO](kgProjectFinder, gitLabProjectFinder, accessTokenFinder)
  }

  private def projectFrom(kgProject: KGProject, gitLabProject: GitLabProject) =
    model.Project(
      id = gitLabProject.id,
      path = kgProject.path,
      name = kgProject.name,
      maybeDescription = gitLabProject.maybeDescription,
      visibility = kgProject.visibility,
      created = model.Creation(
        date = kgProject.created.date,
        maybeCreator = kgProject.created.maybeCreator.map(creator => model.Creator(creator.maybeEmail, creator.name))
      ),
      updatedAt = gitLabProject.updatedAt,
      urls = gitLabProject.urls,
      forking = model.Forking(
        gitLabProject.forksCount,
        kgProject.maybeParent.map { parent =>
          model.ParentProject(
            parent.resourceId.toUnsafe[Path],
            parent.name,
            model.Creation(parent.created.date,
                           parent.created.maybeCreator.map(creator => model.Creator(creator.maybeEmail, creator.name))
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
}
