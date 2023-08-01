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

package io.renku.knowledgegraph.projects.details

import Converters._
import GitLabProjectFinder.GitLabProject
import KGProjectFinder.KGProject
import ProjectsGenerators._
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.projects.Slug
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.graph.tokenrepository.AccessTokenFinder.Implicits.projectSlugToPath
import io.renku.http.client.AccessToken
import io.renku.http.server.security.model.AuthUser
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectFinderSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "findProject" should {

    "merge the project metadata found in the KG and in GitLab" in new TestCase {
      val maybeAuthUser = authUsers.generateSome

      val kgProject = anyProjectEntities.generateOne.to(kgProjectConverter)
      givenKgProjectFinder(kgProject.slug, maybeAuthUser, returning = kgProject.some.pure[IO])

      val accessToken = givenAccessToken(existsFor = kgProject.slug)

      val gitLabProject = gitLabProjects.generateOne
      givenGLProjectFinder(kgProject.slug, accessToken, returning = gitLabProject.some.pure[IO])

      projectFinder.findProject(kgProject.slug, maybeAuthUser).unsafeRunSync() shouldBe Some(
        projectFrom(kgProject, gitLabProject)
      )
    }

    "merge the project metadata found in the KG and in GitLab - case when no AuthUser given" in new TestCase {

      val kgProject = anyProjectEntities.generateOne.to(kgProjectConverter)
      givenKgProjectFinder(kgProject.slug, maybeAuthUser = None, returning = kgProject.some.pure[IO])

      val accessToken = givenAccessToken(existsFor = kgProject.slug)

      val gitLabProject = gitLabProjects.generateOne
      givenGLProjectFinder(kgProject.slug, accessToken, returning = gitLabProject.some.pure[IO])

      projectFinder.findProject(kgProject.slug, maybeAuthUser = None).unsafeRunSync() shouldBe Some(
        projectFrom(kgProject, gitLabProject)
      )
    }

    "return None if there's no project for the slug in the KG" in new TestCase {
      val maybeAuthUser = authUsers.generateSome

      val projectSlug = projectSlugs.generateOne
      givenKgProjectFinder(projectSlug, maybeAuthUser, returning = None.pure[IO])

      val accessToken = givenAccessToken(existsFor = projectSlug)

      val gitLabProject = gitLabProjects.generateOne
      givenGLProjectFinder(projectSlug, accessToken, returning = gitLabProject.some.pure[IO])

      projectFinder.findProject(projectSlug, maybeAuthUser).unsafeRunSync() shouldBe None
    }

    "return None if there's no project for the slug in GitLab" in new TestCase {
      val maybeAuthUser = authUsers.generateSome

      val kgProject = anyProjectEntities.generateOne.to(kgProjectConverter)
      givenKgProjectFinder(kgProject.slug, maybeAuthUser, returning = kgProject.some.pure[IO])

      val accessToken = givenAccessToken(existsFor = kgProject.slug)

      givenGLProjectFinder(kgProject.slug, accessToken, returning = None.pure[IO])

      projectFinder.findProject(kgProject.slug, maybeAuthUser).unsafeRunSync() shouldBe None
    }

    "return None if no access token can be found for the given project slug" in new TestCase {

      val kgProject = anyProjectEntities.generateOne.to(kgProjectConverter)
      givenKgProjectFinder(kgProject.slug, maybeAuthUser = None, returning = kgProject.some.pure[IO])

      (accessTokenFinder
        .findAccessToken(_: Slug)(_: Slug => String))
        .expects(kgProject.slug, projectSlugToPath)
        .returning(Option.empty[AccessToken].pure[IO])

      projectFinder.findProject(kgProject.slug, maybeAuthUser = None).unsafeRunSync() shouldBe None
    }

    "fail if finding project in the KG failed" in new TestCase {
      val maybeAuthUser = authUsers.generateSome
      val projectSlug   = projectSlugs.generateOne

      val exception = exceptions.generateOne
      givenKgProjectFinder(projectSlug, maybeAuthUser, returning = exception.raiseError[IO, Option[KGProject]])

      val accessToken = givenAccessToken(existsFor = projectSlug)

      givenGLProjectFinder(projectSlug, accessToken, returning = None.pure[IO]).noMoreThanOnce()

      intercept[Exception] {
        projectFinder.findProject(projectSlug, maybeAuthUser).unsafeRunSync()
      } shouldBe exception
    }

    "fail if finding access token failed" in new TestCase {

      val kgProject = anyProjectEntities.generateOne.to(kgProjectConverter)
      givenKgProjectFinder(kgProject.slug, maybeAuthUser = None, returning = kgProject.some.pure[IO])

      val exception = exceptions.generateOne
      (accessTokenFinder
        .findAccessToken(_: Slug)(_: Slug => String))
        .expects(kgProject.slug, projectSlugToPath)
        .returning(exception.raiseError[IO, Option[AccessToken]])

      intercept[Exception] {
        projectFinder.findProject(kgProject.slug, maybeAuthUser = None).unsafeRunSync()
      } shouldBe exception
    }

    "fail if finding project in GitLab failed" in new TestCase {

      val maybeAuthUser = authUsers.generateSome
      val kgProject     = anyProjectEntities.generateOne.to(kgProjectConverter)
      givenKgProjectFinder(kgProject.slug, maybeAuthUser, returning = kgProject.some.pure[IO])

      val accessToken = givenAccessToken(existsFor = kgProject.slug)

      val exception = exceptions.generateOne
      givenGLProjectFinder(kgProject.slug, accessToken, returning = exception.raiseError[IO, Option[GitLabProject]])

      intercept[Exception] {
        projectFinder.findProject(kgProject.slug, maybeAuthUser).unsafeRunSync()
      } shouldBe exception
    }
  }

  private trait TestCase {
    implicit val accessTokenFinder: AccessTokenFinder[IO] = mock[AccessTokenFinder[IO]]
    val kgProjectFinder     = mock[KGProjectFinder[IO]]
    val gitLabProjectFinder = mock[GitLabProjectFinder[IO]]
    val projectFinder       = new ProjectFinderImpl[IO](kgProjectFinder, gitLabProjectFinder)

    def givenKgProjectFinder(slug: Slug, maybeAuthUser: Option[AuthUser], returning: IO[Option[KGProject]]) =
      (kgProjectFinder
        .findProject(_: Slug, _: Option[AuthUser]))
        .expects(slug, maybeAuthUser)
        .returning(returning)

    def givenGLProjectFinder(slug: Slug, accessToken: AccessToken, returning: IO[Option[GitLabProject]]) =
      (gitLabProjectFinder
        .findProject(_: Slug)(_: AccessToken))
        .expects(slug, accessToken)
        .returning(returning)

    def givenAccessToken(existsFor: Slug): AccessToken = {
      val accessToken = accessTokens.generateOne
      (accessTokenFinder
        .findAccessToken(_: Slug)(_: Slug => String))
        .expects(existsFor, projectSlugToPath)
        .returning(Some(accessToken).pure[IO])
      accessToken
    }
  }

  private def projectFrom(kgProject: KGProject, gitLabProject: GitLabProject) =
    model.Project(
      resourceId = kgProject.resourceId,
      id = gitLabProject.id,
      slug = kgProject.slug,
      name = kgProject.name,
      maybeDescription = kgProject.maybeDescription,
      visibility = kgProject.visibility,
      created = model.Creation(
        kgProject.created.date,
        kgProject.created.maybeCreator.map(toModelCreator)
      ),
      dateModified = kgProject.dateModified,
      urls = gitLabProject.urls,
      forking = model.Forking(
        gitLabProject.forksCount,
        kgProject.maybeParent.map { parent =>
          model.ParentProject(
            parent.resourceId,
            parent.slug,
            parent.name,
            model.Creation(
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
}
