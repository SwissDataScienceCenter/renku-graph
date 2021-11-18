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

package io.renku.triplesgenerator.events.categories.triplesgenerated
package projectinfo

import TriplesGeneratedGenerators._
import cats.data.EitherT
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.entities.Project.{GitLabProjectInfo, ProjectMember}
import io.renku.graph.model.projects
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Try

class ProjectInfoFinderSpec extends AnyWordSpec with MockFactory with should.Matchers with ScalaCheckPropertyChecks {

  "findProjectInfo" should {

    "return info about the project and its members" in new TestCase {
      forAll { (info: GitLabProjectInfo, members: Set[ProjectMember]) =>
        (projectFinder
          .findProject(_: projects.Path)(_: Option[AccessToken]))
          .expects(info.path, maybeAccessToken)
          .returning(EitherT.rightT[Try, ProcessingRecoverableError](info.some))
        (membersFinder
          .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
          .expects(info.path, maybeAccessToken)
          .returning(EitherT.rightT[Try, ProcessingRecoverableError](members))

        finder
          .findProjectInfo(info.path)
          .value shouldBe info.copy(members = members).some.asRight.pure[Try]
      }
    }

    "return no info if project cannot be found" in new TestCase {
      val path = projectPaths.generateOne

      (projectFinder
        .findProject(_: projects.Path)(_: Option[AccessToken]))
        .expects(path, maybeAccessToken)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](None))

      finder.findProjectInfo(path).value shouldBe None.asRight.pure[Try]
    }

    "return project info without members if none can be found" in new TestCase {
      val info = gitLabProjectInfos.generateOne

      (projectFinder
        .findProject(_: projects.Path)(_: Option[AccessToken]))
        .expects(info.path, maybeAccessToken)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](info.some))
      (membersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(info.path, maybeAccessToken)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](Set.empty))

      finder.findProjectInfo(info.path).value shouldBe info.copy(members = Set.empty).some.asRight.pure[Try]
    }

    "fail with a RecoverableError if finding project fails recoverably" in new TestCase {
      val path = projectPaths.generateOne

      val error = transformationRecoverableErrors.generateOne
      (projectFinder
        .findProject(_: projects.Path)(_: Option[AccessToken]))
        .expects(path, maybeAccessToken)
        .returning(EitherT.leftT[Try, Option[GitLabProjectInfo]](error))

      finder.findProjectInfo(path).value shouldBe error.asLeft.pure[Try]
    }

    "fail with a RecoverableError if finding members fails recoverably" in new TestCase {
      val info = gitLabProjectInfos.generateOne

      (projectFinder
        .findProject(_: projects.Path)(_: Option[AccessToken]))
        .expects(info.path, maybeAccessToken)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](info.some))

      val error = transformationRecoverableErrors.generateOne
      (membersFinder
        .findProjectMembers(_: projects.Path)(_: Option[AccessToken]))
        .expects(info.path, maybeAccessToken)
        .returning(EitherT.leftT[Try, Set[ProjectMember]](error))

      finder.findProjectInfo(info.path).value shouldBe error.asLeft.pure[Try]
    }

    "fail if finding members fails non-recoverably" in new TestCase {
      val path = projectPaths.generateOne

      val exception = exceptions.generateOne
      (projectFinder
        .findProject(_: projects.Path)(_: Option[AccessToken]))
        .expects(path, maybeAccessToken)
        .returning(EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]))

      finder.findProjectInfo(path).value shouldBe exception
        .raiseError[Try, Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption

    private implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val projectFinder = mock[ProjectFinder[Try]]
    val membersFinder = mock[ProjectMembersFinder[Try]]
    val finder        = new ProjectInfoFinderImpl[Try](projectFinder, membersFinder)
  }
}
