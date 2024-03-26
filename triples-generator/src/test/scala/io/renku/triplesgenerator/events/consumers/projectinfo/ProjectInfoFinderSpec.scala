/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.projectinfo

import cats.data.EitherT
import cats.data.EitherT.rightT
import cats.effect.IO
import cats.syntax.all._
import io.renku.http.client.GitLabGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.{personEmails, personNames, projectSlugs}
import io.renku.graph.model.gitlab.{GitLabMember, GitLabProjectInfo}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Role
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesgenerator.errors.ErrorGenerators.processingRecoverableErrors
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectInfoFinderSpec
    extends AnyWordSpec
    with IOSpec
    with MockFactory
    with should.Matchers
    with ScalaCheckPropertyChecks {

  val gitlabMemberNoEmail: Gen[GitLabMember] = projectMembersNoEmail

  "findProjectInfo" should {

    "return info about the project, its creator and members with matched emails" in new TestCase {
      forAll(gitLabProjectInfos, projectMembersNoEmail, projectMembersNoEmail.toGeneratorOfSet()) {
        (info: GitLabProjectInfo, creator: GitLabMember, members: Set[GitLabMember]) =>
          val infoWithCreator = info.copy(maybeCreator = creator.user.some, members = Set.empty)
          (projectFinder
            .findProject(_: projects.Slug)(_: AccessToken))
            .expects(infoWithCreator.slug, accessToken)
            .returning(rightT[IO, ProcessingRecoverableError](infoWithCreator.some))
          (membersFinder
            .findProjectMembers(_: projects.Slug)(_: AccessToken))
            .expects(infoWithCreator.slug, accessToken)
            .returning(rightT[IO, ProcessingRecoverableError](members))
          val updatedMembers = members map { member =>
            val updatedMember = memberGitLabEmailLens.replace(Some(personEmails.generateOne))(member)
            (memberEmailFinder
              .findMemberEmail(_: GitLabMember, _: Project)(_: AccessToken))
              .expects(member, Project(infoWithCreator.id, infoWithCreator.slug), accessToken)
              .returning(rightT[IO, ProcessingRecoverableError](updatedMember))
            updatedMember
          }
          val updatedCreator = memberGitLabEmailLens.replace(Some(personEmails.generateOne))(creator)
          (memberEmailFinder
            .findMemberEmail(_: GitLabMember, _: Project)(_: AccessToken))
            .expects(creator.withOwnerLevel, Project(infoWithCreator.id, infoWithCreator.slug), accessToken)
            .returning(rightT[IO, ProcessingRecoverableError](updatedCreator))

          finder
            .findProjectInfo(infoWithCreator.slug)
            .value
            .unsafeRunSync() shouldBe infoWithCreator
            .copy(maybeCreator = updatedCreator.user.some, members = updatedMembers)
            .some
            .asRight
      }
    }

    "return no info if project cannot be found" in new TestCase {
      val slug = projectSlugs.generateOne

      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](None))

      finder.findProjectInfo(slug).value.unsafeRunSync() shouldBe None.asRight
    }

    "return project info with creator only if no members can be found" in new TestCase {
      val creator     = projectMembersNoEmail.generateOne
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = Some(creator.user), members = Set.empty)

      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](projectInfo.some))
      (membersFinder
        .findProjectMembers(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](Set.empty))
      val updatedCreator =
        gitLabProjectMembers.map(memberGitLabIdLens.modify(_ => creator.user.gitLabId)).generateOne
      (memberEmailFinder
        .findMemberEmail(_: GitLabMember, _: Project)(_: AccessToken))
        .expects(creator.withOwnerLevel, Project(projectInfo.id, projectInfo.slug), accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](updatedCreator))

      finder.findProjectInfo(projectInfo.slug).value.unsafeRunSync() shouldBe projectInfo
        .copy(maybeCreator = Some(updatedCreator.user))
        .some
        .asRight
    }

    "return project info without creator but with members if they can be found" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = None, members = Set.empty)

      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](projectInfo.some))
      val members: Set[GitLabMember] = projectMembersNoEmail.generateNonEmptyList().toList.toSet
      (membersFinder
        .findProjectMembers(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](members))
      val updatedMembers = members map { member =>
        val updatedMember =
          gitLabProjectMembers.map(memberGitLabIdLens.modify(_ => member.user.gitLabId)).generateOne
        (memberEmailFinder
          .findMemberEmail(_: GitLabMember, _: Project)(_: AccessToken))
          .expects(member, Project(projectInfo.id, projectInfo.slug), accessToken)
          .returning(rightT[IO, ProcessingRecoverableError](updatedMember))
        updatedMember
      }

      finder.findProjectInfo(projectInfo.slug).value.unsafeRunSync() shouldBe projectInfo
        .copy(maybeCreator = None, members = updatedMembers)
        .some
        .asRight
    }

    "return project info without creator and members if none can be found" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = None, members = Set.empty)

      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](projectInfo.some))
      (membersFinder
        .findProjectMembers(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](Set.empty))

      finder.findProjectInfo(projectInfo.slug).value.unsafeRunSync() shouldBe projectInfo.some.asRight
    }

    "fail with a RecoverableError if finding project fails recoverably" in new TestCase {
      val slug = projectSlugs.generateOne

      val error = processingRecoverableErrors.generateOne
      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(slug, accessToken)
        .returning(EitherT.leftT[IO, Option[GitLabProjectInfo]](error))

      finder.findProjectInfo(slug).value.unsafeRunSync() shouldBe error.asLeft
    }

    "fail with a RecoverableError if finding members fails recoverably" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne

      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](projectInfo.some))

      val error = processingRecoverableErrors.generateOne
      (membersFinder
        .findProjectMembers(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(EitherT.leftT[IO, Set[GitLabMember]](error))

      finder.findProjectInfo(projectInfo.slug).value.unsafeRunSync() shouldBe error.asLeft
    }

    "fail with a RecoverableError if finding members' emails fails recoverably" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = None, members = Set.empty)

      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](projectInfo.some))
      val members: Set[GitLabMember] = projectMembersNoEmail.generateNonEmptyList().toList.toSet
      (membersFinder
        .findProjectMembers(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](members))
      val error = processingRecoverableErrors.generateOne
      members foreach { member =>
        (memberEmailFinder
          .findMemberEmail(_: GitLabMember, _: Project)(_: AccessToken))
          .expects(member, Project(projectInfo.id, projectInfo.slug), accessToken)
          .returning(EitherT.leftT[IO, GitLabMember](error))
      }

      finder.findProjectInfo(projectInfo.slug).value.unsafeRunSync() shouldBe error.asLeft
    }

    "fail if finding project fails non-recoverably" in new TestCase {
      val slug = projectSlugs.generateOne

      val exception = exceptions.generateOne
      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(slug, accessToken)
        .returning(EitherT(exception.raiseError[IO, Either[ProcessingRecoverableError, Option[GitLabProjectInfo]]]))

      intercept[Exception](finder.findProjectInfo(slug).value.unsafeRunSync()) shouldBe exception
    }

    "fail if finding members fails non-recoverably" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne

      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](projectInfo.some))

      val exception = exceptions.generateOne
      (membersFinder
        .findProjectMembers(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(EitherT(exception.raiseError[IO, Either[ProcessingRecoverableError, Set[GitLabMember]]]))

      intercept[Exception](finder.findProjectInfo(projectInfo.slug).value.unsafeRunSync()) shouldBe exception
    }

    "fail if finding members' emails fails non-recoverably" in new TestCase {
      val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = None, members = Set.empty)

      (projectFinder
        .findProject(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](projectInfo.some))
      val members: Set[GitLabMember] = projectMembersNoEmail.generateNonEmptyList().toList.toSet
      (membersFinder
        .findProjectMembers(_: projects.Slug)(_: AccessToken))
        .expects(projectInfo.slug, accessToken)
        .returning(rightT[IO, ProcessingRecoverableError](members))
      val exception = exceptions.generateOne
      members foreach { member =>
        (memberEmailFinder
          .findMemberEmail(_: GitLabMember, _: Project)(_: AccessToken))
          .expects(member, Project(projectInfo.id, projectInfo.slug), accessToken)
          .returning(EitherT(exception.raiseError[IO, Either[ProcessingRecoverableError, GitLabMember]]))
      }

      intercept[Exception](finder.findProjectInfo(projectInfo.slug).value.unsafeRunSync()) shouldBe exception
    }
  }

  "findProjectInfo - members de-duplication" should {

    val sameNameMember1 = projectMembersNoEmail.generateOne
    val sameNameMember2 = sameNameMember1.copy(user = sameNameMember1.user.copy(name = personNames.generateOne))
    val members         = Set(sameNameMember1, sameNameMember2)

    val sameNameMember1Updated = sameNameMember1.withEmail(personEmails.generateOne)
    val sameNameMember2Updated = sameNameMember2.withEmail(personEmails.generateOne)

    forAll {
      Table(
        ("case", "member1 after email finding", "member2 after email finding", "expected result"),
        ("email on member1", sameNameMember1Updated, sameNameMember2, sameNameMember1Updated),
        ("email on member2", sameNameMember1, sameNameMember2Updated, sameNameMember2Updated),
        ("email on both", sameNameMember1Updated, sameNameMember2Updated, sameNameMember1Updated),
        ("email on any", sameNameMember1, sameNameMember2, sameNameMember2)
      )
    } { case (caseName, member1AfterEmailFinding, member2AfterEmailFinding, expected) =>
      s"deduplicate members/creator if there are two members with the same GitLabId but different name - $caseName" in new TestCase {
        val projectInfo = gitLabProjectInfos.generateOne.copy(maybeCreator = None, members = members)

        (projectFinder
          .findProject(_: projects.Slug)(_: AccessToken))
          .expects(projectInfo.slug, accessToken)
          .returning(rightT[IO, ProcessingRecoverableError](projectInfo.some))
        (membersFinder
          .findProjectMembers(_: projects.Slug)(_: AccessToken))
          .expects(projectInfo.slug, accessToken)
          .returning(rightT[IO, ProcessingRecoverableError](members))

        (memberEmailFinder
          .findMemberEmail(_: GitLabMember, _: Project)(_: AccessToken))
          .expects(sameNameMember1, Project(projectInfo.id, projectInfo.slug), accessToken)
          .returning(rightT[IO, ProcessingRecoverableError](member1AfterEmailFinding))
        (memberEmailFinder
          .findMemberEmail(_: GitLabMember, _: Project)(_: AccessToken))
          .expects(sameNameMember2, Project(projectInfo.id, projectInfo.slug), accessToken)
          .returning(rightT[IO, ProcessingRecoverableError](member2AfterEmailFinding))

        finder.findProjectInfo(projectInfo.slug).value.unsafeRunSync() shouldBe projectInfo
          .copy(members = Set(expected))
          .some
          .asRight
      }
    }
  }

  private class TestCase {
    implicit val accessToken: AccessToken = accessTokens.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectFinder     = mock[ProjectFinder[IO]]
    val membersFinder     = mock[ProjectMembersFinder[IO]]
    val memberEmailFinder = mock[MemberEmailFinder[IO]]
    lazy val finder       = new ProjectInfoFinderImpl[IO](projectFinder, membersFinder, memberEmailFinder)

    implicit class GitLabMemberOps(m: GitLabMember) {
      def withOwnerLevel: GitLabMember = m.copy(accessLevel = Role.toGitLabAccessLevel(Role.Owner))
    }
  }
}
