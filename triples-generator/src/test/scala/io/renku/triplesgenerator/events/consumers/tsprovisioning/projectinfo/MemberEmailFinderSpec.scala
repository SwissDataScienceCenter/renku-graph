/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning
package projectinfo

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators.accessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.ints
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.{personEmails, personGitLabIds, personNames, projectIds, projectPaths}
import io.renku.graph.model.entities.Project.ProjectMember
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.AccessToken
import io.renku.interpreters.TestLogger
import io.renku.stubbing.ExternalServiceStubbing
import io.renku.testtools.IOSpec
import io.renku.tinytypes.json.TinyTypeEncoders
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.generators.ErrorGenerators.processingRecoverableErrors
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Random

class MemberEmailFinderSpec
    extends AnyWordSpec
    with IOSpec
    with ExternalServiceStubbing
    with should.Matchers
    with ScalaCheckPropertyChecks
    with MockFactory
    with TinyTypeEncoders {

  "findEmail" should {

    "iterate over project's push events " +
      "until commit info for the commit found on the push event lists an author with the same name" in new TestCase {
        val event  = pushEvents.generateOne.forMember(member).forProject(project)
        val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

        val returningVal = EitherT.fromEither[IO]((events, PagingInfo(None, None)).asRight[ProcessingRecoverableError])
        (projectEventsFinder
          .find(_: Project, _: Int)(_: Option[AccessToken]))
          .expects(project, 1, maybeAccessToken)
          .returning(returningVal)

        val authorEmail = personEmails.generateOne
        (commitAuthorFinder
          .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
          .expects(
            project.path,
            event.commitId,
            maybeAccessToken
          )
          .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

        finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
      }

    "do nothing if email is already set on the given member" in new TestCase {
      val memberWithEmail = member add personEmails.generateOne
      finder.findMemberEmail(memberWithEmail, project).value.unsafeRunSync() shouldBe memberWithEmail.asRight
    }

    "take the email from commitTo and skip commitFrom if both commitIds exist on the event" in new TestCase {
      val commit = commitIds.generateOne
      val event = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
        .copy(commitId = commit)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      val returningVal = EitherT.fromEither[IO]((events, PagingInfo(None, None)).asRight[ProcessingRecoverableError])

      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 1, maybeAccessToken)
        .returning(returningVal)

      val authorEmail = personEmails.generateOne
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(project.path, commit, maybeAccessToken)
        .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
    }

    "find the email if a matching commit exists on the second page" in new TestCase {
      val event       = pushEvents.generateOne.forMember(member).forProject(project)
      val eventsPage2 = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      val returnedFirstTime =
        EitherT.fromEither[IO](
          (pushEvents.generateNonEmptyList().toList, PagingInfo(Some(2), Some(2))).asRight[ProcessingRecoverableError]
        )

      val returnedSecondTime =
        EitherT.fromEither[IO]((eventsPage2, PagingInfo(None, None)).asRight[ProcessingRecoverableError])

      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 1, maybeAccessToken)
        .returning(returnedFirstTime)

      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 2, maybeAccessToken)
        .returning(returnedSecondTime)

      val authorEmail = personEmails.generateOne
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(
          project.path,
          event.commitId,
          maybeAccessToken
        )
        .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
    }

    /* checking all events is really inefficient
    do it selectively
    you'll get about 30 pages of results
     */
    "select 30 pages, at an interval (step), from the total number of pages (including the first and the last one) " +
      "if the total number of pages for the project is more than 30" +
      "to avoid overloading GitLab" in new TestCase {
        val maybeTotalPages @ Some(totalPages) = ints(min = 30 * 20 + 1, max = 1000000).generateSome

        val step = totalPages / 30 // if the totalPages is 3000, the step will be 100

        def setExpectationForFirstPage = {
          val firstPage        = 1
          val firstPageEvents  = pushEvents.generateNonEmptyList(minElements = 20, maxElements = 20).toList
          val firstPageResults = wrapResult(firstPageEvents, PagingInfo(2.some, maybeTotalPages))
          (projectEventsFinder
            .find(_: Project, _: Int)(_: Option[AccessToken]))
            .expects(project, firstPage, maybeAccessToken)
            .returning(firstPageResults)
        }

        def setExpectationForMiddlePages =
          step to (totalPages - 1, step) foreach { page => // gets every every value from 100, 200, 300 up to 2990
            val nextPageEvents  = pushEvents.generateNonEmptyList(minElements = 20, maxElements = 20).toList
            val nextPageResults = wrapResult(nextPageEvents, PagingInfo((page + 1).some, maybeTotalPages))
            (projectEventsFinder
              .find(_: Project, _: Int)(_: Option[AccessToken]))
              .expects(project, page, maybeAccessToken)
              .returning(nextPageResults)
          }

        val event = pushEvents.generateOne.forMember(member).forProject(project)

        def setExpectationForLastPage = {
          val nextPageEvents  = event :: pushEvents.generateNonEmptyList(minElements = 20, maxElements = 20).toList
          val nextPageResults = wrapResult(nextPageEvents, PagingInfo(None, maybeTotalPages))
          (projectEventsFinder
            .find(_: Project, _: Int)(_: Option[AccessToken]))
            .expects(project, totalPages, maybeAccessToken)
            .returning(nextPageResults)
        }

        val authorEmail = personEmails.generateOne

        def setExpectationForCommitAuthorFinder =
          (commitAuthorFinder
            .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
            .expects(
              project.path,
              event.commitId,
              maybeAccessToken
            )
            .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

        setExpectationForFirstPage
        setExpectationForMiddlePages
        setExpectationForLastPage
        setExpectationForCommitAuthorFinder

        finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
      }

    "find the email if a matching commits exist on both pages, " +
      "however, the first matching commit has author with a different name" in new TestCase {
        val eventPage1 = pushEvents.generateOne
          .forMember(member)
          .forProject(project)
        val eventsPage1 = Random.shuffle(eventPage1 :: pushEvents.generateNonEmptyList().toList)
        val eventPage2 = pushEvents.generateOne
          .forMember(member)
          .forProject(project)
        val eventsPage2 = Random.shuffle(eventPage2 :: pushEvents.generateNonEmptyList().toList)

        val firstEvent = pushEvents.generateOne.forMember(member).forProject(project)

        (projectEventsFinder
          .find(_: Project, _: Int)(_: Option[AccessToken]))
          .expects(project, 1, maybeAccessToken)
          .returning(wrapResult(eventsPage1, PagingInfo(Some(2), Some(2))))
        (projectEventsFinder
          .find(_: Project, _: Int)(_: Option[AccessToken]))
          .expects(project, 2, maybeAccessToken)
          .returning(wrapResult(eventsPage2, PagingInfo(None, Some(2))))

        (commitAuthorFinder
          .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
          .expects(
            project.path,
            eventPage1.commitId,
            maybeAccessToken
          )
          .returning(
            EitherT.rightT[IO, ProcessingRecoverableError]((personNames.generateOne -> personEmails.generateOne).some)
          )
        val authorEmail = personEmails.generateOne
        (commitAuthorFinder
          .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
          .expects(
            project.path,
            eventPage2.commitId,
            maybeAccessToken
          )
          .returning(EitherT.rightT[IO, ProcessingRecoverableError]((member.name -> authorEmail).some))

        finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe (member add authorEmail).asRight
      }

    "return the given member back if a matching commit author cannot be found on any events" in new TestCase {
      val eventPage1 = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
      val eventsPage1 = Random.shuffle(eventPage1 :: pushEvents.generateNonEmptyList().toList)
      val eventPage2 = pushEvents.generateOne
        .forMember(member)
        .forProject(project)
      val eventsPage2 = Random.shuffle(eventPage2 :: pushEvents.generateNonEmptyList().toList)

      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 1, maybeAccessToken)
        .returning(wrapResult(eventsPage1, PagingInfo(Some(2), Some(2))))
      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 2, maybeAccessToken)
        .returning(wrapResult(eventsPage2, PagingInfo(None, Some(2))))

      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(
          project.path,
          eventPage1.commitId,
          maybeAccessToken
        )
        .returning(
          EitherT.rightT[IO, ProcessingRecoverableError]((personNames.generateOne -> personEmails.generateOne).some)
        )
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(
          project.path,
          eventPage2.commitId,
          maybeAccessToken
        )
        .returning(
          EitherT.rightT[IO, ProcessingRecoverableError]((personNames.generateOne -> personEmails.generateOne).some)
        )

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe member.asRight
    }

    "return the given member back if no project events with the member as an author are found for the project" in new TestCase {

      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 1, maybeAccessToken)
        .returning(wrapResult(pushEvents.generateNonEmptyList().toList, PagingInfo(Some(2), Some(2))))
      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 2, maybeAccessToken)
        .returning(wrapResult(pushEvents.generateNonEmptyList().toList, PagingInfo(None, Some(2))))

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe member.asRight
    }

    "return the given member back if no project events are found" in new TestCase {
      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 1, maybeAccessToken)
        .returning(wrapResult(Nil, PagingInfo(None, None)))

      finder.findMemberEmail(member, project).value.unsafeRunSync() shouldBe member.asRight
    }

    s"return a Recoverable Failure when returned when fetching commit info" in new TestCase {
      val event  = pushEvents.generateOne.forMember(member).forProject(project)
      val events = Random.shuffle(event :: pushEvents.generateNonEmptyList().toList)

      (projectEventsFinder
        .find(_: Project, _: Int)(_: Option[AccessToken]))
        .expects(project, 1, maybeAccessToken)
        .returning(wrapResult(events, PagingInfo(None, None)))

      val error = processingRecoverableErrors.generateOne
      (commitAuthorFinder
        .findCommitAuthor(_: projects.Path, _: CommitId)(_: Option[AccessToken]))
        .expects(
          project.path,
          event.commitId,
          maybeAccessToken
        )
        .returning(EitherT.leftT[IO, Option[(persons.Name, persons.Email)]](error))

      val Left(failure) = finder.findMemberEmail(member, project).value.unsafeRunSync()
      failure shouldBe a[ProcessingRecoverableError]
    }
  }

  private trait TestCase {
    implicit val maybeAccessToken: Option[AccessToken] = accessTokens.generateOption
    val project = Project(projectIds.generateOne, projectPaths.generateOne)
    val member  = projectMembersNoEmail.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val commitAuthorFinder  = mock[CommitAuthorFinder[IO]]
    val projectEventsFinder = mock[ProjectEventsFinder[IO]]
    val finder              = new MemberEmailFinderImpl[IO](commitAuthorFinder, projectEventsFinder)

    def wrapResult(events: List[PushEvent], pagingInfo: PagingInfo) =
      EitherT.fromEither[IO]((events, pagingInfo).asRight[ProcessingRecoverableError])
  }

  private implicit class PushEventOps(event: PushEvent) {
    def forMember(member: ProjectMember): PushEvent =
      event.copy(authorId = member.gitLabId, authorName = member.name)

    def forProject(project: Project): PushEvent = event.copy(projectId = project.id)
  }

  private lazy val pushEvents: Gen[PushEvent] = for {
    projectId <- projectIds
    commitId  <- commitIds
    userId    <- personGitLabIds
    userName  <- personNames
  } yield PushEvent(projectId, commitId, userId, userName)
}
