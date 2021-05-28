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

package ch.datascience.commiteventservice.events.categories.commitsync
package eventgeneration
package historytraversal

import cats.MonadError
import cats.syntax.all._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEvent._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.Generators._
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.events._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.NonNegative
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import scala.util._

class CommitToEventLogSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "storeCommitsInEventLog" should {

    "transform the Start Commit into commit events and store them in the Event Log if they do not exist yet" in new TestCase {

      val commitEvent = commitEventFrom(startCommit, minParents = 0).generateOne
      val doesExist   = Random.nextBoolean()

      (commitInfoFinder.findCommitInfo _)
        .expects(startCommit.project.id, startCommit.id, maybeAccessToken)
        .returning(commitEvent.toCommitInfo.pure[Try])

      val eventCreationResults = if (doesExist) {
        (eventDetailsFinder.checkIfExists _)
          .expects(commitEvent.project.id, commitEvent.id)
          .returning(doesExist.pure[Try])
        Existed
      } else {
        (eventDetailsFinder.checkIfExists _)
          .expects(commitEvent.project.id, commitEvent.id)
          .returning(doesExist.pure[Try])
        (commitEventSender
          .send(_: CommitEvent))
          .expects(commitEvent)
          .returning(().pure[Try])
        Created

      }
      commitToEventLog.storeCommitsInEventLog(startCommit, maybeAccessToken) shouldBe Success(eventCreationResults)
    }

    "transform the Start Commit into a SkippedCommitEvent if the commit event message is 'renku migrate'" in new TestCase {

      val commitMessage = CommitMessage(sentenceContaining("renku migrate").generateOne)
      val commitEvent =
        skippedCommitEventFrom(startCommit.id, startCommit.project, withMessage = commitMessage).generateOne

      (commitInfoFinder.findCommitInfo _)
        .expects(startCommit.project.id, startCommit.id, maybeAccessToken)
        .returning(commitEvent.toCommitInfo.pure[Try])

      (eventDetailsFinder.checkIfExists _)
        .expects(commitEvent.project.id, commitEvent.id)
        .returning(false.pure[Try])

      (commitEventSender
        .send(_: CommitEvent))
        .expects(commitEvent)
        .returning(().pure[Try])

      commitToEventLog.storeCommitsInEventLog(startCommit, maybeAccessToken) shouldBe Success(Created)
    }

    "skip the event if it has the commit id  0000000000000000000000000000000000000000 ref " in new TestCase {
      val skippedCommit = startCommits.generateOne.copy(id = CommitId("0000000000000000000000000000000000000000"))

      commitToEventLog.storeCommitsInEventLog(skippedCommit, maybeAccessToken) shouldBe Success(Skipped)
    }

    "fail if finding commit info fails" in new TestCase {
      val exception = exceptions.generateOne

      (commitInfoFinder.findCommitInfo _)
        .expects(startCommit.project.id, startCommit.id, maybeAccessToken)
        .returning(context raiseError exception)

      commitToEventLog.storeCommitsInEventLog(startCommit, maybeAccessToken) shouldBe Failure(exception)

      logger.loggedOnly(Error(failedEventFinding(startCommit), exception))
    }

    "return a Failed status if check if the event exists fails" in new TestCase {

      val commitEvent = commitEventFrom(startCommit, minParents = 0).generateOne

      (commitInfoFinder.findCommitInfo _)
        .expects(startCommit.project.id, startCommit.id, maybeAccessToken)
        .returning(commitEvent.toCommitInfo.pure[Try])

      val exception = exceptions.generateOne

      (eventDetailsFinder.checkIfExists _)
        .expects(commitEvent.project.id, commitEvent.id)
        .returning(exception.raiseError[Try, Boolean])

      commitToEventLog.storeCommitsInEventLog(startCommit, maybeAccessToken) shouldBe
        Failed(failedCheckIfExists(startCommit, commitEvent), exception).pure[Try]
    }

    "return a Failed status if sending the event fails" in new TestCase {

      val commitEvent = commitEventFrom(startCommit, minParents = 0).generateOne

      (commitInfoFinder.findCommitInfo _)
        .expects(startCommit.project.id, startCommit.id, maybeAccessToken)
        .returning(commitEvent.toCommitInfo.pure[Try])

      (eventDetailsFinder.checkIfExists _)
        .expects(commitEvent.project.id, commitEvent.id)
        .returning(false.pure[Try])

      val exception = exceptions.generateOne
      (commitEventSender
        .send(_: CommitEvent))
        .expects(commitEvent)
        .returning(exception.raiseError[Try, Unit])

      commitToEventLog.storeCommitsInEventLog(startCommit, maybeAccessToken) shouldBe
        Failed(failedStoring(startCommit, commitEvent), exception).pure[Try]
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val maybeAccessToken = Gen.option(accessTokens).generateOne

    val startCommit = startCommits.generateOne
    val batchDate   = BatchDate(Instant.now)
    val clock       = Clock.fixed(batchDate.value, ZoneId.of(ZoneOffset.UTC.getId))

    val commitEventSender  = mock[CommitEventSender[Try]]
    val eventDetailsFinder = mock[EventDetailsFinder[Try]]
    val commitInfoFinder   = mock[CommitInfoFinder[Try]]
    val logger             = TestLogger[Try]()

    val commitToEventLog = new CommitToEventLogImpl[Try](
      commitEventSender,
      eventDetailsFinder,
      commitInfoFinder,
      logger,
      clock
    )

    def failedCheckIfExists(startCommit: StartCommit, commitEvent: CommitEvent): String =
      s"$categoryName: id = ${startCommit.id}, addedId = ${commitEvent.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> " +
        "checking if event exists in the event log failed"

    def failedStoring(startCommit: StartCommit, commitEvent: CommitEvent): String =
      s"$categoryName: id = ${startCommit.id}, addedId = ${commitEvent.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> " +
        "storing in the event log failed"

    def failedEventFinding(startCommit: StartCommit): String =
      s"$categoryName: id = ${startCommit.id}, projectId = ${startCommit.project.id}, projectPath = ${startCommit.project.path} -> " +
        "finding commit events failed"

    def commitEventFrom(startCommit: StartCommit, minParents: Int Refined NonNegative): Gen[CommitEvent] =
      commitEventFrom(
        startCommit.id,
        startCommit.project,
        minParents
      )

    def commitEventFrom(commitId:   CommitId,
                        project:    Project,
                        minParents: Int Refined NonNegative = 0
    ): Gen[CommitEvent] = for {
      message       <- commitMessages
      committedDate <- committedDates
      author        <- authors
      committer     <- committers
      parentsIds    <- listOf(commitIds, minParents)
    } yield NewCommitEvent(
      id = commitId,
      message = message,
      committedDate = committedDate,
      author = author,
      committer = committer,
      parents = parentsIds,
      project = project,
      batchDate = batchDate
    )

    def skippedCommitEventFrom(commitId:    CommitId,
                               project:     Project,
                               withMessage: CommitMessage = commitMessages.generateOne,
                               minParents:  Int Refined NonNegative = 0
    ): Gen[CommitEvent] = for {
      committedDate <- committedDates
      author        <- authors
      committer     <- committers
      parentsIds    <- listOf(commitIds, minParents)
    } yield SkippedCommitEvent(
      id = commitId,
      message = withMessage,
      committedDate = committedDate,
      author = author,
      committer = committer,
      parents = parentsIds,
      project = project,
      batchDate = batchDate
    )

    implicit class CommitEventOps(commitEvent: CommitEvent) {
      lazy val toCommitInfo = CommitInfo(commitEvent.id,
                                         commitEvent.message,
                                         commitEvent.committedDate,
                                         commitEvent.author,
                                         commitEvent.committer,
                                         commitEvent.parents
      )
    }
  }
}
