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

import cats.effect.{ContextShift, IO}
import ch.datascience.commiteventservice.events.categories.commitsync.Generators._
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.CommitEventSynchronizer.UpdateResult.{Failed, Updated}
import ch.datascience.commiteventservice.events.categories.commitsync.eventgeneration.historytraversal.CommitToEventLog
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.global

class MissedEventsGeneratorSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "loadMissedEvents" should {

    "add missing events to the Event Log " +
      "when the latest eventId differs from the latest commit in GitLab" in new TestCase {
        val commitSyncEvent = fullCommitSyncEvents.generateOne

        givenStoring(
          StartCommit(id = commitSyncEvent.id,
                      project = Project(commitSyncEvent.project.id, commitSyncEvent.project.path)
          )
        ).returning(IO.unit)

        eventsGenerator
          .generateMissedEvents(commitSyncEvent.project, commitSyncEvent.id)
          .unsafeRunSync() shouldBe Updated

      }

    "not break processing if storing start Commit for one of the events fails" in new TestCase {
      val commitSyncEvent = fullCommitSyncEvents.generateOne

      val exception = exceptions.generateOne
      givenStoring(
        StartCommit(id = commitSyncEvent.id,
                    project = Project(commitSyncEvent.project.id, commitSyncEvent.project.path)
        )
      ).returning(IO.raiseError(exception))

      eventsGenerator.generateMissedEvents(commitSyncEvent.project, commitSyncEvent.id).unsafeRunSync() shouldBe Failed(
        "event generation failed",
        exception
      )

    }
  }

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {

    val commitToEventLog = mock[CommitToEventLog[IO]]
    val eventsGenerator  = new MissedEventsGeneratorImpl[IO](commitToEventLog)

    def givenStoring(pushEvent: StartCommit) =
      (commitToEventLog
        .storeCommitsInEventLog(_: StartCommit))
        .expects(pushEvent)
  }
}
