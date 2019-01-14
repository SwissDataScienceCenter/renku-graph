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

package ch.datascience.webhookservice.eventprocessing.pushevent

import java.time.Instant

import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.events.{CommitEvent, CommitMessage, CommittedDate, User}
import ch.datascience.webhookservice.eventprocessing.PushEvent
import ch.datascience.webhookservice.generators.ServiceTypesGenerators.pushEvents
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{Success, Try}

class CommitEventsFinderSpec extends WordSpec {

  "findCommitEvents" should {

    "return commit event for the given push event" in new TestCase {
      commitEventFinder.findCommitEvents(pushEvent) shouldBe Success(commitEventFrom(pushEvent))
    }
  }

  private trait TestCase {
    val pushEvent = pushEvents.generateOne

    val commitEventFinder = new CommitEventsFinder[Try]()
  }

  private def commitEventFrom(pushEvent: PushEvent) = CommitEvent(
    id            = pushEvent.after,
    message       = CommitMessage("abc"),
    committedDate = CommittedDate(Instant.EPOCH),
    pushUser      = pushEvent.pushUser,
    author        = User(pushEvent.pushUser.username, pushEvent.pushUser.email),
    committer     = User(pushEvent.pushUser.username, pushEvent.pushUser.email),
    parents       = Seq(),
    project       = pushEvent.project
  )
}
