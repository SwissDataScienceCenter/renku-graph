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

package io.renku.eventlog.events.consumers.statuschange.projecteventstonew

import cats.Show
import cats.data.Kleisli
import cats.effect.IO
import io.circe.Encoder
import io.renku.eventlog.EventLogDB
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.api.events.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.api.events.StatusChangeGenerators
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEventsQueue.EventType
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, DBUpdater, StatusChangeEventsQueue}
import io.renku.generators.Generators.Implicits._
import io.renku.testtools.CustomAsyncIOSpec
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk.Session

class DbUpdaterSpec extends AsyncWordSpec with CustomAsyncIOSpec with should.Matchers with AsyncMockFactory {

  "updateDB" should {

    "offer the ProjectEventsToNew event to the queue" in {
      (queue
        .offer(_: ProjectEventsToNew)(_: Encoder[ProjectEventsToNew],
                                      _: EventType[ProjectEventsToNew],
                                      _: Show[ProjectEventsToNew]
        ))
        .expects(event, *, *, *)
        .returning(Kleisli.pure(()))

      handler.updateDB(event)(session).asserting(_ shouldBe DBUpdateResults.ForProjects.empty)
    }
  }

  "onRollback" should {
    "return RollbackOp.empty" in {
      implicit val sr: SessionResource[IO] = mock[io.renku.db.SessionResource[IO, EventLogDB]]
      handler.onRollback(event)(sr) shouldBe DBUpdater.RollbackOp.empty[IO]
    }
  }

  private lazy val event   = StatusChangeGenerators.projectEventsToNewEvents.generateOne
  private lazy val session = mock[Session[IO]]

  private lazy val queue   = mock[StatusChangeEventsQueue[IO]]
  private lazy val handler = new DbUpdater[IO](queue)
}
