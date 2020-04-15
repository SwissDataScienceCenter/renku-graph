/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog.statuschange

import ch.datascience.dbeventlog.DbEventLogGenerators.{eventDates, executionDates}
import ch.datascience.dbeventlog.EventStatus
import ch.datascience.dbeventlog.EventStatus.{New, Processing}
import ch.datascience.dbeventlog.commands.InMemoryEventLogDbSpec
import ch.datascience.dbeventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import ch.datascience.graph.model.events.CompoundEventId
import eu.timepit.refined.auto._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class UpdateCommandsRunnerSpec extends WordSpec with InMemoryEventLogDbSpec {

  "run" should {

    "execute query from the given command " +
      "and map the result using command's result mapping rules" in new TestCase {

      store(eventId, New)

      runner.run(TestCommand(eventId)).unsafeRunSync() shouldBe UpdateResult.Updated

      findEvents(status = Processing).map(_._1) shouldBe List(eventId)
    }
  }

  private trait TestCase {
    val eventId = compoundEventIds.generateOne

    val runner = new UpdateCommandsRunner(transactor)
  }

  private case class TestCommand(eventId: CompoundEventId) extends ChangeStatusCommand {
    import doobie.implicits._

    override val status: EventStatus = Processing

    override def query =
      sql"""|update event_log 
            |set status = $status
            |where event_id = ${eventId.id} and project_id = ${eventId.projectId} and status = ${New: EventStatus}
            |""".stripMargin

    override def mapResult: Int => UpdateResult = {
      case 0 => UpdateResult.Conflict
      case 1 => UpdateResult.Updated
      case _ => UpdateResult.Failure("error message")
    }
  }

  private def store(eventId: CompoundEventId, status: EventStatus): Unit =
    storeEvent(eventId, status, executionDates.generateOne, eventDates.generateOne, eventBodies.generateOne)
}
