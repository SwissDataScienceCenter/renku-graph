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

package io.renku.eventlog.statuschange

import cats.effect.IO
import ch.datascience.db.DbTransactor
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{compoundEventIds, eventBodies}
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.events.CompoundEventId
import ch.datascience.graph.model.projects
import ch.datascience.metrics.LabeledGauge
import eu.timepit.refined.auto._
import io.renku.eventlog.DbEventLogGenerators.{eventDates, executionDates}
import io.renku.eventlog.EventStatus.{New, Processing}
import io.renku.eventlog.statuschange.commands.UpdateResult.Updated
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, UpdateResult}
import io.renku.eventlog.{EventLogDB, EventStatus, InMemoryEventLogDbSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.language.higherKinds

class StatusUpdatesRunnerSpec extends WordSpec with InMemoryEventLogDbSpec with MockFactory {

  "run" should {

    "execute query from the given command, " +
      "map the result using command's result mapping rules " +
      "and update metrics gauges" in new TestCase {

      store(eventId, projectPath, New)

      (gauge.increment _).expects(projectPath).returning(IO.unit)

      runner.run(TestCommand(eventId, projectPath, gauge)).unsafeRunSync() shouldBe Updated

      findEvents(status = Processing).map(_._1) shouldBe List(eventId)
    }
  }

  private trait TestCase {
    val eventId     = compoundEventIds.generateOne
    val projectPath = projectPaths.generateOne

    val gauge  = mock[LabeledGauge[IO, projects.Path]]
    val runner = new StatusUpdatesRunnerImpl[IO](transactor)
  }

  private case class TestCommand(eventId:     CompoundEventId,
                                 projectPath: projects.Path,
                                 gauge:       LabeledGauge[IO, projects.Path])
      extends ChangeStatusCommand[IO] {
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

    override def updateGauges(
        updateResult:      UpdateResult
    )(implicit transactor: DbTransactor[IO, EventLogDB]) = gauge increment projectPath
  }

  private def store(eventId: CompoundEventId, projectPath: projects.Path, status: EventStatus): Unit =
    storeEvent(eventId,
               status,
               executionDates.generateOne,
               eventDates.generateOne,
               eventBodies.generateOne,
               projectPath = projectPath)
}
