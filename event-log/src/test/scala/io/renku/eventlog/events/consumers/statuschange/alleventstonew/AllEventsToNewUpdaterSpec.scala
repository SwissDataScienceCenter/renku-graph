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

package io.renku.eventlog.events.consumers.statuschange
package alleventstonew

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.circe.literal._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.api.events.StatusChangeEvent.AllEventsToNew
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, categoryName}
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.util.Random

class AllEventsToNewUpdaterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  "updateDB" should {

    "send ProjectEventsToNew events for all the projects" in testDBResource.use { implicit cfg =>
      val projects = consumerProjects.generateNonEmptyList().toList

      for {
        _ <- projects.traverse_ { project =>
               if (Random.nextBoolean()) addEvent(project)
               else upsertProject(project)
             }

        _ = projects foreach { givenEventSending(_, returning = IO.unit) }

        _ <- moduleSessionResource.session
               .useKleisli(dbUpdater updateDB AllEventsToNew)
               .asserting(_ shouldBe DBUpdateResults.empty)
      } yield Succeeded
    }

    "do not send any events if there are no projects in the DB" in testDBResource.use { implicit cfg =>
      moduleSessionResource.session
        .useKleisli(dbUpdater updateDB AllEventsToNew)
        .asserting(_ shouldBe DBUpdateResults.empty)
    }
  }

  private val eventSender = mock[EventSender[IO]]
  val dbUpdater = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new DbUpdater[IO](eventSender)
  }

  private def givenEventSending(project: Project, returning: IO[Unit]) =
    (eventSender
      .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
      .expects(
        EventRequestContent.NoPayload(toEventJson(project)),
        EventSender.EventContext(CategoryName(projecteventstonew.eventType.show),
                                 show"$categoryName: generating ${projecteventstonew.eventType} for $project failed"
        )
      )
      .returning(returning)

  private def addEvent(project: Project)(implicit cfg: DBConfig[EventLogDB]) =
    storeGeneratedEvent(eventStatuses.generateOne, project = project)

  private def toEventJson(project: Project) = json"""{
    "categoryName": "EVENTS_STATUS_CHANGE",
    "project": {
      "id":   ${project.id},
      "slug": ${project.slug}
    },
    "subCategory": "ProjectEventsToNew"
  }"""
}
