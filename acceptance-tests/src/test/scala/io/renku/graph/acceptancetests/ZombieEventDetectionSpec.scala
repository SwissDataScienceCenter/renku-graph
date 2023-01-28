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

package io.renku.graph.acceptancetests

import cats.effect.IO
import cats.syntax.all._
import data._
import db.EventLog
import flows.AccessTokenPresence
import io.circe.literal._
import io.renku.eventlog._
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.EventsGenerators.commitIds
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import io.renku.graph.model.projects._
import io.renku.graph.model.testentities.cliShapedPersons
import io.renku.graph.model.testentities.generators.EntitiesGenerators._
import io.renku.microservices.MicroserviceIdentifier
import org.scalacheck.Gen
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Minutes, Seconds, Span}
import skunk.data.Completion
import skunk.implicits._
import skunk.{Command, Session, ~}
import testing.AcceptanceTestPatience
import tooling.{AcceptanceSpec, ApplicationServices, ModelImplicits}

import java.lang.Thread.sleep
import java.time.Instant
import scala.concurrent.duration._

class ZombieEventDetectionSpec
    extends AcceptanceSpec
    with ApplicationServices
    with Eventually
    with ModelImplicits
    with AccessTokenPresence
    with TypeSerializers
    with AcceptanceTestPatience {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(3, Minutes)),
    interval = scaled(Span(10, Seconds))
  )

  Scenario(
    s"An event which got stuck in either $GeneratingTriples or $TransformingTriples status " +
      s"should be detected and re-processes"
  ) {
    val user = authUsers.generateOne
    val project = dataProjects(
      renkuProjectEntities(visibilityPublic, creatorGen = cliShapedPersons).modify(removeMembers())
    ).map(addMemberWithId(user.id)).generateOne

    val commitId  = commitIds.generateOne
    Given("Triples generation is successful")
    `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project, commitId)

    And("a project with a commit exists in GitLab")
    gitLabStub.addAuthenticated(user)
    gitLabStub.setupProject(project, commitId)

    And("an access token is present")
    givenAccessTokenPresentFor(project, user.accessToken)

    And("an event that should be classified as zombie is in the EventLog DB")
    val eventDate = eventDates.generateOne
    insertProjectToDB(project, eventDate) shouldBe 1
    EventLog.execute { implicit session =>
      insertEventToDB(commitId, project, eventDate) >> insertEventDeliveryToDB(commitId, project)
    }
    sleep((5 seconds).toMillis)

    Then("the zombie chasing functionality should re-do the event")
    eventually {
      EventLog.findEvents(project.id, status = GeneratingTriples).isEmpty shouldBe true
      EventLog.findEvents(project.id, status = TriplesStore)              shouldBe List(commitId)
    }
  }

  private def insertProjectToDB(project: data.Project, eventDate: EventDate): Int = EventLog.execute { session =>
    val query: Command[GitLabId ~ Path ~ EventDate] =
      sql"""INSERT INTO project (project_id, project_path, latest_event_date)
          VALUES ($projectIdEncoder, $projectPathEncoder, $eventDateEncoder)
          ON CONFLICT (project_id)
          DO UPDATE SET latest_event_date = excluded.latest_event_date WHERE excluded.latest_event_date > project.latest_event_date
          """.command
    session.prepare(query).flatMap(_.execute(project.id ~ project.path ~ eventDate)).flatMap {
      case Completion.Insert(n) => n.pure[IO]
      case completion =>
        new RuntimeException(s"insertProjectToDB failed with completion code $completion")
          .raiseError[IO, Int]
    }
  }

  private def insertEventToDB(
      commitId:  CommitId,
      project:   data.Project,
      eventDate: EventDate
  )(implicit session: Session[IO]) = {
    val query
        : Command[EventId ~ GitLabId ~ EventStatus ~ CreatedDate ~ ExecutionDate ~ EventDate ~ BatchDate ~ EventBody] =
      sql"""
          INSERT INTO event (event_id, project_id, status, created_date, execution_date, event_date, batch_date, event_body)
          VALUES ($eventIdEncoder, $projectIdEncoder, $eventStatusEncoder, $createdDateEncoder,
          $executionDateEncoder,
          $eventDateEncoder,
          $batchDateEncoder,
          $eventBodyEncoder)
          """.command
    session
      .prepare(query)
      .flatMap(
        _.execute(
          EventId(commitId.value) ~ project.id ~ GeneratingTriples ~ CreatedDate(eventDate.value) ~ ExecutionDate(
            Instant.now.minusSeconds(60 * 6)
          ) ~ eventDate ~ BatchDate(eventDate.value) ~ EventBody(json"""{
                      "id": ${commitId.value},
                      "project": {
                        "id":   ${project.id.value},
                        "path": ${project.path.value}
                      },
                      "parents": []
                    }""".noSpaces)
        )
      )
  }

  private def insertEventDeliveryToDB(commitId: CommitId, project: data.Project)(implicit session: Session[IO]) = {
    val query: Command[EventId ~ GitLabId ~ MicroserviceIdentifier] = sql"""
          INSERT INTO event_delivery (event_id, project_id, delivery_id)
          VALUES ($eventIdEncoder, $projectIdEncoder, $microserviceIdentifierEncoder)
          """.command
    session.prepare(query).flatMap(_.execute(EventId(commitId.value) ~ project.id ~ MicroserviceIdentifier.generate))
  }

  private implicit lazy val eventDates: Gen[EventDate] = timestampsNotInTheFuture map EventDate.apply
}
