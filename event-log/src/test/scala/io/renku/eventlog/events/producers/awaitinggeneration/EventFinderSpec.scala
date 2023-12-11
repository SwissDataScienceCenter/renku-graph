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

package io.renku.eventlog.events.producers
package awaitinggeneration

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.events.producers.ProjectPrioritisation.Priority.MaxPriority
import io.renku.eventlog.events.producers.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.eventlog.metrics.TestEventStatusGauges._
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes, TestEventStatusGauges, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{OptionValues, Succeeded}

import java.time.Instant
import java.time.temporal.ChronoUnit.DAYS

private class EventFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues {

  it should s"find the most recent event in status $New or $GenerationRecoverableFailure " +
    s"and mark it as $GeneratingTriples" in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne
      for {
        notLatestEvent <- createEvent(
                            status = Gen.oneOf(New, GenerationRecoverableFailure),
                            eventDate = timestamps(max = now.minus(2, DAYS)).generateAs(EventDate),
                            project = project
                          )
        event2 <- createEvent(
                    status = Gen.oneOf(New, GenerationRecoverableFailure),
                    eventDate = timestamps(min = notLatestEvent.date.value, max = now).generateAs(EventDate),
                    project = project
                  )

        _ <- findEvents(GeneratingTriples).asserting(_ shouldBe List.empty)

        _ = givenPrioritisation(
              takes = List(ProjectInfo(project, event2.date, currentOccupancy = 0)),
              totalOccupancy = 0,
              returns = List(ProjectIds(project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting(_ shouldBe event2.toAwaitingEvent)

        _ <- gauges.awaitingGeneration.getValue(project.slug).asserting(_ shouldBe -1)
        _ <- gauges.underGeneration.getValue(project.slug).asserting(_ shouldBe 1)

        _ <- findEvents(GeneratingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(FoundEvent(event2.id, executionDate))
             }

        _ = givenPrioritisation(takes = Nil, totalOccupancy = 1, returns = Nil)

        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should s"find the most recent event in status $New or $GenerationRecoverableFailure " +
    "if there are multiple latest events with the same date" in testDBResource.use { implicit cfg =>
      val project         = consumerProjects.generateOne
      val latestEventDate = eventDates.generateOne
      for {
        event1 <- createEvent(
                    status = Gen.oneOf(New, GenerationRecoverableFailure),
                    eventDate = latestEventDate,
                    project = project
                  )
        event2 <- createEvent(
                    status = Gen.oneOf(New, GenerationRecoverableFailure),
                    eventDate = latestEventDate,
                    project = project
                  )

        _ <- findEvents(GeneratingTriples).asserting(_ shouldBe List.empty)

        // 1st event with the same event date
        _ = givenPrioritisation(
              takes = List(ProjectInfo(project, latestEventDate, currentOccupancy = 0)),
              totalOccupancy = 0,
              returns = List(ProjectIds(project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting {
               _ should { be(event1.toAwaitingEvent) or be(event2.toAwaitingEvent) }
             }

        _ <- gauges.awaitingGeneration.getValue(project.slug).asserting(_ shouldBe -1)
        _ <- gauges.underGeneration.getValue(project.slug).asserting(_ shouldBe 1)

        _ <- findEvents(GeneratingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) should {
                 be(List(FoundEvent(event1.id, executionDate))) or be(List(FoundEvent(event2.id, executionDate)))
               }
             }

        // 2nd event with the same event date
        _ = givenPrioritisation(
              takes = List(ProjectInfo(project, latestEventDate, currentOccupancy = 1)),
              totalOccupancy = 1,
              returns = List(ProjectIds(project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting {
               _ should { be(event1.toAwaitingEvent) or be(event2.toAwaitingEvent) }
             }

        _ <- gauges.awaitingGeneration.getValue(project.slug).asserting(_ shouldBe -2)
        _ <- gauges.underGeneration.getValue(project.slug).asserting(_ shouldBe 2)

        _ <- findEvents(GeneratingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) should contain theSameElementsAs
                 List(FoundEvent(event1.id, executionDate), FoundEvent(event2.id, executionDate))
             }

        // no more events left
        _ = givenPrioritisation(takes = Nil, totalOccupancy = 2, returns = Nil)

        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should s"find the most recent event in status $New or $GenerationRecoverableFailure " +
    "if the latest event is status GenerationNonRecoverableFailure" in testDBResource.use { implicit cfg =>
      `the most recent dated event should be found`(GenerationNonRecoverableFailure)
    }
  it should s"find the most recent event in status $New or $GenerationRecoverableFailure " +
    "if the latest event is status TransformationNonRecoverableFailure" in testDBResource.use { implicit cfg =>
      `the most recent dated event should be found`(TransformationNonRecoverableFailure)
    }
  it should s"find the most recent event in status $New or $GenerationRecoverableFailure " +
    "if the latest event is status Skipped" in testDBResource.use { implicit cfg =>
      `the most recent dated event should be found`(Skipped)
    }

  private def `the most recent dated event should be found`(latestEventStatus: EventStatus)(implicit
      cfg: DBConfig[EventLogDB]
  ) = {
    val project = consumerProjects.generateOne
    for {
      event1 <- createEvent(
                  status = Gen.oneOf(New, GenerationRecoverableFailure),
                  eventDate = timestamps(max = now.minus(2, DAYS)).generateAs(EventDate),
                  project = project
                )
      latestDateEvent <- createEvent(
                           status = latestEventStatus,
                           eventDate = timestamps(min = event1.date.value, max = now).generateAs(EventDate),
                           project = project
                         )

      _ <- findEvents(GeneratingTriples).asserting(_ shouldBe List.empty)
      _ = givenPrioritisation(
            takes = List(ProjectInfo(project, latestDateEvent.date, currentOccupancy = 0)),
            totalOccupancy = 0,
            returns = List(ProjectIds(project) -> MaxPriority)
          )

      _ <- finder.popEvent().map(_.value).asserting(_ shouldBe event1.toAwaitingEvent)

      _ <- gauges.awaitingGeneration.getValue(project.slug).asserting(_ shouldBe -1)
      _ <- gauges.underGeneration.getValue(project.slug).asserting(_ shouldBe 1)
    } yield Succeeded
  }

  it should s"find no event when there are older statuses in status $New or $GenerationRecoverableFailure " +
    "but the latest event is GeneratingTriples" in testDBResource.use { implicit cfg =>
      `not find an event in New status if the latest is in certain status`(GeneratingTriples)
    }
  it should s"find no event when there are older statuses in status $New or $GenerationRecoverableFailure " +
    "but the latest event is TriplesGenerated" in testDBResource.use { implicit cfg =>
      `not find an event in New status if the latest is in certain status`(TriplesGenerated)
    }
  it should s"find no event when there are older statuses in status $New or $GenerationRecoverableFailure " +
    "but the latest event is TransformationRecoverableFailure" in testDBResource.use { implicit cfg =>
      `not find an event in New status if the latest is in certain status`(TransformationRecoverableFailure)
    }
  it should s"find no event when there are older statuses in status $New or $GenerationRecoverableFailure " +
    "but the latest event is TriplesStore" in testDBResource.use { implicit cfg =>
      `not find an event in New status if the latest is in certain status`(TriplesStore)
    }
  it should s"find no event when there are older statuses in status $New or $GenerationRecoverableFailure " +
    "but the latest event is AwaitingDeletion" in testDBResource.use { implicit cfg =>
      `not find an event in New status if the latest is in certain status`(AwaitingDeletion)
    }
  it should s"find no event when there are older statuses in status $New or $GenerationRecoverableFailure " +
    "but the latest event is Deleting" in testDBResource.use { implicit cfg =>
      `not find an event in New status if the latest is in certain status`(Deleting)
    }

  private def `not find an event in New status if the latest is in certain status`(
      latestEventStatus: EventStatus
  )(implicit cfg: DBConfig[EventLogDB]) = {
    val project = consumerProjects.generateOne
    for {
      notLatestDateEvent <- createEvent(
                              status = Gen.oneOf(New, GenerationRecoverableFailure),
                              eventDate = timestamps(max = now.minus(2, DAYS)).generateAs(EventDate),
                              project = project
                            )
      _ <- createEvent(
             status = latestEventStatus,
             eventDate = timestamps(min = notLatestDateEvent.date.value, max = now).generateAs(EventDate),
             project = project
           )

      _ = givenPrioritisation(takes = Nil,
                              totalOccupancy = if (latestEventStatus == GeneratingTriples) 1 else 0,
                              returns = Nil
          )

      _ <- finder.popEvent().asserting(_ shouldBe None)
    } yield Succeeded
  }

  it should "find no event when execution date is in the future " +
    s"and status $New or $GenerationRecoverableFailure " in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne
      for {
        event1 <- createEvent(
                    status = New,
                    eventDate = timestamps(max = Instant.now().minusSeconds(5)).generateAs(EventDate),
                    project = project
                  )
        event2 <- createEvent(
                    status = GenerationRecoverableFailure,
                    eventDate = EventDate(event1.date.value plusSeconds 1),
                    executionDate = timestampsInTheFuture.generateAs(ExecutionDate),
                    project = project
                  )
        _ <- createEvent(
               status = New,
               eventDate = EventDate(event2.date.value plusSeconds 1),
               executionDate = timestampsInTheFuture.generateAs(ExecutionDate),
               project = project
             )

        _ <- findEvents(GeneratingTriples).asserting(_ shouldBe List.empty)

        _ = givenPrioritisation(takes = Nil, totalOccupancy = 0, returns = Nil)

        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "find the latest events from each project" in testDBResource.use { implicit cfg =>
    for {
      events <- readyStatuses.generateNonEmptyList(min = 2).map(createEvent(_)).toList.sequence

      _ <- findEvents(GeneratingTriples).asserting(_ shouldBe List.empty)

      _ = events.sortBy(_.date).reverse.zipWithIndex foreach { case (event, index) =>
            givenPrioritisation(
              takes = List(ProjectInfo(event.project, event.date, 0)),
              totalOccupancy = index,
              returns = List(ProjectIds(event.project) -> MaxPriority)
            )
          }

      _ <- events.traverse_ { _ =>
             finder.popEvent().asserting(_ shouldBe a[Some[_]])
           }

      _ <- events.traverse_ { event =>
             gauges.awaitingGeneration.getValue(event.project.slug).asserting(_ shouldBe -1) >>
               gauges.underGeneration.getValue(event.project.slug).asserting(_ shouldBe 1)
           }

      _ <- findEvents(GeneratingTriples).asserting {
             _.map(_.id) should contain theSameElementsAs events.map(_.id)
           }
    } yield Succeeded
  }

  it should "return the latest events from each projects - case with projectsFetchingLimit > 1" in testDBResource.use {
    implicit cfg =>
      val finder =
        new EventFinderImpl[IO](() => now, projectsFetchingLimit = 5, projectPrioritisation = projectPrioritisation)

      for {
        events <- readyStatuses
                    .generateNonEmptyList(min = 3, max = 6)
                    .toList
                    .flatMap { status =>
                      (1 to positiveInts(max = 2).generateOne.value)
                        .map(_ => createEvent(status, project = consumerProjects.generateOne))
                    }
                    .sequence

        _ <- findEvents(GeneratingTriples).asserting(_ shouldBe List.empty)

        eventsPerProject = events.groupBy(_.project).toList.sortBy(_._2.maxBy(_.date).date).reverse
        _ = eventsPerProject.zipWithIndex foreach { case ((_, events), index) =>
              val theNewestEvent = events.sortBy(_.date).reverse.head
              (projectPrioritisation.prioritise _)
                .expects(*, index)
                .returning(List(ProjectIds(theNewestEvent.project) -> MaxPriority))
            }

        _ <- eventsPerProject.map(_ => finder.popEvent().asserting(_ shouldBe a[Some[_]])).sequence

        _ <- eventsPerProject.map { case (project, events) =>
               gauges.awaitingGeneration.getValue(project.slug).asserting(_ shouldBe -events.size) >>
                 gauges.underGeneration.getValue(project.slug).asserting(_ shouldBe events.size)
             }.sequence

        _ <- findEvents(GeneratingTriples).asserting {
               _.map(_.id) should contain theSameElementsAs
                 eventsPerProject.map { case (_, list) => list.maxBy(_.date).id }
             }

        _ = givenPrioritisation(takes = Nil, totalOccupancy = eventsPerProject.size, returns = Nil)
        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
  }

  private lazy val now           = Instant.now()
  private lazy val executionDate = ExecutionDate(now)

  private implicit lazy val gauges: EventStatusGauges[IO]     = TestEventStatusGauges[IO]
  private implicit val qet:         QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private lazy val projectPrioritisation = mock[ProjectPrioritisation[IO]]

  private def givenPrioritisation(takes:          List[ProjectInfo],
                                  totalOccupancy: Long,
                                  returns:        List[(ProjectIds, Priority)]
  ) =
    (projectPrioritisation.prioritise _)
      .expects(takes, totalOccupancy)
      .returning(returns)

  private def finder(implicit cfg: DBConfig[EventLogDB]) =
    new EventFinderImpl[IO](() => now, projectsFetchingLimit = 1, projectPrioritisation)

  private def executionDatesInThePast: Gen[ExecutionDate] = timestampsNotInTheFuture map ExecutionDate.apply

  private def readyStatuses = Gen.oneOf(New, GenerationRecoverableFailure)

  private case class CreatedEvent(id: CompoundEventId, body: EventBody, date: EventDate, project: Project) {
    lazy val toAwaitingEvent = AwaitingGenerationEvent(id, project.slug, body)
  }

  private def createEvent(status:        Gen[EventStatus],
                          eventDate:     EventDate = eventDates.generateOne,
                          executionDate: ExecutionDate = executionDatesInThePast.generateOne,
                          batchDate:     BatchDate = batchDates.generateOne,
                          project:       Project = consumerProjects.generateOne
  )(implicit cfg: DBConfig[EventLogDB]): IO[CreatedEvent] = {
    val eventId = compoundEventIds(project.id).generateOne
    val body    = eventBodies.generateOne
    storeEvent(eventId,
               status.generateOne,
               executionDate,
               eventDate,
               body,
               batchDate = batchDate,
               projectSlug = project.slug
    ).as(CreatedEvent(eventId, body, eventDate, project))
  }
}
