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

package io.renku.eventlog.events.producers
package triplesgenerated

import ProjectPrioritisation.Priority.MaxPriority
import ProjectPrioritisation.{Priority, ProjectInfo}
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.TestEventStatusGauges._
import io.renku.eventlog.metrics.{EventStatusGauges, QueriesExecutionTimes, TestEventStatusGauges, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.events.consumers.Project
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{OptionValues, Succeeded}

import java.time.Instant

private class EventFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues {

  it should s"return the most recent event in status $TriplesGenerated or $TransformationRecoverableFailure " +
    s"and mark it as $TransformingTriples" in testDBResource.use { implicit cfg =>
      for {
        event <- createEvent(status = TriplesGenerated)

        _ <- createEvent(
               status = TransformationRecoverableFailure,
               timestamps(max = event.date.value).generateAs(EventDate),
               project = event.project
             )

        _ <- findEvents(TransformingTriples).asserting(_ shouldBe Nil)

        _ = givenPrioritisation(
              takes = List(ProjectInfo(event.project, event.date, 0)),
              totalOccupancy = 0,
              returns = List(ProjectIds(event.project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting { actual =>
               actual.id          shouldBe event.id
               actual.projectSlug shouldBe event.project.slug
               actual.payload.value should contain theSameElementsAs event.payload.value
             }

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(FoundEvent(event.id, executionDate))
             }

        _ = givenPrioritisation(takes = Nil, totalOccupancy = 1, returns = Nil)

        _ <- finder.popEvent().asserting(_ shouldBe None)

        _ <- gauges.awaitingTransformation.getValue(event.project.slug).asserting(_ shouldBe -1d)
        _ <- gauges.underTransformation.getValue(event.project.slug).asserting(_ shouldBe 1d)

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(FoundEvent(event.id, executionDate))
             }
      } yield Succeeded
    }

  it should "return an event if there are multiple latest events with the same date" in testDBResource.use {
    implicit cfg =>
      val project         = consumerProjects.generateOne
      val latestEventDate = eventDates.generateOne
      for {
        event1 <- createEvent(status = Gen.oneOf(TriplesGenerated, TransformationRecoverableFailure).generateOne,
                              eventDate = latestEventDate,
                              project = project
                  )
        event2 <- createEvent(
                    status = Gen.oneOf(TriplesGenerated, TransformationRecoverableFailure).generateOne,
                    eventDate = latestEventDate,
                    project = project
                  )

        _ <- findEvents(TransformingTriples).asserting(_ shouldBe List.empty)

        // 1st event with the same event date
        _ = givenPrioritisation(
              takes = List(ProjectInfo(project, latestEventDate, 0)),
              totalOccupancy = 0,
              returns = List(ProjectIds(project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting { actual =>
               if (actual.id == event1.id) {
                 actual.projectSlug shouldBe event1.project.slug
                 actual.payload.value should contain theSameElementsAs event1.payload.value
               } else {
                 actual.id          shouldBe event2.id
                 actual.projectSlug shouldBe event2.project.slug
                 actual.payload.value should contain theSameElementsAs event2.payload.value
               }
             }

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) should {
                 be(List(FoundEvent(event1.id, executionDate))) or be(List(FoundEvent(event2.id, executionDate)))
               }
             }

        _ <- gauges.awaitingTransformation.getValue(project.slug).asserting(_ shouldBe -1d)
        _ <- gauges.underTransformation.getValue(project.slug).asserting(_ shouldBe 1d)

        // 2nd event with the same event date
        _ = givenPrioritisation(
              takes = List(ProjectInfo(project, latestEventDate, 1)),
              totalOccupancy = 1,
              returns = List(ProjectIds(project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting { actual =>
               if (actual.id == event1.id) {
                 actual.projectSlug shouldBe event1.project.slug
                 actual.payload.value should contain theSameElementsAs event1.payload.value
               } else {
                 actual.id          shouldBe event2.id
                 actual.projectSlug shouldBe event2.project.slug
                 actual.payload.value should contain theSameElementsAs event2.payload.value
               }
             }

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) should contain theSameElementsAs List(
                 FoundEvent(event1.id, executionDate),
                 FoundEvent(event2.id, executionDate)
               )
             }

        _ <- gauges.awaitingTransformation.getValue(project.slug).asserting(_ shouldBe -2d)
        _ <- gauges.underTransformation.getValue(project.slug).asserting(_ shouldBe 2d)

        // no more events left
        _ = givenPrioritisation(takes = Nil, totalOccupancy = 2, returns = Nil)

        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
  }

  it should "return an event with the latest event date " +
    s"and status $TriplesGenerated or $TransformationRecoverableFailure " +
    s"and mark it as $TransformingTriples " +
    s"case - when a newer event arrive after the pop" in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne
      for {
        event1 <- createEvent(status = TriplesGenerated, project = project)
        _      <- findEvents(TransformingTriples).asserting(_ shouldBe List.empty)

        _ = givenPrioritisation(
              takes = List(ProjectInfo(project, event1.date, 0)),
              totalOccupancy = 0,
              returns = List(ProjectIds(project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting { actual =>
               actual.id          shouldBe event1.id
               actual.projectSlug shouldBe project.slug
               actual.payload.value should contain theSameElementsAs event1.payload.value
             }

        _ <- gauges.awaitingTransformation.getValue(project.slug).asserting(_ shouldBe -1d)
        _ <- gauges.underTransformation.getValue(project.slug).asserting(_ shouldBe 1d)

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(FoundEvent(event1.id, executionDate))
             }

        event2 <- createEvent(
                    status = TransformationRecoverableFailure,
                    timestamps(min = event1.date.value, max = now).generateAs(EventDate),
                    project = project
                  )

        _ = givenPrioritisation(
              takes = List(ProjectInfo(project, event2.date, 1)),
              totalOccupancy = 1,
              returns = List(ProjectIds(project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting { actual =>
               actual.id          shouldBe event2.id
               actual.projectSlug shouldBe project.slug
               actual.payload.value should contain theSameElementsAs event2.payload.value
             }

        _ <- gauges.awaitingTransformation.getValue(project.slug).asserting(_ shouldBe -2d)
        _ <- gauges.underTransformation.getValue(project.slug).asserting(_ shouldBe 2d)

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(FoundEvent(event1.id, executionDate),
                                                                            FoundEvent(event2.id, executionDate)
               )
             }
      } yield Succeeded
    }

  it should s"skip events in $TriplesGenerated status which do not have payload - within a project" in testDBResource
    .use { implicit cfg =>
      val project    = consumerProjects.generateOne
      val event1Date = timestampsNotInTheFuture.generateAs(EventDate)
      for {
        _ <- storeEvent(
               compoundEventIds(projectId = project.id).generateOne,
               TriplesGenerated,
               executionDatesInThePast.generateOne,
               event1Date,
               eventBodies.generateOne,
               projectSlug = project.slug,
               maybeEventPayload = None
             )
        event2 <- createEvent(
                    status = TriplesGenerated,
                    eventDate = timestamps(max = event1Date.value).generateAs(EventDate),
                    project = project
                  )

        _ = givenPrioritisation(
              takes = List(ProjectInfo(project, event1Date, 0)),
              totalOccupancy = 0,
              returns = List(ProjectIds(project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting { actual =>
               actual.id          shouldBe event2.id
               actual.projectSlug shouldBe project.slug
               actual.payload.value should contain theSameElementsAs event2.payload.value
             }

        _ <- gauges.awaitingTransformation.getValue(project.slug).asserting(_ shouldBe -1d)
        _ <- gauges.underTransformation.getValue(project.slug).asserting(_ shouldBe 1d)

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(FoundEvent(event2.id, executionDate))
             }
      } yield Succeeded
    }

  it should s"skip projects with events in $TriplesGenerated status which do not have payload" in testDBResource.use {
    implicit cfg =>
      val event1Date = timestampsNotInTheFuture.generateAs(EventDate)
      for {
        _ <- storeEvent(
               compoundEventIds.generateOne,
               TriplesGenerated,
               executionDatesInThePast.generateOne,
               event1Date,
               eventBodies.generateOne,
               projectSlug = projectSlugs.generateOne,
               maybeEventPayload = None
             )
        event2 <- createEvent(
                    status = TriplesGenerated,
                    eventDate = timestamps(max = event1Date.value).generateAs(EventDate),
                    project = consumerProjects.generateOne
                  )

        _ = givenPrioritisation(
              takes = List(ProjectInfo(event2.project, event2.date, 0)),
              totalOccupancy = 0,
              returns = List(ProjectIds(event2.project) -> MaxPriority)
            )

        _ <- finder.popEvent().map(_.value).asserting { actual =>
               actual.id          shouldBe event2.id
               actual.projectSlug shouldBe event2.project.slug
               actual.payload.value should contain theSameElementsAs event2.payload.value
             }

        _ <- gauges.awaitingTransformation.getValue(event2.project.slug).asserting(_ shouldBe -1d)
        _ <- gauges.underTransformation.getValue(event2.project.slug).asserting(_ shouldBe 1d)

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.select(Field.Id, Field.ExecutionDate)) shouldBe List(FoundEvent(event2.id, executionDate))
             }
      } yield Succeeded
  }

  it should "return no event when execution date is in the future " +
    s"and status $TriplesGenerated or $TransformationRecoverableFailure " in testDBResource.use { implicit cfg =>
      val project = consumerProjects.generateOne
      for {
        event1 <- createEvent(
                    status = TriplesGenerated,
                    eventDate = timestamps(max = Instant.now().minusSeconds(5)).generateAs(EventDate),
                    project = project
                  )
        event2 <- createEvent(
                    status = TransformationRecoverableFailure,
                    eventDate = EventDate(event1.date.value plusSeconds 1),
                    executionDate = ExecutionDate(timestampsInTheFuture.generateOne),
                    project = project
                  )
        _ <- createEvent(
               status = TriplesGenerated,
               eventDate = EventDate(event2.date.value plusSeconds 1),
               executionDate = ExecutionDate(timestampsInTheFuture.generateOne),
               project = project
             )

        _ <- findEvents(TransformingTriples).asserting(_ shouldBe List.empty)

        _ = givenPrioritisation(takes = Nil, totalOccupancy = 0, returns = Nil)

        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
    }

  it should "return events from all the projects" in testDBResource.use { implicit cfg =>
    for {
      events <- readyStatuses.generateNonEmptyList(min = 2).map(createEvent(_)).toList.sequence

      _ <- findEvents(TransformingTriples).asserting(_ shouldBe List.empty)

      _ = events.sortBy(_.date).reverse.zipWithIndex foreach { case (event, idx) =>
            givenPrioritisation(
              takes = List(ProjectInfo(event.project, event.date, 0)),
              totalOccupancy = idx,
              returns = List(ProjectIds(event.project) -> MaxPriority)
            )
          }

      _ <- events.map { _ =>
             finder.popEvent().asserting(_ shouldBe a[Some[_]])
           }.sequence

      _ <- events.map { event =>
             gauges.awaitingTransformation.getValue(event.project.slug).asserting(_ shouldBe -1d)
             gauges.underTransformation.getValue(event.project.slug).asserting(_ shouldBe 1d)
           }.sequence

      _ <- findEvents(status = TransformingTriples).asserting {
             _.map(_.id) shouldBe events.map(_.id)
           }
    } yield Succeeded
  }

  it should "return events from all the projects - case with projectsFetchingLimit > 1" in testDBResource.use {
    implicit cfg =>
      val finder = new EventFinderImpl[IO](() => now, projectsFetchingLimit = 5, projectPrioritisation)

      for {
        events <- readyStatuses
                    .generateNonEmptyList(min = 3, max = 6)
                    .toList
                    .flatMap { status =>
                      (1 to positiveInts(max = 2).generateOne.value).map(_ => createEvent(status))
                    }
                    .sequence

        _ <- findEvents(TransformingTriples).asserting(_ shouldBe List.empty)

        eventsGroupedByProjects = events.groupBy(_.id.projectId)
        _ = eventsGroupedByProjects.zipWithIndex foreach {
              case ((_, event :: _), idx) =>
                (projectPrioritisation.prioritise _)
                  .expects(*, idx)
                  .returning(List(ProjectIds(event.project) -> MaxPriority))
              case ((_, Nil), _) => ()
            }

        _ <- eventsGroupedByProjects.toList.map { _ =>
               finder.popEvent().asserting(_ shouldBe a[Some[_]])
             }.sequence

        _ <- eventsGroupedByProjects.toList.map { case (_, events) =>
               val slug = events.head.project.slug
               gauges.awaitingTransformation.getValue(slug).asserting(_ shouldBe -1d) >>
                 gauges.underTransformation.getValue(slug).asserting(_ shouldBe 1d)
             }.sequence

        _ <- findEvents(TransformingTriples).asserting {
               _.map(_.id) should contain theSameElementsAs
                 eventsGroupedByProjects.map { case (_, events) => events.maxBy(_.date).id }.toList
             }

        _ = givenPrioritisation(takes = Nil, totalOccupancy = eventsGroupedByProjects.size, returns = Nil)

        _ <- finder.popEvent().asserting(_ shouldBe None)
      } yield Succeeded
  }

  private lazy val now           = Instant.now()
  private lazy val executionDate = ExecutionDate(now)

  private implicit val gauges: EventStatusGauges[IO]     = TestEventStatusGauges[IO]
  private implicit val qet:    QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
  private val projectPrioritisation = mock[ProjectPrioritisation[IO]]

  private def givenPrioritisation(takes:          List[ProjectInfo],
                                  totalOccupancy: Long,
                                  returns:        List[(ProjectIds, Priority)]
  ) = (projectPrioritisation.prioritise _)
    .expects(takes, totalOccupancy)
    .returning(returns)

  private def finder(implicit cfg: DBConfig[EventLogDB]) =
    new EventFinderImpl[IO](() => now, projectsFetchingLimit = 1, projectPrioritisation)

  private def executionDatesInThePast: Gen[ExecutionDate] = timestampsNotInTheFuture map ExecutionDate.apply

  private def readyStatuses = Gen.oneOf(TriplesGenerated, TransformationRecoverableFailure)

  private case class CreatedEvent(id:      CompoundEventId,
                                  body:    EventBody,
                                  date:    EventDate,
                                  project: Project,
                                  payload: ZippedEventPayload
  )

  private def createEvent(status:        EventStatus,
                          eventDate:     EventDate = eventDates.generateOne,
                          executionDate: ExecutionDate = executionDatesInThePast.generateOne,
                          batchDate:     BatchDate = batchDates.generateOne,
                          project:       Project = consumerProjects.generateOne,
                          eventPayload:  ZippedEventPayload = zippedEventPayloads.generateOne
  )(implicit cfg: DBConfig[EventLogDB]): IO[CreatedEvent] = {
    val eventId   = compoundEventIds(project.id).generateOne
    val eventBody = eventBodies.generateOne
    storeEvent(
      eventId,
      status,
      executionDate,
      eventDate,
      eventBody,
      batchDate = batchDate,
      projectSlug = project.slug,
      maybeEventPayload = eventPayload.some
    ).as(CreatedEvent(eventId, eventBody, eventDate, project, eventPayload))
  }
}
