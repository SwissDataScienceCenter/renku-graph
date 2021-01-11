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

package io.renku.eventlog

import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.eventlog.Event.{NewEvent, SkippedEvent}
import org.scalacheck.Gen
import eu.timepit.refined.auto._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.language.postfixOps

object DbEventLogGenerators {

  implicit val eventDates:     Gen[EventDate]     = timestampsNotInTheFuture map EventDate.apply
  implicit val createdDates:   Gen[CreatedDate]   = timestampsNotInTheFuture map CreatedDate.apply
  implicit val executionDates: Gen[ExecutionDate] = timestamps map ExecutionDate.apply

  implicit val eventMessages: Gen[EventMessage] = nonEmptyStrings() map EventMessage.apply
  implicit val eventPayloads: Gen[EventPayload] = nonEmptyStrings() map EventPayload.apply

  lazy val newEvents: Gen[NewEvent] = for {
    eventId   <- eventIds
    project   <- eventProjects
    date      <- eventDates
    batchDate <- batchDates
    body      <- eventBodies
  } yield NewEvent(eventId, project, date, batchDate, body)

  lazy val skippedEvents: Gen[SkippedEvent] = for {
    eventId   <- eventIds
    project   <- eventProjects
    date      <- eventDates
    batchDate <- batchDates
    body      <- eventBodies
    message   <- eventMessages
  } yield SkippedEvent(eventId, project, date, batchDate, body, message)

  implicit lazy val events: Gen[Event] =
    Gen.oneOf(newEvents, skippedEvents)

  implicit lazy val eventProjects: Gen[EventProject] = for {
    id   <- projectIds
    path <- projectPaths
  } yield EventProject(id, path)

  implicit lazy val eventProcessingTimes: Gen[EventProcessingTime] = for {
    length   <- positiveLongs()
    timeUnit <- Gen.oneOf(TimeUnit.values().toList)
  } yield EventProcessingTime(FiniteDuration.apply(length, timeUnit))
}
