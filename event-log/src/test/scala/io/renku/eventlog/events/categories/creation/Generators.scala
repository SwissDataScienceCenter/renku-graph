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

package io.renku.eventlog.events.categories.creation

import ch.datascience.events.consumers.ConsumersModelGenerators.projects
import ch.datascience.graph.model.EventsGenerators.{batchDates, eventBodies, eventIds}
import io.renku.eventlog.EventContentGenerators.{eventDates, eventMessages}
import io.renku.eventlog.events.categories.creation.Event.{NewEvent, SkippedEvent}
import org.scalacheck.Gen

private object Generators {
  lazy val newEvents: Gen[NewEvent] = for {
    eventId   <- eventIds
    project   <- projects
    date      <- eventDates
    batchDate <- batchDates
    body      <- eventBodies
  } yield NewEvent(eventId, project, date, batchDate, body)

  lazy val skippedEvents: Gen[SkippedEvent] = for {
    eventId   <- eventIds
    project   <- projects
    date      <- eventDates
    batchDate <- batchDates
    body      <- eventBodies
    message   <- eventMessages
  } yield SkippedEvent(eventId, project, date, batchDate, body, message)

  implicit lazy val newOrSkippedEvents: Gen[Event] = Gen.oneOf(newEvents, skippedEvents)
}
