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

package ch.datascience.dbeventlog

import EventStatus._
import cats.implicits._
import ch.datascience.dbeventlog.commands.ProcessingStatus
import ch.datascience.dbeventlog.config.RenkuLogTimeout
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import org.scalacheck.Gen

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object DbEventLogGenerators {

  implicit val renkuLogTimeouts: Gen[RenkuLogTimeout] = durations(max = 5 hours) map RenkuLogTimeout.apply

  implicit val eventDates:     Gen[EventDate]     = timestampsNotInTheFuture map EventDate.apply
  implicit val createdDates:   Gen[CreatedDate]   = timestampsNotInTheFuture map CreatedDate.apply
  implicit val executionDates: Gen[ExecutionDate] = timestamps map ExecutionDate.apply
  implicit val eventStatuses: Gen[EventStatus] = Gen.oneOf(
    New,
    Processing,
    TriplesStore,
    RecoverableFailure,
    NonRecoverableFailure
  )
  implicit val eventMessages: Gen[EventMessage] = nonEmptyStrings() map EventMessage.apply
  implicit val processingStatuses: Gen[ProcessingStatus] =
    for {
      total <- positiveInts(max = Integer.MAX_VALUE)
      done  <- positiveInts(max = total.value)
    } yield ProcessingStatus.from[Try](done.value, total.value).fold(throw _, identity)

  implicit lazy val events: Gen[Event] = for {
    eventId   <- eventIds
    project   <- projects
    date      <- eventDates
    batchDate <- batchDates
    body      <- eventBodies
  } yield Event(eventId, project, date, batchDate, body)

  implicit lazy val projects: Gen[EventProject] = for {
    id   <- projectIds
    path <- projectPaths
  } yield EventProject(id, path)
}
