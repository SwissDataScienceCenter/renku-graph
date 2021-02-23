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

package ch.datascience.graph.model

import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events._
import org.scalacheck.Gen

import java.time.Duration

object EventsGenerators {

  implicit val categoryNames:  Gen[CategoryName]  = nonBlankStrings() map (value => CategoryName(value.value))
  implicit val commitIds:      Gen[CommitId]      = shas map CommitId.apply
  implicit val commitMessages: Gen[CommitMessage] = nonEmptyStrings() map CommitMessage.apply
  implicit val committedDates: Gen[CommittedDate] = timestampsNotInTheFuture map CommittedDate.apply
  implicit val eventIds:       Gen[EventId]       = shas map EventId.apply
  implicit val batchDates:     Gen[BatchDate]     = timestampsNotInTheFuture map BatchDate.apply
  implicit val eventBodies:    Gen[EventBody]     = jsons.map(_.noSpaces).map(EventBody.apply)
  implicit val eventStatuses: Gen[EventStatus] = Gen.oneOf(
    New,
    GeneratingTriples,
    TriplesGenerated,
    TransformingTriples,
    TriplesStore,
    Skipped,
    GenerationRecoverableFailure,
    GenerationNonRecoverableFailure
  )

  implicit val compoundEventIds: Gen[CompoundEventId] = for {
    eventId   <- eventIds
    projectId <- projectIds
  } yield CompoundEventId(eventId, projectId)

  implicit lazy val eventProcessingTimes: Gen[EventProcessingTime] =
    javaDurations(min = Duration ofMinutes 10).map(EventProcessingTime.apply)
}
