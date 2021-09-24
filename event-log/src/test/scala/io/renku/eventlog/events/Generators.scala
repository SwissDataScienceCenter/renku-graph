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

package io.renku.eventlog.events

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.{eventIds, eventProcessingTimes, eventStatuses}
import ch.datascience.graph.model.events.EventStatus
import io.renku.eventlog.EventContentGenerators.eventMessages
import io.renku.eventlog.events.EventsEndpoint.{EventInfo, StatusProcessingTime}
import org.scalacheck.Gen

private object Generators {

  lazy val eventInfos: Gen[EventInfo] = for {
    id           <- eventIds
    status       <- eventStatuses
    maybeMessage <- eventMessages.toGeneratorOfOptions
    processingTimes <-
      statusProcessingTimeObjects(status).toGeneratorOfList().map(_.sortBy(_.status)).map(_.distinctBy(_.status))
  } yield EventInfo(id, status, maybeMessage, processingTimes)

  def statusProcessingTimeObjects(lowerThan: EventStatus): Gen[StatusProcessingTime] = for {
    status         <- eventStatuses.retryUntil(Ordering[EventStatus].lt(_, lowerThan))
    processingTime <- eventProcessingTimes
  } yield StatusProcessingTime(status, processingTime)
}
