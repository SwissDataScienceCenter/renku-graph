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

package io.renku.eventlog.statuschange.commands

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators.projectSchemaVersions
import ch.datascience.graph.model.events.EventStatus
import io.circe.literal._
import io.renku.eventlog.EventContentGenerators._
import io.renku.eventlog.statuschange.ChangeStatusRequest.EventOnlyRequest
import org.scalacheck.Gen

private object Generators {

  def changeStatusRequestsWith(status: EventStatus) =
    Gen.oneOf(eventOnlyRequests(status), eventAndPayloadRequests(status))

  def eventOnlyRequests(status: EventStatus) = for {
    eventId             <- compoundEventIds
    maybeProcessingTime <- eventProcessingTimes.toGeneratorOfOptions
    maybeMessage        <- eventMessages.toGeneratorOfOptions
  } yield EventOnlyRequest(eventId, status, maybeProcessingTime, maybeMessage)

  def eventAndPayloadRequests(status: EventStatus) = for {
    eventOnly     <- eventOnlyRequests(status)
    payload       <- eventPayloads
    schemaVersion <- projectSchemaVersions
  } yield eventOnly.addPayload(json"""{
    "schemaVersion": ${schemaVersion.value},
    "payload": ${payload.value}
  }""".noSpaces)
}
