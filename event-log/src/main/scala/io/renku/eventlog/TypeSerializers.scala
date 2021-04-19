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

import ch.datascience.events.consumers.Project
import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier}
import skunk.codec.all._
import skunk.{Decoder, Encoder}

import java.time.{OffsetDateTime, ZoneId}

object TypeSerializers extends TypeSerializers

trait TypeSerializers {

  val eventIdGet: Decoder[EventId] = varchar.map(EventId.apply)
  val eventIdPut: Encoder[EventId] = varchar.values.contramap(_.value)

  val projectIdGet: Decoder[projects.Id] = int4.map(projects.Id.apply)
  val projectIdPut: Encoder[projects.Id] = int4.values.contramap(_.value)

  val projectPathGet: Decoder[projects.Path] = varchar.map(projects.Path.apply)
  val projectPathPut: Encoder[projects.Path] =
    varchar.values.contramap((b: projects.Path) => b.value)

  val eventBodyGet: Decoder[EventBody] = text.map(EventBody.apply)
  val eventBodyPut: Encoder[EventBody] = text.values.contramap(_.value)

  val createdDateGet: Decoder[CreatedDate] = timestamptz.map(timestamp => CreatedDate(timestamp.toInstant))
  val createdDatePut: Encoder[CreatedDate] =
    timestamptz.values.contramap((b: CreatedDate) => OffsetDateTime.ofInstant(b.value, ZoneId.systemDefault()))

  val executionDateGet: Decoder[ExecutionDate] =
    timestamptz.map(timestamp => ExecutionDate(timestamp.toInstant))
  val executionDatePut: Encoder[ExecutionDate] =
    timestamptz.values.contramap((b: ExecutionDate) => OffsetDateTime.ofInstant(b.value, ZoneId.systemDefault()))

  val eventDateGet: Decoder[EventDate] = timestamptz.map(timestamp => EventDate(timestamp.toInstant))
  val eventDatePut: Encoder[EventDate] =
    timestamptz.values.contramap((b: EventDate) => OffsetDateTime.ofInstant(b.value, ZoneId.systemDefault()))

  val batchDateGet: Decoder[BatchDate] = timestamptz.map(timestamp => BatchDate(timestamp.toInstant))
  val batchDatePut: Encoder[BatchDate] =
    timestamptz.values.contramap((b: BatchDate) => OffsetDateTime.ofInstant(b.value, ZoneId.systemDefault()))

  val eventMessageGet: Decoder[EventMessage] = varchar.map(EventMessage.apply)
  val eventMessagePut: Encoder[EventMessage] = varchar.values.contramap(_.value)

  val eventProcessingTimeGet: Decoder[EventProcessingTime] = interval.map(EventProcessingTime.apply)
  val eventProcessingTimePut: Encoder[EventProcessingTime] = interval.values.contramap(_.value)

  val eventPayloadGet: Decoder[EventPayload] = text.map(EventPayload.apply)
  val eventPayloadPut: Encoder[EventPayload] = text.values.contramap(_.value)

  val eventStatusGet: Decoder[EventStatus] = varchar.map(EventStatus.apply)
  val eventStatusPut: Encoder[EventStatus] = varchar.values.contramap(_.value)

  val schemaVersionGet: Decoder[SchemaVersion] = text.map(SchemaVersion.apply)
  val schemaVersionPut: Encoder[SchemaVersion] = text.values.contramap(_.value)

  val compoundEventIdGet: Decoder[CompoundEventId] = (eventIdGet ~ projectIdGet).gmap[CompoundEventId]
  val projectGet:         Decoder[Project]         = (projectIdGet ~ projectPathGet).gmap[Project]

  val subscriberUrlGet: Decoder[SubscriberUrl] = varchar.map(SubscriberUrl.apply)
  val subscriberUrlPut: Encoder[SubscriberUrl] = varchar.values.contramap(_.value)

  val microserviceBaseUrlGet: Decoder[MicroserviceBaseUrl] = varchar.map(MicroserviceBaseUrl.apply)
  val microserviceBaseUrlPut: Encoder[MicroserviceBaseUrl] = varchar.values.contramap(_.value)

  val subscriberIdGet: Decoder[SubscriberId] = varchar(19).map(SubscriberId.apply)
  val subscriberIdPut: Encoder[SubscriberId] = varchar(19).values.contramap(_.value)

  val microserviceIdentifierGet: Decoder[MicroserviceIdentifier] = varchar.map(MicroserviceIdentifier.apply)
  val microserviceIdentifierPut: Encoder[MicroserviceIdentifier] = varchar.values.contramap(_.value)

  val microserviceUrlGet: Decoder[MicroserviceBaseUrl] = varchar.map(MicroserviceBaseUrl.apply)
  val microserviceUrlPut: Encoder[MicroserviceBaseUrl] = varchar.values.contramap(_.value)

}
