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

import java.time.{OffsetDateTime, ZoneOffset}

object TypeSerializers extends TypeSerializers

trait TypeSerializers {

  val eventIdDecoder: Decoder[EventId] = varchar.map(EventId.apply)
  val eventIdEncoder: Encoder[EventId] = varchar.values.contramap(_.value)

  val projectIdDecoder: Decoder[projects.Id] = int4.map(projects.Id.apply)
  val projectIdEncoder: Encoder[projects.Id] = int4.values.contramap(_.value)

  val projectPathDecoder: Decoder[projects.Path] = varchar.map(projects.Path.apply)
  val projectPathEncoder: Encoder[projects.Path] =
    varchar.values.contramap((b: projects.Path) => b.value)

  val eventBodyDecoder: Decoder[EventBody] = text.map(EventBody.apply)
  val eventBodyEncoder: Encoder[EventBody] = text.values.contramap(_.value)

  val createdDateDecoder: Decoder[CreatedDate] = timestamptz.map(timestamp => CreatedDate(timestamp.toInstant))
  val createdDateEncoder: Encoder[CreatedDate] =
    timestamptz.values.contramap((b: CreatedDate) =>
      OffsetDateTime.ofInstant(b.value, b.value.atOffset(ZoneOffset.UTC).toZonedDateTime.getZone)
    )

  val executionDateDecoder: Decoder[ExecutionDate] =
    timestamptz.map(timestamp => ExecutionDate(timestamp.toInstant))
  val executionDateEncoder: Encoder[ExecutionDate] =
    timestamptz.values.contramap((b: ExecutionDate) =>
      OffsetDateTime.ofInstant(b.value, b.value.atOffset(ZoneOffset.UTC).toZonedDateTime.getZone)
    )

  val eventDateDecoder: Decoder[EventDate] = timestamptz.map(timestamp => EventDate(timestamp.toInstant))
  val eventDateEncoder: Encoder[EventDate] =
    timestamptz.values.contramap((b: EventDate) =>
      OffsetDateTime.ofInstant(b.value, b.value.atOffset(ZoneOffset.UTC).toZonedDateTime.getZone)
    )

  val batchDateDecoder: Decoder[BatchDate] = timestamptz.map(timestamp => BatchDate(timestamp.toInstant))
  val batchDateEncoder: Encoder[BatchDate] =
    timestamptz.values.contramap((b: BatchDate) =>
      OffsetDateTime.ofInstant(b.value, b.value.atOffset(ZoneOffset.UTC).toZonedDateTime.getZone)
    )

  val eventMessageDecoder: Decoder[EventMessage] = varchar.map(EventMessage.apply)
  val eventMessageEncoder: Encoder[EventMessage] = varchar.values.contramap(_.value)

  val eventProcessingTimeDecoder: Decoder[EventProcessingTime] = interval.map(EventProcessingTime.apply)
  val eventProcessingTimeEncoder: Encoder[EventProcessingTime] = interval.values.contramap(_.value)

  val eventPayloadDecoder: Decoder[EventPayload] = text.map(EventPayload.apply)
  val eventPayloadEncoder: Encoder[EventPayload] = text.values.contramap(_.value)

  val eventStatusDecoder: Decoder[EventStatus] = varchar.map(EventStatus.apply)
  val eventStatusEncoder: Encoder[EventStatus] = varchar.values.contramap(_.value)

  val schemaVersionDecoder: Decoder[SchemaVersion] = text.map(SchemaVersion.apply)
  val schemaVersionEncoder: Encoder[SchemaVersion] = text.values.contramap(_.value)

  val compoundEventIdDecoder: Decoder[CompoundEventId] = (eventIdDecoder ~ projectIdDecoder).gmap[CompoundEventId]

  val projectDecoder: Decoder[Project] = (projectIdDecoder ~ projectPathDecoder).gmap[Project]

  val subscriberUrlDecoder: Decoder[SubscriberUrl] = varchar.map(SubscriberUrl.apply)
  val subscriberUrlEncoder: Encoder[SubscriberUrl] = varchar.values.contramap(_.value)

  val microserviceBaseUrlDecoder: Decoder[MicroserviceBaseUrl] = varchar.map(MicroserviceBaseUrl.apply)
  val microserviceBaseUrlEncoder: Encoder[MicroserviceBaseUrl] = varchar.values.contramap(_.value)

  val subscriberIdDecoder: Decoder[SubscriberId] = varchar(19).map(SubscriberId.apply)
  val subscriberIdEncoder: Encoder[SubscriberId] = varchar(19).values.contramap(_.value)

  val microserviceIdentifierDecoder: Decoder[MicroserviceIdentifier] = varchar.map(MicroserviceIdentifier.apply)
  val microserviceIdentifierEncoder: Encoder[MicroserviceIdentifier] = varchar.values.contramap(_.value)

  val microserviceUrlDecoder: Decoder[MicroserviceBaseUrl] = varchar.map(MicroserviceBaseUrl.apply)
  val microserviceUrlEncoder: Encoder[MicroserviceBaseUrl] = varchar.values.contramap(_.value)

}
