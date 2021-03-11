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

import cats.data.NonEmptyList
import ch.datascience.events.consumers.Project
import ch.datascience.events.consumers.subscriptions.{SubscriberId, SubscriberUrl}
import ch.datascience.graph.model.events.{BatchDate, CompoundEventId, EventBody, EventId, EventProcessingTime, EventStatus}
import ch.datascience.graph.model.{SchemaVersion, projects}
import ch.datascience.microservices.MicroserviceBaseUrl
import doobie.util.meta.{LegacyInstantMetaInstance, LegacyLocalDateMetaInstance}
import doobie.util.{Get, Put, Read}
import org.postgresql.util.PGInterval

import java.time.{Duration, Instant}

object TypeSerializers extends TypeSerializers

trait TypeSerializers extends LegacyLocalDateMetaInstance with LegacyInstantMetaInstance {

  implicit val eventIdGet: Get[EventId] = Get[String].tmap(EventId.apply)
  implicit val eventIdPut: Put[EventId] = Put[String].contramap(_.value)

  implicit val projectIdGet: Get[projects.Id] = Get[Int].tmap(projects.Id.apply)
  implicit val projectIdPut: Put[projects.Id] = Put[Int].contramap(_.value)

  implicit val projectPathGet: Get[projects.Path] = Get[String].tmap(projects.Path.apply)
  implicit val projectPathPut: Put[projects.Path] = Put[String].contramap(_.value)

  implicit val eventBodyGet: Get[EventBody] = Get[String].tmap(EventBody.apply)
  implicit val eventBodyPut: Put[EventBody] = Put[String].contramap(_.value)

  implicit val createdDateGet: Get[CreatedDate] = Get[Instant].tmap(CreatedDate.apply)
  implicit val createdDatePut: Put[CreatedDate] = Put[Instant].contramap(_.value)

  implicit val executionDateGet: Get[ExecutionDate] = Get[Instant].tmap(ExecutionDate.apply)
  implicit val executionDatePut: Put[ExecutionDate] = Put[Instant].contramap(_.value)

  implicit val eventDateGet: Get[EventDate] = Get[Instant].tmap(EventDate.apply)
  implicit val eventDatePut: Put[EventDate] = Put[Instant].contramap(_.value)

  implicit val batchDateGet: Get[BatchDate] = Get[Instant].tmap(BatchDate.apply)
  implicit val batchDatePut: Put[BatchDate] = Put[Instant].contramap(_.value)

  implicit val eventMessageGet: Get[EventMessage] = Get[String].tmap(EventMessage.apply)
  implicit val eventMessagePut: Put[EventMessage] = Put[String].contramap(_.value)

  implicit val eventStatusGet: Get[EventStatus] = Get[String].tmap(EventStatus.apply)
  implicit val eventStatusPut: Put[EventStatus] = Put[String].contramap(_.value)

  private val nanosPerSecond = 1000000000L
  private val secsPerMinute  = 60
  private val secsPerHour    = 3600
  private val secsPerDay     = 86400
  private val secsPerMonth   = 30 * secsPerDay
  private val secsPerYear    = (365.25 * secsPerDay).toInt

  implicit val statusProcessingTimeGet: Get[EventProcessingTime] =
    Get.Advanced.other[PGInterval](NonEmptyList.of("interval")).tmap { pgInterval =>
      val nanos = (pgInterval.getSeconds - pgInterval.getSeconds.floor) * nanosPerSecond
      val seconds = pgInterval.getSeconds.toLong +
        pgInterval.getMinutes * secsPerMinute +
        pgInterval.getHours * secsPerHour +
        pgInterval.getDays * secsPerDay +
        pgInterval.getMonths * secsPerMonth +
        pgInterval.getYears * secsPerYear
      EventProcessingTime(Duration.ofSeconds(seconds, nanos.toLong))
    }
  implicit val statusProcessingTimePut: Put[EventProcessingTime] =
    Put.Advanced.other[PGInterval](NonEmptyList.of("interval")).tcontramap[EventProcessingTime] { processingTime =>
      val nano         = processingTime.value.getNano.toDouble / nanosPerSecond.toDouble
      val totalSeconds = processingTime.value.getSeconds
      val years        = totalSeconds / secsPerYear
      val yearLeft     = totalSeconds % secsPerYear
      val months       = yearLeft / secsPerMonth
      val monthLeft    = yearLeft     % secsPerMonth
      val days         = monthLeft / secsPerDay
      val dayLeft      = monthLeft    % secsPerDay
      val hours        = dayLeft / secsPerHour
      val hoursLeft    = dayLeft      % secsPerHour
      val minutes      = hoursLeft / secsPerMinute
      val seconds      = (hoursLeft % secsPerMinute).toDouble + nano
      new PGInterval(
        years.toInt,
        months.toInt,
        days.toInt,
        hours.toInt,
        minutes.toInt,
        seconds
      )
    }

  implicit val compoundEventIdRead: Read[CompoundEventId] = Read[(EventId, projects.Id)].map {
    case (eventId, projectId) => CompoundEventId(eventId, projectId)
  }

  implicit val projectRead: Read[Project] = Read[(projects.Id, projects.Path)].map { case (id, path) =>
    Project(id, path)
  }

  implicit val schemaVersionGet: Get[SchemaVersion] = Get[String].tmap(SchemaVersion.apply)
  implicit val schemaVersionPut: Put[SchemaVersion] = Put[String].contramap(_.value)

  implicit val subscriberUrlGet: Get[SubscriberUrl] = Get[String].tmap(SubscriberUrl.apply)
  implicit val subscriberUrlPut: Put[SubscriberUrl] = Put[String].contramap(_.value)

  implicit val subscriberIdGet: Get[SubscriberId] = Get[String].tmap(SubscriberId.apply)
  implicit val subscriberIdPut: Put[SubscriberId] = Put[String].contramap(_.value)

  implicit val microserviceUrlGet: Get[MicroserviceBaseUrl] = Get[String].tmap(MicroserviceBaseUrl.apply)
  implicit val microserviceUrlPut: Put[MicroserviceBaseUrl] = Put[String].contramap(_.value)
}
