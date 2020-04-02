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

import java.time.Instant

import ch.datascience.graph.model.events.{BatchDate, CommitId, CompoundEventId, EventBody, EventId}
import ch.datascience.graph.model.projects
import doobie.util.{Get, Put, Read}

object TypesSerializers extends TypesSerializers

trait TypesSerializers {

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

  implicit val compoundEventIdRead: Read[CompoundEventId] = Read[(EventId, projects.Id)].map {
    case (eventId, projectId) => CompoundEventId(eventId, projectId)
  }

  //TODO to be removed
  implicit val commitIdGet: Get[CommitId] = Get[String].tmap(CommitId.apply)
  implicit val commitIdPut: Put[CommitId] = Put[String].contramap(_.value)
}
