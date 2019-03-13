/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.dbeventlog.commands

import java.time.Instant

import ch.datascience.dbeventlog._
import ch.datascience.graph.model.events.{CommitId, ProjectId}
import doobie.util.{Get, Put}

object ModelReadsAndWrites {

  implicit val eventIdGet: Get[CommitId] = Get[String].tmap(CommitId.apply)
  implicit val eventIdPut: Put[CommitId] = Put[String].contramap(_.value)

  implicit val projectIdGet: Get[ProjectId] = Get[Int].tmap(ProjectId.apply)
  implicit val projectIdPut: Put[ProjectId] = Put[Int].contramap(_.value)

  implicit val eventBodyGet: Get[EventBody] = Get[String].tmap(EventBody.apply)
  implicit val eventBodyPut: Put[EventBody] = Put[String].contramap(_.value)

  implicit val createdDateGet: Get[CreatedDate] = Get[Instant].tmap(CreatedDate.apply)
  implicit val createdDatePut: Put[CreatedDate] = Put[Instant].contramap(_.value)

  implicit val executionDateGet: Get[ExecutionDate] = Get[Instant].tmap(ExecutionDate.apply)
  implicit val executionDatePut: Put[ExecutionDate] = Put[Instant].contramap(_.value)

  implicit val messageGet: Get[Message] = Get[String].tmap(Message.apply)
  implicit val messagePut: Put[Message] = Put[String].contramap(_.value)

  implicit val eventStatusGet: Get[EventStatus] = Get[String].tmap(EventStatus.apply)
  implicit val eventStatusPut: Put[EventStatus] = Put[String].contramap(_.value)
}
