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

package ch.datascience.dbeventlog.commands

import cats.data.NonEmptyList
import cats.effect.{Bracket, ContextShift, IO}
import cats.implicits._
import ch.datascience.db.DbTransactor
import ch.datascience.dbeventlog.EventLogDB
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.ProjectId
import doobie.implicits._
import doobie.util.fragments.in

import scala.language.higherKinds

class EventLogVerifyExistence[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB]
)(implicit ME:  Bracket[Interpretation, Throwable]) {

  def filterNotExistingInLog(eventIds: List[CommitId], projectId: ProjectId): Interpretation[List[CommitId]] =
    eventIds match {
      case Nil          => ME.pure(List.empty)
      case head +: tail => checkInDB(NonEmptyList.of(head, tail: _*), projectId)
    }

  private def checkInDB(eventIds: NonEmptyList[CommitId], projectId: ProjectId) = {
    fr"""
    select event_id
    from event_log
    where project_id = $projectId""" ++ `and event_id IN`(eventIds)
  }.query[CommitId]
    .to[List]
    .transact(transactor.get)
    .map(existingEventIds => eventIds.toList diff existingEventIds)

  private def `and event_id IN`(eventIds: NonEmptyList[CommitId]) =
    fr" and " ++ in(fr"event_id", eventIds)
}

class IOEventLogVerifyExistence(
    transactor:          DbTransactor[IO, EventLogDB]
)(implicit contextShift: ContextShift[IO])
    extends EventLogVerifyExistence[IO](transactor)
