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

package io.renku.eventlog.statuschange.commands

import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.events.CompoundEventId
import doobie.util.fragment.Fragment
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.renku.eventlog.{EventLogDB, EventStatus, TypesSerializers}

import scala.language.higherKinds

trait ChangeStatusCommand[Interpretation[_]] extends Product with Serializable with TypesSerializers {
  def eventId: CompoundEventId
  def status:  EventStatus
  def query:   SqlQuery[Int]
  def updateGauges(updateResult: UpdateResult)(
      implicit transactor:       DbTransactor[Interpretation, EventLogDB]
  ): Interpretation[Unit]

  def mapResult: Int => UpdateResult = {
    case 0 => UpdateResult.Conflict
    case 1 => UpdateResult.Updated
    case _ => UpdateResult.Failure(Refined.unsafeApply(s"An attempt to set status $status on $eventId failed"))
  }
}

sealed trait UpdateResult extends Product with Serializable

object UpdateResult {
  case object Conflict extends UpdateResult
  case object Updated  extends UpdateResult
  case class Failure(message: String Refined NonEmpty) extends UpdateResult
}
