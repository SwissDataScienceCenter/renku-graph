/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange.projecteventstonew

import cats.MonadThrow
import io.circe.Encoder
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.eventlog.api.events.StatusChangeEvent.ProjectEventsToNew
import io.renku.eventlog.events.consumers.statuschange
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.events.consumers.statuschange.{DBUpdateResults, StatusChangeEventsQueue}

private[statuschange] class DbUpdater[F[_]: MonadThrow](eventsQueue: StatusChangeEventsQueue[F])
    extends statuschange.DBUpdater[F, ProjectEventsToNew] {

  implicit val eventsQueueMessageEncoder: Encoder[ProjectEventsToNew] =
    Encoder[StatusChangeEvent].contramap(identity)

  override def updateDB(event: ProjectEventsToNew): UpdateOp[F] =
    eventsQueue.offer[ProjectEventsToNew](event).map(_ => DBUpdateResults.ForProjects.empty)

  override def onRollback(event: ProjectEventsToNew)(implicit sr: SessionResource[F]): RollbackOp[F] =
    RollbackOp.empty[F]
}
