/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.consumers.statuschange.redoprojecttransformation

import cats.MonadThrow
import cats.data.Kleisli
import io.circe.Encoder
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.eventlog.api.events.StatusChangeEvent.RedoProjectTransformation
import io.renku.eventlog.events.consumers.statuschange
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.events.consumers.statuschange._
import skunk.Session

private[statuschange] class DbUpdater[F[_]: MonadThrow](eventsQueue: StatusChangeEventsQueue[F])
    extends statuschange.DBUpdater[F, RedoProjectTransformation] {

  implicit lazy val eventQueueMessageEncoder: Encoder[RedoProjectTransformation] =
    Encoder[StatusChangeEvent].contramap(identity)

  override def updateDB(event: RedoProjectTransformation): UpdateOp[F] =
    eventsQueue.offer[RedoProjectTransformation](event).map(_ => DBUpdateResults.ForProjects.empty)

  override def onRollback(event: RedoProjectTransformation): Kleisli[F, Session[F], Unit] = RollbackOp.none
}
