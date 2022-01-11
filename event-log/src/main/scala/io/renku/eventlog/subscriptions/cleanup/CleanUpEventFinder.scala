/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.cleanup

import cats.MonadThrow
import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.eventlog.subscriptions.{EventFinder, SubscriptionTypeSerializers}
import io.renku.metrics.LabeledHistogram
import io.renku.db.SqlStatement
import io.renku.db.DbClient

private class CleanUpEventFinderImpl[F[_]: MonadCancelThrow](
    queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, CleanUpEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): F[Option[CleanUpEvent]] = Option.empty[CleanUpEvent].pure[F]
}

private object CleanUpEventFinder {
  def apply[F[_]: MonadCancelThrow](
      queriesExecTimes: LabeledHistogram[F, SqlStatement.Name]
  ): F[EventFinder[F, CleanUpEvent]] = MonadThrow[F].catchNonFatal {
    new CleanUpEventFinderImpl(queriesExecTimes)
  }
}
