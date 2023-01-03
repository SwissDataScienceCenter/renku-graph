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

package io.renku.eventlog.events.producers

import EventsSender.SendingResult
import cats.syntax.all._
import cats.{MonadThrow, Show}
import io.renku.events.CategoryName
import io.renku.events.consumers.subscriptions.SubscriberUrl
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait DispatchRecovery[F[_], CategoryEvent] {

  def returnToQueue(event: CategoryEvent, reason: SendingResult): F[Unit]

  def recover(url: SubscriberUrl, event: CategoryEvent): PartialFunction[Throwable, F[Unit]]
}

private object LoggingDispatchRecovery {

  def apply[F[_]: MonadThrow: Logger, CategoryEvent](
      categoryName: CategoryName
  )(implicit show:  Show[CategoryEvent]): F[DispatchRecovery[F, CategoryEvent]] = MonadThrow[F].catchNonFatal {
    new DispatchRecovery[F, CategoryEvent] {

      override def returnToQueue(event: CategoryEvent, reason: SendingResult): F[Unit] = ().pure[F]

      override def recover(url: SubscriberUrl, event: CategoryEvent): PartialFunction[Throwable, F[Unit]] = {
        case NonFatal(exception) =>
          Logger[F].error(exception)(show"$categoryName: $event, url = $url failed")
      }
    }
  }
}
