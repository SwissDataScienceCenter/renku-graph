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

package io.renku.eventlog.subscriptions

import cats.MonadError
import cats.syntax.all._
import ch.datascience.events.consumers.subscriptions.SubscriberUrl
import ch.datascience.graph.model.events.CategoryName
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait DispatchRecovery[Interpretation[_], CategoryEvent] {

  def returnToQueue(event: CategoryEvent): Interpretation[Unit]

  def recover(url: SubscriberUrl, event: CategoryEvent): PartialFunction[Throwable, Interpretation[Unit]]
}

private object LoggingDispatchRecovery {

  def apply[Interpretation[_]: MonadError[*[_], Throwable], CategoryEvent](
      categoryName: CategoryName,
      logger:       Logger[Interpretation]
  ): Interpretation[DispatchRecovery[Interpretation, CategoryEvent]] =
    implicitly[MonadError[Interpretation, Throwable]].catchNonFatal {
      new DispatchRecovery[Interpretation, CategoryEvent] {

        override def returnToQueue(event: CategoryEvent): Interpretation[Unit] = ().pure[Interpretation]

        override def recover(url:   SubscriberUrl,
                             event: CategoryEvent
        ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
          logger.error(exception)(s"$categoryName: $event, url = $url failed")
        }
      }
    }
}
