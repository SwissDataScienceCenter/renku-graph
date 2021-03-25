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

package io.renku.eventlog.events.categories.commitsyncrequest

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.events.consumers.EventHandler
import ch.datascience.events.consumers.subscriptions.SubscriptionMechanism
import ch.datascience.metrics.LabeledHistogram
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

import scala.concurrent.ExecutionContext

object SubscriptionFactory {

  def apply(transactor:       DbTransactor[IO, EventLogDB],
            queriesExecTimes: LabeledHistogram[IO, SqlQuery.Name],
            logger:           Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[(EventHandler[IO], SubscriptionMechanism[IO])] = for {
    handler <- EventHandler(transactor, queriesExecTimes, logger)
  } yield handler -> SubscriptionMechanism.noOpSubscriptionMechanism(categoryName)
}
