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

package io.renku.eventlog.events.consumers.statuschange

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.eventlog.events.consumers.statuschange.StatusChangeEvent.ToFailure
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.{CategoryName, consumers}
import io.renku.events.consumers.ProcessExecutor
import io.renku.graph.model.events.EventStatus.{FailureStatus, ProcessingStatus}
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger

private class ToFailureHandler[F[_]: Async: Logger](
    override val categoryName: CategoryName,
    dbUpdater:                 DBUpdater[F, ToFailure[ProcessingStatus, FailureStatus]],
    statusChanger:             StatusChanger[F],
    processExecutor:           ProcessExecutor[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  protected override type Event = ToFailure[ProcessingStatus, FailureStatus]

  override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = ToFailure.decoder,
      process = statusChanger.updateStatuses(dbUpdater)
    )
}

private object ToFailureHandler {

  def apply[F[_]: Async: Logger: MetricsRegistry: QueriesExecutionTimes](
      infoRemover:   DeliveryInfoRemover[F],
      statusChanger: StatusChanger[F]
  ): F[consumers.EventHandler[F]] =
    ProcessExecutor
      .concurrent(5)
      .map(new ToFailureHandler[F](categoryName, new ToFailureUpdater(infoRemover), statusChanger, _))
}
