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

package io.renku.knowledgegraph.projects.update

import ProvisioningStatusFinder.ProvisioningStatus
import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.api.EventLogClient
import io.renku.eventlog.api.EventLogClient.SearchCriteria
import io.renku.graph.model.events.EventStatus
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.projects
import io.renku.http.rest.paging.model.PerPage
import org.typelevel.log4cats.Logger

private trait ProvisioningStatusFinder[F[_]] {
  def checkHealthy(slug: projects.Slug): F[ProvisioningStatus]
}

private object ProvisioningStatusFinder {

  def apply[F[_]: Async: Logger]: F[ProvisioningStatusFinder[F]] =
    EventLogClient[F].map(new ProvisioningStatusFinderImpl[F](_))

  sealed trait ProvisioningStatus {
    lazy val widen: ProvisioningStatus = this
  }
  object ProvisioningStatus {
    type Healthy = Healthy.type
    case object Healthy                             extends ProvisioningStatus
    final case class Unhealthy(status: EventStatus) extends ProvisioningStatus
  }
}

private class ProvisioningStatusFinderImpl[F[_]: MonadThrow](elClient: EventLogClient[F])
    extends ProvisioningStatusFinder[F] {

  private val okProvisioningStatuses = Set(
    New,
    Skipped,
    GeneratingTriples,
    GenerationRecoverableFailure,
    TriplesGenerated,
    TransformingTriples,
    TransformationRecoverableFailure,
    TriplesStore,
    AwaitingDeletion,
    Deleting
  )

  override def checkHealthy(slug: projects.Slug): F[ProvisioningStatus] =
    elClient
      .getEvents(
        SearchCriteria.forProject(slug).withPerPage(PerPage(1)).sortBy(SearchCriteria.Sort.EventDateDesc)
      )
      .flatMap(r => MonadThrow[F].fromEither(r.toEither))
      .map(_.headOption.map(_.status))
      .map(_.fold(ifEmpty = ProvisioningStatus.Healthy.widen) {
        case status if okProvisioningStatuses contains status => ProvisioningStatus.Healthy.widen
        case status                                           => ProvisioningStatus.Unhealthy(status)
      })
}
