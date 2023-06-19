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

package io.renku.eventlog.events.consumers.projectsync

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.metrics.MetricsRegistry
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import org.typelevel.log4cats.Logger

private trait SyncRepoMetadataSender[F[_]] {
  def sendSyncRepoMetadata(event: ProjectSyncEvent): F[Unit]
}

private object SyncRepoMetadataSender {
  def apply[F[_]: Async: SessionResource: QueriesExecutionTimes: Logger: MetricsRegistry]
      : F[SyncRepoMetadataSender[F]] =
    triplesgenerator.api.events
      .Client[F]
      .map(new SyncRepoMetadataSenderImpl[F](PayloadFinder[F], _))
}

private class SyncRepoMetadataSenderImpl[F[_]: MonadThrow](payloadFinder: PayloadFinder[F],
                                                           tgClient: triplesgenerator.api.events.Client[F]
) extends SyncRepoMetadataSender[F] {

  override def sendSyncRepoMetadata(event: ProjectSyncEvent): F[Unit] =
    payloadFinder
      .findLatestPayload(event.projectId)
      .map(SyncRepoMetadata(event.projectPath, _)) >>=
      tgClient.send
}
