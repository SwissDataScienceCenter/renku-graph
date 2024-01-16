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

package io.renku.eventlog

import cats.MonadThrow
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider

sealed trait EventLogDB

object EventLogDB {
  type SessionResource[F[_]] = io.renku.db.SessionResource[F, EventLogDB]

  object SessionResource {
    def apply[F[_]](implicit sr: SessionResource[F]): SessionResource[F] = sr
  }
}

class EventLogDbConfigProvider[F[_]: MonadThrow]()
    extends DBConfigProvider[F, EventLogDB](
      namespace = "event-log",
      dbName = "event_log"
    )
