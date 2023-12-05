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

package io.renku.eventlog

import cats.effect._
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.testtools.IOSpec
import natchez.Trace.Implicits.noop
import skunk._

trait ExternalEventLogDb { self: IOSpec =>

  val dbConfig: DBConfig[EventLogDB] =
    DBConfig(
      "localhost",
      5432,
      "event_log",
      "renku",
      "renku",
      1
    )

  implicit lazy val sessionResource: SessionResource[IO] = io.renku.db.SessionResource[IO, EventLogDB](
    Session.single(
      host = dbConfig.host,
      port = dbConfig.port,
      user = dbConfig.user,
      database = dbConfig.name,
      password = Some(dbConfig.pass)
    ),
    dbConfig
  )
}
