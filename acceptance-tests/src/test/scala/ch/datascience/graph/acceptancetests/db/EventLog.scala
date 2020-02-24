/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.acceptancetests.db

import cats.effect.IO
import ch.datascience.db.DBConfigProvider
import ch.datascience.dbeventlog._
import ch.datascience.dbeventlog.commands._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.Id
import doobie.implicits._

import scala.language.postfixOps

object EventLog extends InMemoryEventLogDb {

  def findEvents(projectId: Id, status: EventStatus): List[CommitId] = execute {
    sql"""select event_id
         |from event_log
         |where project_id = $projectId and status = $status
         """.stripMargin
      .query[CommitId]
      .to[List]
  }

  protected override val dbConfig: DBConfigProvider.DBConfig[EventLogDB] =
    new EventLogDbConfigProvider[IO].get().unsafeRunSync()
}
