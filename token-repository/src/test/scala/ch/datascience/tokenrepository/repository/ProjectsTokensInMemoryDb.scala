/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tokenrepository.repository

import ch.datascience.tokenrepository.repository.H2TransactorProvider.transactor
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import cats.implicits._

object ProjectsTokensInMemoryDb {

  def assureProjectsTokensIsEmpty(): Unit =
    sql"""
         |CREATE TABLE projects_tokens(
         | project_id int4 PRIMARY KEY,
         | token VARCHAR (100) NOT NULL,
         | token_type VARCHAR (20) NOT NULL
         |);
       """.stripMargin.update.run
      .flatMap {
        case 1 => 1.pure[ConnectionIO]
        case 0 => emptyProjectsTokensTable()
      }
      .transact(transactor)
      .unsafeRunSync()

  private def emptyProjectsTokensTable() =
    sql"TRUNCATE TABLE projects_tokens".update.run
}
