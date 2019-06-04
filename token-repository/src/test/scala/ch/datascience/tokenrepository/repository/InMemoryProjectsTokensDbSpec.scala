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

import ch.datascience.db.DbSpec
import ch.datascience.graph.model.events.ProjectId
import doobie.implicits._
import org.scalatest.TestSuite

trait InMemoryProjectsTokensDbSpec extends DbSpec with InMemoryProjectsTokensDb {
  self: TestSuite =>

  protected def initDb(): Unit = execute {
    sql"""|CREATE TABLE projects_tokens(
          | project_id int4 PRIMARY KEY,
          | token VARCHAR NOT NULL
          |);""".stripMargin.update.run.map(_ => ())
  }

  protected def prepareDbForTest(): Unit = execute {
    sql"TRUNCATE TABLE projects_tokens".update.run.map(_ => ())
  }

  protected def findToken(projectId: ProjectId): Option[String] = execute {
    sql"""select token 
          from projects_tokens  
          where project_id = ${projectId.value}"""
      .query[String]
      .option
  }
}
