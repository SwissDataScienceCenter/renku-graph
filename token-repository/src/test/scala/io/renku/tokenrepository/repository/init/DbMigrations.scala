/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tokenrepository.repository.init

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.db.SqlStatement
import io.renku.interpreters.TestLogger
import io.renku.metrics.{LabeledHistogram, TestLabeledHistogram}
import io.renku.testtools.IOSpec
import io.renku.tokenrepository.repository.InMemoryProjectsTokensDb

trait DbMigrations {
  self: InMemoryProjectsTokensDb with IOSpec =>

  protected type Migration = { def run(): IO[Unit] }

  private lazy val queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name] =
    TestLabeledHistogram[SqlStatement.Name]("query_id")
  protected implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()

  protected lazy val projectsTokensTableCreator: Migration = ProjectsTokensTableCreator(sessionResource)
  protected lazy val projectPathAdded: Migration = ProjectPathAdder(sessionResource, queriesExecTimes).unsafeRunSync()
  protected lazy val duplicateProjectsRemover: Migration = DuplicateProjectsRemover(sessionResource)

  protected lazy val allMigrations: List[Migration] =
    List(projectsTokensTableCreator, projectPathAdded, duplicateProjectsRemover)
}
