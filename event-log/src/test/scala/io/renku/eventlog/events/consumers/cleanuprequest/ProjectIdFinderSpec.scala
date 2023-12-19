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

package io.renku.eventlog.events.consumers.cleanuprequest

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.eventlog.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import io.renku.eventlog.{EventLogDB, EventLogPostgresSpec}
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class ProjectIdFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with EventLogPostgresSpec
    with AsyncMockFactory
    with should.Matchers {

  private val project = consumerProjects.generateOne

  it should "return id of the project with the given slug" in testDBResource.use { implicit cfg =>
    upsertProject(project) >>
      finder.findProjectId(project.slug).asserting(_ shouldBe project.id.some)
  }

  it should "return None if project with the given slug does not exist" in testDBResource.use { implicit cfg =>
    finder.findProjectId(project.slug).asserting(_ shouldBe None)
  }

  private def finder(implicit cfg: DBConfig[EventLogDB]) = {
    implicit val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
    new ProjectIdFinderImpl[IO]
  }
}
