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

package io.renku.eventlog.events.categories.projectsync

import cats.effect.IO
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.eventDates
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectPaths}
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DBUpdaterSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers {

  "update" should {

    "change project_path in the project table for the given project_id" in new TestCase {

      val latestEventDate = eventDates.generateOne
      upsertProject(projectId, projectPath, latestEventDate)

      findProjects shouldBe List((projectId, projectPath, latestEventDate))

      val newPath = projectPaths.generateOne
      updater.update(projectId, newPath).unsafeRunSync() shouldBe ()

      findProjects shouldBe List((projectId, newPath, latestEventDate))
    }

    "do nothing if project with the given id does not exist" in new TestCase {

      updater.update(projectId, projectPath).unsafeRunSync() shouldBe ()

      findProjects shouldBe Nil
    }
  }

  private trait TestCase {
    val projectId   = projectIds.generateOne
    val projectPath = projectPaths.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val updater          = new DBUpdaterImpl[IO](queriesExecTimes)
  }
}
