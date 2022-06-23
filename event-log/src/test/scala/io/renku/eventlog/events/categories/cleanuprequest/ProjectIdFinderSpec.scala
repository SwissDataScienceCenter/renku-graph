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

package io.renku.eventlog.events.categories.cleanuprequest

import cats.effect.IO
import cats.syntax.all._
import io.renku.db.SqlStatement
import io.renku.eventlog.EventContentGenerators.eventDates
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.metrics.TestLabeledHistogram
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ProjectIdFinderSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryEventLogDbSpec
    with TypeSerializers
    with should.Matchers {

  "findProjectId" should {

    "return id of the project with the given path" in new TestCase {
      val id = projectIds.generateOne
      upsertProject(id, path, eventDates.generateOne)

      finder.findProjectId(path).unsafeRunSync() shouldBe id.some
    }

    "return None if project with the given path does not exist" in new TestCase {
      finder.findProjectId(path).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val path = projectPaths.generateOne

    val queriesExecTimes = TestLabeledHistogram[SqlStatement.Name]("query_id")
    val finder           = new ProjectIdFinderImpl[IO](queriesExecTimes)
  }
}
