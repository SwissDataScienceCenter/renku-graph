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
import cats.syntax.all._
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.eventlog.{InMemoryEventLogDbSpec, TypeSerializers}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventContentGenerators.eventDates
import io.renku.graph.model.GraphModelGenerators._
import io.renku.metrics.TestMetricsRegistry
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

    "return id of the project with the given slug" in new TestCase {
      val id = projectIds.generateOne
      upsertProject(id, slug, eventDates.generateOne)

      finder.findProjectId(slug).unsafeRunSync() shouldBe id.some
    }

    "return None if project with the given path does not exist" in new TestCase {
      finder.findProjectId(slug).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    val slug = projectSlugs.generateOne

    private implicit val metricsRegistry:  TestMetricsRegistry[IO]   = TestMetricsRegistry[IO]
    private implicit val queriesExecTimes: QueriesExecutionTimes[IO] = QueriesExecutionTimes[IO]().unsafeRunSync()
    val finder = new ProjectIdFinderImpl[IO]
  }
}
