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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package v10migration

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class ProjectsFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  private val chunkSize = 50

  "findProjects" should {

    "find requested page of projects existing in the TS" in new TestCase {

      val projects = anyProjectEntities
        .generateList(min = chunkSize + 1, max = Gen.choose(chunkSize + 1, (2 * chunkSize) - 1).generateOne)
        .map(_.to[entities.Project])
        .sortBy(_.path)

      upload(to = projectsDataset, projects: _*)

      val (page1, page2) = projects splitAt chunkSize
      finder.findProjects(page = 1).unsafeRunSync() shouldBe page1.map(_.path)
      finder.findProjects(page = 2).unsafeRunSync() shouldBe page2.map(_.path)
      finder.findProjects(page = 3).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder = new PagedProjectsFinderImpl[IO](RecordsFinder(projectsDSConnectionInfo))
  }
}
