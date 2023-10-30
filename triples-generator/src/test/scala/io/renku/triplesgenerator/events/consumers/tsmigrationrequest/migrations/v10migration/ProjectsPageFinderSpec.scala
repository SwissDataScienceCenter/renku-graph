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
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.OptionValues
import tooling._

class ProjectsPageFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with MigrationsDataset
    with MockFactory
    with OptionValues {

  private val pageSize = 50

  "nextProjectsPage" should {

    "return next page of projects for migration each time the method is called" in new TestCase {

      val slugs = projectSlugs
        .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)
        .sorted

      runUpdate(on = migrationsDataset, BacklogCreator.asToBeMigratedInserts.apply(slugs).value).unsafeRunSync()

      val (page1, page2) = slugs splitAt pageSize

      finder.nextProjectsPage().unsafeRunSync() shouldBe page1

      page1.foreach(donePersister.noteDone(_).unsafeRunSync())
      finder.nextProjectsPage().unsafeRunSync() shouldBe page2

      page2.foreach(donePersister.noteDone(_).unsafeRunSync())
      finder.nextProjectsPage().unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    implicit val renkuUrl:             RenkuUrl                    = renkuUrls.generateOne
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder        = new ProjectsPageFinderImpl[IO](RecordsFinder[IO](migrationsDSConnectionInfo))
    val donePersister = new ProjectDonePersisterImpl[IO](TSClient[IO](migrationsDSConnectionInfo))
  }
}
