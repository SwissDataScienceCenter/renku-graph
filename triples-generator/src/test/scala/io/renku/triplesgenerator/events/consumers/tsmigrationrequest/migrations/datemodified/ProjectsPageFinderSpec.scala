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
package datemodified

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import tooling._

class ProjectsPageFinderSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with MigrationsDataset
    with AsyncMockFactory
    with OptionValues {

  private val pageSize = 50

  it should "return next page of projects for migration each time the method is called" in {

    val slugs = projectSlugs
      .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)
      .sorted

    val (page1, page2) = slugs splitAt pageSize

    runUpdate(on = migrationsDataset, BacklogCreator.asToBeMigratedInserts.apply(slugs).value).assertNoException >>
      finder.nextProjectsPage().asserting(_ shouldBe page1) >>
      page1.traverse_(donePersister.noteDone).assertNoException >>
      finder.nextProjectsPage().asserting(_ shouldBe page2) >>
      page2.traverse_(donePersister.noteDone).assertNoException >>
      finder.nextProjectsPage().asserting(_ shouldBe Nil)
  }

  private implicit lazy val renkuUrl:     RenkuUrl                    = renkuUrls.generateOne
  private implicit lazy val logger:       TestLogger[IO]              = TestLogger[IO]()
  private implicit lazy val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val finder        = new ProjectsPageFinderImpl[IO](RecordsFinder[IO](migrationsDSConnectionInfo))
  private lazy val donePersister = new ProjectDonePersisterImpl[IO](TSClient[IO](migrationsDSConnectionInfo))
}
