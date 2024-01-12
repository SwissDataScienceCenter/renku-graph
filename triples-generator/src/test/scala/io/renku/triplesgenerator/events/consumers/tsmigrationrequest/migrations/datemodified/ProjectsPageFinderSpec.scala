/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore._
import org.scalacheck.Gen
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import tooling._

class ProjectsPageFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with TriplesGeneratorJenaSpec
    with AsyncMockFactory
    with should.Matchers
    with OptionValues {

  private val pageSize = 50

  it should "return next page of projects for migration each time the method is called" in migrationsDSConfig.use {
    implicit mcc =>
      val slugs = projectSlugs
        .generateList(min = pageSize + 1, max = Gen.choose(pageSize + 1, (2 * pageSize) - 1).generateOne)
        .sorted

      val (page1, page2) = slugs splitAt pageSize

      runUpdate(BacklogCreator.asToBeMigratedInserts.apply(slugs).value).assertNoException >>
        finder.nextProjectsPage().asserting(_ shouldBe page1) >>
        page1.traverse_(donePersister.noteDone).assertNoException >>
        finder.nextProjectsPage().asserting(_ shouldBe page2) >>
        page2.traverse_(donePersister.noteDone).assertNoException >>
        finder.nextProjectsPage().asserting(_ shouldBe Nil)
  }

  private implicit lazy val renkuUrl:     RenkuUrl                    = renkuUrls.generateOne
  private implicit lazy val logger:       TestLogger[IO]              = TestLogger[IO]()
  private implicit lazy val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
  private def finder(implicit mcc: MigrationsConnectionConfig) = new ProjectsPageFinderImpl[IO](RecordsFinder[IO](mcc))
  private def donePersister(implicit mcc: MigrationsConnectionConfig) = new ProjectDonePersisterImpl[IO](tsClient)
}
