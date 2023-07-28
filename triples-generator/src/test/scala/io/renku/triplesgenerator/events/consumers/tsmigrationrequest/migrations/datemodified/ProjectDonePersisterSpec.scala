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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.datemodified

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import scala.util.Random

class ProjectDonePersisterSpec
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with MigrationsDataset
    with OptionValues {

  it should "remove the (ProvisionProjectsGraph, renku:toBeMigrated, slug) triple" in {

    val slugs       = projectSlugs.generateList(min = 2)
    val insertQuery = BacklogCreator.asToBeMigratedInserts.apply(slugs).value

    runUpdate(on = migrationsDataset, insertQuery).assertNoException >>
      progressFinder.findProgressInfo.asserting(_ shouldBe s"${slugs.size} left from ${slugs.size}") >>
      donePersister.noteDone(Random.shuffle(slugs).head).assertNoException >>
      progressFinder.findProgressInfo.asserting(_ shouldBe s"${slugs.size - 1} left from ${slugs.size}")
  }

  private implicit lazy val renkuUrl: RenkuUrl                    = renkuUrls.generateOne
  private implicit val logger:        TestLogger[IO]              = TestLogger[IO]()
  private implicit lazy val tr:       SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val tsClient       = TSClient[IO](migrationsDSConnectionInfo)
  private lazy val progressFinder = new ProgressFinderImpl[IO](tsClient)
  private lazy val donePersister  = new ProjectDonePersisterImpl[IO](tsClient)
}
