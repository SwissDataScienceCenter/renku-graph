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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.lucenereindex

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.scalatest.{OptionValues, Succeeded}

import scala.util.Random

class ProjectDonePersisterSpec
    extends AsyncFlatSpec
    with should.Matchers
    with CustomAsyncIOSpec
    with InMemoryJenaForSpec
    with MigrationsDataset
    with OptionValues {

  it should "persist the (MigrationToV10, renku:migrated, slug) triple" in {

    val slugs = projectSlugs.generateList(min = 2)

    for {
      _ <- slugs.map { slug =>
             val insertQuery = BacklogCreator.asToBeMigratedInserts(migrationName, slug)
             runUpdate(on = migrationsDataset, insertQuery)
           }.sequence

      _ <- progressFinder.findProgressInfo.asserting(_ shouldBe s"${slugs.size} left from ${slugs.size}")

      _ <- donePersister.noteDone(Random.shuffle(slugs).head).assertNoException

      _ <- progressFinder.findProgressInfo.asserting(_ shouldBe s"${slugs.size - 1} left from ${slugs.size}")
    } yield Succeeded
  }

  private implicit lazy val renkuUrl: RenkuUrl                    = renkuUrls.generateOne
  private implicit val logger:        TestLogger[IO]              = TestLogger[IO]()
  private implicit val tr:            SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val tsClient       = TSClient[IO](migrationsDSConnectionInfo)
  private lazy val progressFinder = new ProgressFinderImpl[IO](migrationName, tsClient)
  private lazy val donePersister  = new ProjectDonePersisterImpl[IO](migrationName, tsClient)
}
