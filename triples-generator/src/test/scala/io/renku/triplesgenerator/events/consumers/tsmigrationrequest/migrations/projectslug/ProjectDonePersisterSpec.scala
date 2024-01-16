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
package projectslug

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators.projectSlugs
import io.renku.graph.model.RenkuUrl
import io.renku.interpreters.TestLogger
import io.renku.triplesstore._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

import scala.util.Random

class ProjectDonePersisterSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with should.Matchers
    with OptionValues {

  it should "remove the (AddProjectSlug, renku:toBeMigrated, slug) triple" in migrationsDSConfig.use { implicit mcc =>
    val slugs       = projectSlugs.generateList(min = 2)
    val insertQuery = BacklogCreator.asToBeMigratedInserts.apply(slugs).value
    val finder      = progressFinder

    runUpdate(insertQuery).assertNoException >>
      finder.findProgressInfo.asserting(_ shouldBe s"${slugs.size} left from ${slugs.size}") >>
      donePersister.noteDone(Random.shuffle(slugs).head).assertNoException >>
      finder.findProgressInfo.asserting(_ shouldBe s"${slugs.size - 1} left from ${slugs.size}")
  }

  private implicit lazy val renkuUrl: RenkuUrl       = renkuUrls.generateOne
  private implicit val logger:        TestLogger[IO] = TestLogger[IO]()
  private def progressFinder(implicit mcc: MigrationsConnectionConfig) = new ProgressFinderImpl[IO](tsClient)
  private def donePersister(implicit mcc:  MigrationsConnectionConfig) = new ProjectDonePersisterImpl[IO](tsClient)
}
