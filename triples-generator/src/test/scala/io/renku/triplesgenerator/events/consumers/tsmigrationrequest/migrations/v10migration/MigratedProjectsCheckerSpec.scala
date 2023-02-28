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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.v10migration

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.renkuUrls
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Triple
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Random

class MigratedProjectsCheckerSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with MigrationsDataset {

  "filterNotMigrated" should {

    "filter out projects that are registered as already migrated" in new TestCase {

      val paths = projectPaths.generateNonEmptyList().toList

      val migrated = Random.shuffle(paths).take(paths.size / 2)

      migrated foreach { path =>
        insert(to = migrationsDataset, Triple(MigrationToV10.name.asEntityId, renku / "migrated", path.asObject))
      }

      checker.filterNotMigrated(paths).unsafeRunSync() shouldBe (paths diff migrated)
    }
  }

  private trait TestCase {
    implicit val renkuUrl:             RenkuUrl                    = renkuUrls.generateOne
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val checker = new MigratedProjectsCheckerImpl[IO](TSClient[IO](migrationsDSConnectionInfo))
  }
}
