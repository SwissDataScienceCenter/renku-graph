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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations.v10migration

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.graph.model.versions.SchemaVersion
import io.renku.interpreters.TestLogger
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class MigrationNeedCheckerSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with should.Matchers
    with AsyncMockFactory {

  private val v9  = SchemaVersion("9")
  private val v10 = SchemaVersion("10")

  "checkMigrationNeeded" should {

    "return Yes if there are projects in schema v9" in projectsDSConfig.use { implicit pcc =>
      val v9Project  = anyRenkuProjectEntities.map(setSchema(v9)).generateOne
      val v10Project = anyRenkuProjectEntities.map(setSchema(v10)).generateOne

      uploadToProjects(v9Project, v10Project) >>
        checker.checkMigrationNeeded.asserting(_ shouldBe a[ConditionedMigration.MigrationRequired.Yes])
    }

    "return No if there are only projects in schema different than v9" in projectsDSConfig.use { implicit pcc =>
      uploadToProjects(anyRenkuProjectEntities.map(setSchema(v10)).generateOne) >>
        checker.checkMigrationNeeded.asserting(_ shouldBe a[ConditionedMigration.MigrationRequired.No])
    }

    "return No if there are projects without schema" in projectsDSConfig.use { implicit pcc =>
      uploadToProjects(anyNonRenkuProjectEntities.generateOne) >>
        checker.checkMigrationNeeded.asserting(_ shouldBe a[ConditionedMigration.MigrationRequired.No])
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def checker(implicit pcc: ProjectsConnectionConfig) = new MigrationNeedCheckerImpl[IO](tsClient)

  private def setSchema(version: SchemaVersion): Project => Project =
    _.fold(_.copy(version = version), _.copy(version = version), identity, identity)
}
