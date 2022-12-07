/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import cats.effect.IO
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.{TestExecutionTimeRecorder, TestSparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TriplesRemoverSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "removeAllTriples" should {

    "remove all the triples from the storage except for CLI version" in new TestCase {

      upload(to = projectsDataset, anyRenkuProjectEntities.generateOne, anyRenkuProjectEntities.generateOne)

      triplesCount(on = projectsDataset) should be > 0L

      triplesRemover.removeAllTriples().unsafeRunSync() shouldBe ()

      triplesCount(on = projectsDataset) shouldBe 0L
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val etr = TestExecutionTimeRecorder[IO]()
    private implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO](etr)
    val triplesRemover = new TriplesRemoverImpl[IO](projectsDSConnectionInfo)
  }
}
