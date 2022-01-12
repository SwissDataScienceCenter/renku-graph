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

package io.renku.knowledgegraph.entities

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EntitiesFinderSpec extends AnyWordSpec with should.Matchers with InMemoryRdfStore with IOSpec {

  "findEntities" should {

    "return all entities sorted by name if no query is given" in new TestCase {
      val project = projectEntities(visibilityPublic)
        .withDatasets(datasetEntities(provenanceNonModified))
        .withActivities(activityEntities(planEntities()))
        .generateOne

      loadToStore(project)

      finder.findEntities().unsafeRunSync() shouldBe List(project.to[model.Project])
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())
    val finder               = new EntitiesFinderImpl[IO](rdfStoreConfig, timeRecorder)
  }
}
