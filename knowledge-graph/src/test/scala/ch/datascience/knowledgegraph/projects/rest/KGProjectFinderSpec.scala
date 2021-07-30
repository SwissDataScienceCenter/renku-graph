/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.projects.rest

import Converters._
import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.knowledgegraph.projects.rest.KGProjectFinder.KGProject
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGProjectFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "findProject" should {

    "return details of the project with the given path when there's no parent" in new TestCase {
      forAll(projectEntities[ForksCount.Zero](anyVisibility)) { project =>
        loadToStore(projectEntities[ForksCount.Zero](anyVisibility).generateOne, project)

        metadataFinder.findProject(project.path).unsafeRunSync() shouldBe Some(project.to[KGProject])
      }
    }

    "return details of the project with the given path if it has a parent project" in new TestCase {
      forAll(projectWithParentEntities(anyVisibility)) { project =>
        loadToStore(project)

        metadataFinder.findProject(project.path).unsafeRunSync() shouldBe Some(project.to[KGProject])
      }
    }

    "return None if there's no project with the given path" in new TestCase {
      metadataFinder.findProject(projectPaths.generateOne).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val metadataFinder       = new KGProjectFinderImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }
}
