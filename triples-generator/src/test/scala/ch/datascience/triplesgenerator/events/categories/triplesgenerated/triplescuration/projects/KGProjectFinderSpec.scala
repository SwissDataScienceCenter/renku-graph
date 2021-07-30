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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGProjectFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "find" should {

    "return name, derivedFrom, visibility, and creator for a given project ResourceId" in new TestCase {
      forAll(projectEntities(anyVisibility)(anyForksCount).map(_.to[entities.Project])) { project =>
        val maybeParent = project match {
          case projectWithParent: entities.ProjectWithParent    => Some(projectWithParent.parentResourceId)
          case _:                 entities.ProjectWithoutParent => None
        }

        loadToStore(project)

        finder.find(project.resourceId).unsafeRunSync() shouldBe Some(
          (project.name, maybeParent, project.visibility, project.maybeCreator.map(_.resourceId))
        )
      }
    }

    "return no None if there's no Project with the given resourceId" in new TestCase {
      finder.find(projectResourceIds.generateOne).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val finder               = new KGProjectFinderImpl(rdfStoreConfig, logger, timeRecorder)
  }
}
