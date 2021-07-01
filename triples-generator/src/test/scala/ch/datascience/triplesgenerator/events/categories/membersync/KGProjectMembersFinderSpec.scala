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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators.projectPaths
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.events.categories.membersync.PersonOps._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGProjectMembersFinderSpec
    extends AnyWordSpec
    with InMemoryRdfStore
    with ScalaCheckPropertyChecks
    with should.Matchers {

  "findProjectMembers" should {

    "return all members of a given project" in new TestCase {
      val members = personEntities(withGitLabId).generateFixedSizeSet()
      val project = projectEntities[Project.ForksCount.Zero](visibilityAny).generateOne.copy(members = members)

      loadToStore(project)

      val expectedMembers = members.flatMap(_.toMaybe[KGProjectMember])

      finder.findProjectMembers(project.path).unsafeRunSync() shouldBe expectedMembers
    }

    "return no members if there's no project with the given path" in new TestCase {
      finder.findProjectMembers(projectPaths.generateOne).unsafeRunSync() shouldBe Set.empty
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val finder               = new KGProjectMembersFinderImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }
}
