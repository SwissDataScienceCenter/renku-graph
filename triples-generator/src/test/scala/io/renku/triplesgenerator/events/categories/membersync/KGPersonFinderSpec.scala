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

package io.renku.triplesgenerator.events.categories.membersync

import Generators._
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGPersonFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "findPersonIds" should {

    "return person resourceIds if found in the triples store" in new TestCase {

      val memberNonExistingInKG = gitLabProjectMembers.generateOne
      val memberExistingInKG    = gitLabProjectMembers.generateOne
      val person                = personEntities(fixed(memberExistingInKG.gitLabId.some)).generateOne

      loadToStore(person)

      finder.findPersonIds(Set(memberNonExistingInKG, memberExistingInKG)).unsafeRunSync() shouldBe Set(
        memberNonExistingInKG -> None,
        memberExistingInKG    -> person.resourceId.some
      )
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val finder               = new KGPersonFinderImpl(rdfStoreConfig, timeRecorder)
  }
}
