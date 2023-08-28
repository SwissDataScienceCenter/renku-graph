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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.membersync.PersonOps._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGProjectMembersFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with ScalaCheckPropertyChecks {

  "findProjectMembers" should {

    "return all members of a given project" in new TestCase {
      val members = personEntities(withGitLabId).generateFixedSizeSet()
      val project = anyRenkuProjectEntities.modify(membersLens.modify(_ => members)).generateOne

      upload(to = projectsDataset, project)

      val expectedMembers = members.flatMap(_.toMaybe[KGProjectMember])

      finder.findProjectMembers(project.slug).unsafeRunSync() shouldBe expectedMembers
    }

    "return no members if there's no project with the given slug" in new TestCase {
      finder.findProjectMembers(projectSlugs.generateOne).unsafeRunSync() shouldBe Set.empty
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val finder = new KGProjectMembersFinderImpl[IO](projectsDSConnectionInfo)
  }
}
