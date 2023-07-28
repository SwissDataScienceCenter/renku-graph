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

package io.renku.graph.http.server.security

import cats.effect.IO
import io.renku.generators.CommonGraphGenerators.authUsers
import io.renku.generators.Generators.Implicits._
import io.renku.graph.http.server.security.Authorizer.SecurityRecord
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TSSlugRecordsFinderSpec
    extends AnyWordSpec
    with IOSpec
    with EntitiesGenerators
    with InMemoryJenaForSpec
    with ProjectsDataset
    with should.Matchers {

  "apply" should {

    "return SecurityRecords with project visibility and all project members" in new TestCase {
      val project = anyProjectEntities.generateOne

      upload(to = projectsDataset, project)

      recordsFinder(project.slug, maybeAuthUser).unsafeRunSync() shouldBe List(
        SecurityRecord(project.visibility, project.slug, project.members.flatMap(_.maybeGitLabId))
      )
    }

    "return SecurityRecords with project visibility and no member is project has none" in new TestCase {
      val project = renkuProjectEntities(anyVisibility).generateOne.copy(members = Set.empty)

      upload(to = projectsDataset, project)

      recordsFinder(project.slug, maybeAuthUser).unsafeRunSync() shouldBe List(
        SecurityRecord(project.visibility, project.slug, Set.empty)
      )
    }

    "nothing if there's no project with the given slug" in new TestCase {
      recordsFinder(projectSlugs.generateOne, maybeAuthUser).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {

    val maybeAuthUser = authUsers.generateOption

    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val recordsFinder = new TSSlugRecordsFinderImpl[IO](projectsDSConnectionInfo)
  }
}
