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
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DatasetSameAsRecordsFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "apply" should {

    "return SecurityRecord with project visibility, path and all project members" in new TestCase {

      val (dataset, project) =
        anyRenkuProjectEntities.addDataset(datasetEntities(provenanceNonModified)).generateOne

      upload(to = projectsDataset, project)

      recordsFinder(model.datasets.SameAs.ofUnsafe(dataset.provenance.topmostSameAs.value))
        .unsafeRunSync() shouldBe List((project.visibility, project.path, project.members.flatMap(_.maybeGitLabId)))
    }

    "return SecurityRecord with project visibility, path and no member if project is none" in new TestCase {

      val (dataset, project) =
        renkuProjectEntities(anyVisibility)
          .modify(removeMembers())
          .addDataset(datasetEntities(provenanceNonModified))
          .generateOne

      upload(to = projectsDataset, project)

      recordsFinder(model.datasets.SameAs.ofUnsafe(dataset.provenance.topmostSameAs.value))
        .unsafeRunSync() shouldBe List((project.visibility, project.path, Set.empty))
    }

    "return SecurityRecords with projects visibilities, paths and members in case of forks" in new TestCase {

      val (dataset, (parentProject, project)) =
        renkuProjectEntities(anyVisibility)
          .modify(removeMembers())
          .addDataset(datasetEntities(provenanceNonModified))
          .forkOnce()
          .generateOne

      upload(to = projectsDataset, parentProject, project)

      recordsFinder(model.datasets.SameAs.ofUnsafe(dataset.provenance.topmostSameAs.value))
        .unsafeRunSync() should contain theSameElementsAs List(
        (parentProject.visibility, parentProject.path, parentProject.members.flatMap(_.maybeGitLabId)),
        (project.visibility, project.path, project.members.flatMap(_.maybeGitLabId))
      )
    }

    "nothing if there's no project with the given path" in new TestCase {
      recordsFinder(datasetSameAs.generateOne).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient = TSClient[IO](projectsDSConnectionInfo)
    val recordsFinder    = new DatasetSameAsRecordsFinderImpl[IO](tsClient)
  }
}
