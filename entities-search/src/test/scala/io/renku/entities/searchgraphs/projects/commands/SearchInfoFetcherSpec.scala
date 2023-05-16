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

package io.renku.entities.searchgraphs.projects
package commands

import Encoders._
import Generators.projectSearchInfoObjects
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.RenkuTinyTypeGenerators.projectResourceIds
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder, TSClient}
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SearchInfoFetcherSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with InMemoryJenaForSpec
    with ProjectsDataset
    with IOSpec {

  "fetchTSSearchInfo" should {

    "find info about all Datasets that are linked to the Project" in new TestCase {

      val info = projectSearchInfoObjects.generateOne

      insert(projectsDataset, info.asQuads)

      // other project DS
      insert(projectsDataset, projectSearchInfoObjects.generateOne.asQuads)

      fetcher.fetchTSSearchInfo(info.id).unsafeRunSync().value shouldBe orderValues(info)
    }

    "return nothing if no Project found" in new TestCase {

      insert(projectsDataset, projectSearchInfoObjects.generateOne.asQuads)

      fetcher.fetchTSSearchInfo(projectResourceIds.generateOne).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {

    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val fetcher = new SearchInfoFetcherImpl[IO](TSClient(projectsDSConnectionInfo))
  }

  private def orderValues(info: ProjectSearchInfo) =
    info.copy(keywords = info.keywords.sorted, images = info.images.sortBy(_.position))
}
