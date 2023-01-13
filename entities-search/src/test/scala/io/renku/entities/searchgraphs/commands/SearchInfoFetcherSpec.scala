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

package io.renku.entities.searchgraphs
package commands

import Encoders._
import Generators._
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{projectResourceIds, projectVisibilities}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SearchInfoFetcherSpec
    extends AnyWordSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with IOSpec {

  "fetchTSSearchInfos" should {

    "find info about all Datasets that are linked to the Project" in new TestCase {

      val infos = searchInfoObjectsGen(withLinkTo = projectId -> projectVisibilities.generateOne).generateList(min = 1)

      insert(projectsDataset, infos.map(_.asQuads).toSet.flatten)

      // other project DS
      insert(projectsDataset, searchInfoObjectsGen.generateOne.asQuads)

      fetcher.fetchTSSearchInfos(projectId).unsafeRunSync() shouldBe infos.sortBy(_.name).map { info =>
        info.copy(creators = info.creators.sortBy(_.name),
                  keywords = info.keywords.sorted,
                  images = info.images.sortBy(_.position),
                  links = info.links.sortBy(_.projectId)
        )
      }
    }

    "return nothing if no Datasets for the Project" in new TestCase {

      insert(projectsDataset, searchInfoObjectsGen.generateOne.asQuads)

      fetcher.fetchTSSearchInfos(projectId).unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {

    val projectId = projectResourceIds.generateOne

    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val fetcher = new SearchInfoFetcherImpl[IO](projectsDSConnectionInfo)
  }
}
