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

package io.renku.knowledgegraph.projects.datasets.tags

import Endpoint._
import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.{datasetNames, projectPaths}
import io.renku.graph.model.testentities._
import io.renku.http.rest.paging.model.Total
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, RenkuDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TagsFinderSpec extends AnyWordSpec with should.Matchers with InMemoryJenaForSpec with RenkuDataset with IOSpec {

  "findTags" should {

    "return all PublicationEvent objects linked to the Dataset family sharing the name" in new TestCase {

      val (original, modified, project) = {
        val projStage1 = renkuProjectEntities(visibilityPublic).generateOne
        val (original, projStage2) = projStage1.addDataset(
          datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(List(publicationEventFactory)))
        )

        val (modified, projStage3) = projStage2.addDataset(
          original.createModification().modify(_.replacePublicationEvents(List(publicationEventFactory)))
        )

        val (_, projStage4) = projStage3.addDataset(
          datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(List(publicationEventFactory)))
        )

        (original, modified, projStage4)
      }

      upload(to = renkuDataset, project.asJsonLD)

      original.identification.name shouldBe modified.identification.name

      project.datasets.flatMap(_.publicationEvents).size shouldBe 3

      val response = finder.findTags(Criteria(project.path, original.identification.name)).unsafeRunSync()

      response.results shouldBe List(original, modified)
        .flatMap(_.publicationEvents)
        .map(_.to[model.Tag])
        .sortBy(_.startDate)
        .reverse
      response.pagingInfo.total shouldBe Total(2)
    }
  }

  "return no PublicationEvent if any on the Dataset" in new TestCase {

    val (original, project) = renkuProjectEntities(visibilityPublic)
      .addDataset(datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(Nil)))
      .generateOne

    upload(to = renkuDataset, project.asJsonLD)

    val response = finder.findTags(Criteria(project.path, original.identification.name)).unsafeRunSync()

    response.results          shouldBe Nil
    response.pagingInfo.total shouldBe Total(0)
  }

  "return no PublicationEvent for non-existing Project" in new TestCase {

    val response = finder.findTags(Criteria(projectPaths.generateOne, datasetNames.generateOne)).unsafeRunSync()

    response.results          shouldBe Nil
    response.pagingInfo.total shouldBe Total(0)
  }

  "not return PublicationEvent if on Dataset having matching name but on different Project" in new TestCase {

    val (original, project) = renkuProjectEntities(visibilityPublic)
      .addDataset(datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(Nil)))
      .generateOne

    upload(to = renkuDataset, project.asJsonLD)

    val response = finder.findTags(Criteria(projectPaths.generateOne, original.identification.name)).unsafeRunSync()

    response.results          shouldBe Nil
    response.pagingInfo.total shouldBe Total(0)
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val finder = new TagsFinderImpl[IO](renkuDSConnectionInfo)
  }

  private implicit lazy val toTag: PublicationEvent => model.Tag = event =>
    model.Tag(
      event.name,
      event.startDate,
      event.maybeDescription,
      event.dataset.identification.identifier
    )
}
