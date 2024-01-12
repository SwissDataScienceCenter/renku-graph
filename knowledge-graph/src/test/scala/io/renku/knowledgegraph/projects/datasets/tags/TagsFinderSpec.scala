/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.testentities._
import io.renku.http.rest.paging.model.Total
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.KnowledgeGraphJenaSpec
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class TagsFinderSpec extends AsyncWordSpec with AsyncIOSpec with KnowledgeGraphJenaSpec with should.Matchers {

  "findTags" should {

    "return all PublicationEvent objects linked to the Dataset family sharing the name" in projectsDSConfig.use {
      implicit pcc =>
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

        for {
          _ <- uploadToProjects(project)

          _ = original.identification.slug                       shouldBe modified.identification.slug
          _ = project.datasets.flatMap(_.publicationEvents).size shouldBe 3

          _ <- finder.findTags(Criteria(project.slug, original.identification.slug)).asserting { response =>
                 response.results shouldBe List(original, modified)
                   .flatMap(_.publicationEvents)
                   .map(_.to[model.Tag])
                   .sortBy(_.startDate)
                   .reverse
                 response.pagingInfo.total shouldBe Total(2)
               }
        } yield Succeeded
    }

    "return PublicationEvent objects linked to the Dataset family sharing the name from a the given project" in projectsDSConfig
      .use { implicit pcc =>
        val (ds, project) = renkuProjectEntities(visibilityPublic)
          .addDataset(
            datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(List(publicationEventFactory)))
          )
          .generateOne

        val dsPublicationEvent = ds.publicationEvents.head

        val (importedDs, projectWithImportedDs) = renkuProjectEntities(visibilityPublic)
          .importDataset(dsPublicationEvent)
          .generateOne

        for {
          _ <- uploadToProjects(project, projectWithImportedDs)

          _ = project.datasets.flatMap(_.publicationEvents).size               shouldBe 1
          _ = projectWithImportedDs.datasets.flatMap(_.publicationEvents).size shouldBe 1

          _ <- finder.findTags(Criteria(project.slug, ds.identification.slug)).asserting { response =>
                 response.results shouldBe List(ds)
                   .flatMap(_.publicationEvents)
                   .map(_.to[model.Tag])
                   .sortBy(_.startDate)
                   .reverse
                 response.pagingInfo.total shouldBe Total(1)
               }

          _ <- finder
                 .findTags(Criteria(projectWithImportedDs.slug, ds.identification.slug))
                 .asserting {
                   _.results shouldBe List(importedDs)
                     .flatMap(_.publicationEvents)
                     .map(_.to[model.Tag])
                     .sortBy(_.startDate)
                     .reverse
                 }
        } yield Succeeded
      }
  }

  "return no PublicationEvent if any on the Dataset" in projectsDSConfig.use { implicit pcc =>
    val (original, project) = renkuProjectEntities(visibilityPublic)
      .addDataset(datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(Nil)))
      .generateOne

    uploadToProjects(project) >>
      finder
        .findTags(Criteria(project.slug, original.identification.slug))
        .asserting { response =>
          response.results          shouldBe Nil
          response.pagingInfo.total shouldBe Total(0)
        }
  }

  "return no PublicationEvent for non-existing Project" in projectsDSConfig.use { implicit pcc =>
    finder.findTags(Criteria(projectSlugs.generateOne, datasetSlugs.generateOne)).asserting { response =>
      response.results          shouldBe Nil
      response.pagingInfo.total shouldBe Total(0)
    }
  }

  "not return PublicationEvent if on Dataset having matching name but on different Project" in projectsDSConfig.use {
    implicit pcc =>
      val (original, project) = renkuProjectEntities(visibilityPublic)
        .addDataset(datasetEntities(provenanceInternal).modify(_.replacePublicationEvents(Nil)))
        .generateOne

      uploadToProjects(project) >>
        finder
          .findTags(Criteria(projectSlugs.generateOne, original.identification.slug))
          .asserting { response =>
            response.results          shouldBe Nil
            response.pagingInfo.total shouldBe Total(0)
          }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def finder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    new TagsFinderImpl[IO](pcc)
  }

  private implicit lazy val toTag: PublicationEvent => model.Tag = event =>
    model.Tag(
      event.name,
      event.startDate,
      event.maybeDescription,
      event.dataset.identification.identifier
    )
}
