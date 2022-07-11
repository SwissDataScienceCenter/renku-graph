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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.datasetImageUris
import io.renku.graph.model.datasets.{ImagePosition, ImageResourceId}
import io.renku.graph.model.testentities._
import io.renku.rdfstore.{InMemoryJenaForSpec, RenkuDataset}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MalformedDSImageIdsSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with RenkuDataset {

  "query" should {

    "find projects with at least one DS with an Image having a broken Resource Id" in {
      val project1 = {
        val p = renkuProjectEntities(anyVisibility)
          .withDatasets(datasetEntities(provenanceInternal))
          .withDatasets(datasetEntities(provenanceInternal))
          .generateOne
          .to[entities.RenkuProject.WithoutParent]
        val ds1 :: ds2 :: Nil = p.datasets
        p.copy(datasets = List(ds1, addImageWithBrokenId(ds2)))
      }
      val project2 = {
        val p = renkuProjectEntities(anyVisibility)
          .withDatasets(datasetEntities(provenanceInternal))
          .generateOne
          .to[entities.RenkuProject.WithoutParent]
        p.copy(datasets = p.datasets map addImageWithBrokenId)
      }
      val project3 = renkuProjectEntities(anyVisibility)
        .withDatasets(
          datasetEntities(provenanceInternal).modify(ds =>
            ds.copy(additionalInfo = ds.additionalInfo.copy(images = datasetImageUris.generateNonEmptyList().toList))
          )
        )
        .generateOne
        .to[entities.RenkuProject.WithoutParent]

      upload(to = renkuDataset,
             project1,
             project2,
             project3,
             anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      )

      runSelect(on = renkuDataset, MalformedDSImageIds.query)
        .unsafeRunSync()
        .map(row => projects.Path(row("path")))
        .toSet shouldBe Set(project1.path, project2.path)
    }
  }

  private def addImageWithBrokenId(
      ds: entities.Dataset[entities.Dataset.Provenance]
  ): entities.Dataset[entities.Dataset.Provenance] = {
    val position = ImagePosition(0)
    ds.copy(additionalInfo =
      ds.additionalInfo.copy(images =
        entities.Dataset
          .Image(
            malformedImageId(ds, position),
            datasetImageUris.generateOne,
            position
          )
          .some
          .toList
      )
    )
  }

  private def malformedImageId(ds: entities.Dataset[entities.Dataset.Provenance], position: ImagePosition) = {
    val identifier = ds.identification.identifier
    val id = Dataset
      .imageEntityId(Dataset.entityId(identifier), position)
      .show
    ImageResourceId(id.replace(identifier.show, Gen.uuid.generateOne.toString))
  }
}
