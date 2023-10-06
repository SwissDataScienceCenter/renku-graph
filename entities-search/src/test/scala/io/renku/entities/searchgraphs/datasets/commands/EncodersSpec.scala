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

package io.renku.entities.searchgraphs.datasets
package commands

import io.renku.entities.searchgraphs.datasets.Generators._
import io.renku.entities.searchgraphs.datasets.{DatasetSearchInfo, DatasetSearchInfoOntology, LinkOntology}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.GraphModelGenerators.{datasetTopmostSameAs, imageUris}
import io.renku.graph.model.Schemas.{rdf, renku, schema}
import io.renku.graph.model.datasets
import io.renku.graph.model.images.{Image, ImagePosition, ImageResourceId}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EncodersSpec extends AnyWordSpec with should.Matchers {

  import io.renku.entities.searchgraphs.datasets.commands.Encoders._

  "imageEncoder" should {

    "turn an Image object into a Set of relevant Quads" in {

      val image = {
        for {
          id       <- entityIds.toGeneratorOf(id => ImageResourceId(id.toString))
          uri      <- imageUris
          position <- positiveInts().toGeneratorOf(p => ImagePosition(p.value))
        } yield Image(id, uri, position)
      }.generateOne

      image.asQuads shouldBe Set(
        DatasetsQuad(image.resourceId, rdf / "type", schema / "ImageObject"),
        DatasetsQuad(image.resourceId, Image.Ontology.contentUrlProperty.id, image.uri.asObject),
        DatasetsQuad(image.resourceId, Image.Ontology.positionProperty.id, image.position.asObject)
      )
    }
  }

  "linkEncoder" should {

    "turn a OriginalDataset object into a Set of relevant Quads" in {

      val link = originalDatasetLinkObjectsGen(datasetTopmostSameAs.generateOne).generateOne

      link.asQuads shouldBe Set(
        DatasetsQuad(link.resourceId, rdf / "type", renku / "DatasetProjectLink"),
        DatasetsQuad(link.resourceId, rdf / "type", renku / "DatasetOriginalProjectLink"),
        DatasetsQuad(link.resourceId, LinkOntology.project, link.projectId.asEntityId),
        DatasetsQuad(link.resourceId, LinkOntology.dataset, link.datasetId.asEntityId)
      )
    }

    "turn a ImportedDataset object into a Set of relevant Quads" in {

      val link = importedDatasetLinkObjectsGen(datasetTopmostSameAs.generateOne).generateOne

      link.asQuads shouldBe Set(
        DatasetsQuad(link.resourceId, rdf / "type", renku / "DatasetProjectLink"),
        DatasetsQuad(link.resourceId, LinkOntology.project, link.projectId.asEntityId),
        DatasetsQuad(link.resourceId, LinkOntology.dataset, link.datasetId.asEntityId)
      )
    }
  }

  "searchInfoEncoder" should {

    "turn a SearchInfo object into a Set of relevant Quads" in {

      val searchInfo = datasetSearchInfoObjects.generateOne

      searchInfo.asQuads shouldBe Set(
        DatasetsQuad(searchInfo.topmostSameAs, rdf / "type", renku / "DiscoverableDataset"),
        DatasetsQuad(searchInfo.topmostSameAs, DatasetSearchInfoOntology.nameProperty.id, searchInfo.name.asObject),
        DatasetsQuad(searchInfo.topmostSameAs, DatasetSearchInfoOntology.slugProperty.id, searchInfo.slug.asObject),
        DatasetsQuad(searchInfo.topmostSameAs,
                     DatasetSearchInfoOntology.visibilityProperty.id,
                     searchInfo.visibility.asObject
        ),
        createdOrPublishedToQuad(searchInfo.topmostSameAs)(searchInfo.createdOrPublished)
      ) ++
        maybeDateModifiedToQuad(searchInfo.topmostSameAs)(searchInfo.maybeDateModified) ++
        creatorsToQuads(searchInfo) ++
        keywordsToQuads(searchInfo) ++
        maybeDescToQuad(searchInfo.topmostSameAs)(searchInfo.maybeDescription) ++
        imagesToQuads(searchInfo) ++
        linksToQuads(searchInfo)
    }
  }

  private def createdOrPublishedToQuad(topmostSameAs: datasets.TopmostSameAs): datasets.CreatedOrPublished => Quad = {
    case d: datasets.DateCreated =>
      DatasetsQuad(topmostSameAs, DatasetSearchInfoOntology.dateCreatedProperty.id, d.asObject)
    case d: datasets.DatePublished =>
      DatasetsQuad(topmostSameAs, DatasetSearchInfoOntology.datePublishedProperty.id, d.asObject)
  }

  private def maybeDateModifiedToQuad(
      topmostSameAs: datasets.TopmostSameAs
  ): Option[datasets.DateModified] => Set[Quad] = {
    case Some(d) => Set(DatasetsQuad(topmostSameAs, DatasetSearchInfoOntology.dateModifiedProperty.id, d.asObject))
    case None    => Set.empty
  }

  private def creatorsToQuads(searchInfo: DatasetSearchInfo): Set[Quad] =
    searchInfo.creators
      .map(creatorId =>
        DatasetsQuad(searchInfo.topmostSameAs, DatasetSearchInfoOntology.creatorProperty, creatorId.asEntityId)
      )
      .toList
      .toSet

  private def keywordsToQuads(searchInfo: DatasetSearchInfo): Set[Quad] =
    searchInfo.keywords
      .map(k => DatasetsQuad(searchInfo.topmostSameAs, DatasetSearchInfoOntology.keywordsProperty.id, k.asObject))
      .toSet

  private def maybeDescToQuad(topmostSameAs: datasets.TopmostSameAs): Option[datasets.Description] => Set[Quad] = {
    case Some(d) =>
      Set(DatasetsQuad(topmostSameAs, DatasetSearchInfoOntology.descriptionProperty.id, d.asObject))
    case None =>
      Set.empty
  }

  private def imagesToQuads(searchInfo: DatasetSearchInfo): Set[Quad] =
    searchInfo.images
      .map(i =>
        i.asQuads + DatasetsQuad(searchInfo.topmostSameAs,
                                 DatasetSearchInfoOntology.imageProperty,
                                 i.resourceId.asEntityId
        )
      )
      .toSet
      .flatten

  private def linksToQuads(searchInfo: DatasetSearchInfo): Set[Quad] =
    searchInfo.links
      .map(l =>
        l.asQuads + DatasetsQuad(searchInfo.topmostSameAs,
                                 DatasetSearchInfoOntology.linkProperty,
                                 l.resourceId.asEntityId
        )
      )
      .toList
      .toSet
      .flatten
}
