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

import Generators._
import io.renku.entities.searchgraphs.SearchInfo.DateModified
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.GraphModelGenerators.{datasetTopmostSameAs, imageUris}
import io.renku.graph.model.Schemas.{rdf, renku, schema}
import io.renku.graph.model.datasets
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.{Image, ImagePosition, ImageResourceId}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EncodersSpec extends AnyWordSpec with should.Matchers {

  import Encoders._

  "personInfoEncoder" should {

    "turn a PersonInfo object into a Set of relevant Quads" in {

      val personInfo = personInfos.generateOne

      personInfo.asQuads shouldBe Set(
        DatasetsQuad(personInfo.resourceId, rdf / "type", Person.Ontology.typeClass.id),
        DatasetsQuad(personInfo.resourceId, Person.Ontology.nameProperty.id, personInfo.name.asObject)
      )
    }
  }

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
        DatasetsQuad(link.resourceId, LinkOntology.projectId, link.projectId.asEntityId),
        DatasetsQuad(link.resourceId, LinkOntology.datasetId, link.datasetId.asEntityId),
        DatasetsQuad(link.resourceId, LinkOntology.visibilityProperty.id, link.visibility.asObject)
      )
    }

    "turn a ImportedDataset object into a Set of relevant Quads" in {

      val link = importedDatasetLinkObjectsGen(datasetTopmostSameAs.generateOne).generateOne

      link.asQuads shouldBe Set(
        DatasetsQuad(link.resourceId, rdf / "type", renku / "DatasetProjectLink"),
        DatasetsQuad(link.resourceId, LinkOntology.projectId, link.projectId.asEntityId),
        DatasetsQuad(link.resourceId, LinkOntology.datasetId, link.datasetId.asEntityId),
        DatasetsQuad(link.resourceId, LinkOntology.visibilityProperty.id, link.visibility.asObject)
      )
    }
  }

  "searchInfoEncoder" should {

    "turn a SearchInfo object into a Set of relevant Quads" in {

      val searchInfo = searchInfoObjectsGen.generateOne

      searchInfo.asQuads shouldBe Set(
        DatasetsQuad(searchInfo.topmostSameAs, rdf / "type", renku / "DiscoverableDataset"),
        DatasetsQuad(searchInfo.topmostSameAs, SearchInfoOntology.nameProperty.id, searchInfo.name.asObject),
        DatasetsQuad(searchInfo.topmostSameAs,
                     SearchInfoOntology.visibilityProperty.id,
                     searchInfo.visibility.asObject
        ),
        dateOriginalToQuad(searchInfo.topmostSameAs)(searchInfo.dateOriginal)
      ) ++
        maybeDateModifiedToQuad(searchInfo.topmostSameAs)(searchInfo.maybeDateModified) ++
        creatorsToQuads(searchInfo) ++
        keywordsToQuads(searchInfo) ++
        maybeDescToQuad(searchInfo.topmostSameAs)(searchInfo.maybeDescription) ++
        imagesToQuads(searchInfo) ++
        linksToQuads(searchInfo)
    }
  }

  private def dateOriginalToQuad(topmostSameAs: datasets.TopmostSameAs): datasets.Date => Quad = {
    case d: datasets.DateCreated =>
      DatasetsQuad(topmostSameAs, SearchInfoOntology.dateCreatedProperty.id, d.asObject)
    case d: datasets.DatePublished =>
      DatasetsQuad(topmostSameAs, SearchInfoOntology.datePublishedProperty.id, d.asObject)
  }

  private def maybeDateModifiedToQuad(topmostSameAs: datasets.TopmostSameAs): Option[DateModified] => Set[Quad] = {
    case Some(d) =>
      Set(DatasetsQuad(topmostSameAs, SearchInfoOntology.dateModifiedProperty.id, d.asObject))
    case None =>
      Set.empty
  }

  private def creatorsToQuads(searchInfo: SearchInfo): Set[Quad] =
    searchInfo.creators
      .map(pi =>
        pi.asQuads + DatasetsQuad(searchInfo.topmostSameAs,
                                  SearchInfoOntology.creatorProperty,
                                  pi.resourceId.asEntityId
        )
      )
      .toList
      .toSet
      .flatten

  private def keywordsToQuads(searchInfo: SearchInfo): Set[Quad] =
    searchInfo.keywords
      .map(k => DatasetsQuad(searchInfo.topmostSameAs, SearchInfoOntology.keywordsProperty.id, k.asObject))
      .toSet

  private def maybeDescToQuad(topmostSameAs: datasets.TopmostSameAs): Option[datasets.Description] => Set[Quad] = {
    case Some(d) =>
      Set(DatasetsQuad(topmostSameAs, SearchInfoOntology.descriptionProperty.id, d.asObject))
    case None =>
      Set.empty
  }

  private def imagesToQuads(searchInfo: SearchInfo): Set[Quad] =
    searchInfo.images
      .map(i =>
        i.asQuads + DatasetsQuad(searchInfo.topmostSameAs, SearchInfoOntology.imageProperty, i.resourceId.asEntityId)
      )
      .toSet
      .flatten

  private def linksToQuads(searchInfo: SearchInfo): Set[Quad] =
    searchInfo.links
      .map(l =>
        l.asQuads + DatasetsQuad(searchInfo.topmostSameAs, SearchInfoOntology.linkProperty, l.resourceId.asEntityId)
      )
      .toList
      .toSet
      .flatten
}
