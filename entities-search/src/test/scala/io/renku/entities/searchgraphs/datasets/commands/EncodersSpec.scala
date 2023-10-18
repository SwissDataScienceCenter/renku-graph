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

import DatasetSearchInfoOntology._
import Generators.{datasetSearchInfoObjects, importedDatasetLinkObjectsGen, originalDatasetLinkObjectsGen}
import cats.syntax.all._
import io.renku.entities.searchgraphs.concatSeparator
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
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EncodersSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

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

      forAll(datasetSearchInfoObjects) { searchInfo =>
        searchInfo.asQuads shouldBe Set(
          DatasetsQuad(searchInfo.topmostSameAs, rdf / "type", renku / "DiscoverableDataset"),
          DatasetsQuad(searchInfo.topmostSameAs, slugProperty.id, searchInfo.name.asObject),
          DatasetsQuad(searchInfo.topmostSameAs, visibilityProperty.id, searchInfo.visibility.asObject),
          createdOrPublishedToQuad(searchInfo.topmostSameAs)(searchInfo.createdOrPublished),
          DatasetsQuad(searchInfo.topmostSameAs, creatorsNamesConcatProperty.id, creatorsNamesConcat(searchInfo)),
          DatasetsQuad(searchInfo.topmostSameAs,
                       projectsVisibilitiesConcatProperty.id,
                       projectVisibilitiesConcat(searchInfo)
          )
        ) ++
          maybeDateModifiedToQuad(searchInfo.topmostSameAs)(searchInfo.maybeDateModified) ++
          creatorsToQuads(searchInfo) ++
          keywordsToQuads(searchInfo) ++
          maybeDescToQuad(searchInfo.topmostSameAs)(searchInfo.maybeDescription) ++
          maybeKeywordsConcatToQuad(searchInfo).toSet ++
          maybeImagesConcatToQuad(searchInfo).toSet ++
          imagesToQuads(searchInfo) ++
          linksToQuads(searchInfo)
      }
    }
  }

  private def createdOrPublishedToQuad(topmostSameAs: datasets.TopmostSameAs): datasets.CreatedOrPublished => Quad = {
    case d: datasets.DateCreated =>
      DatasetsQuad(topmostSameAs, dateCreatedProperty.id, d.asObject)
    case d: datasets.DatePublished =>
      DatasetsQuad(topmostSameAs, datePublishedProperty.id, d.asObject)
  }

  private def maybeDateModifiedToQuad(
      topmostSameAs: datasets.TopmostSameAs
  ): Option[datasets.DateModified] => Set[Quad] = {
    case Some(d) => Set(DatasetsQuad(topmostSameAs, dateModifiedProperty.id, d.asObject))
    case None    => Set.empty
  }

  private def creatorsToQuads(searchInfo: DatasetSearchInfo): Set[Quad] =
    searchInfo.creators
      .map(_.resourceId)
      .map(creatorId => DatasetsQuad(searchInfo.topmostSameAs, creatorProperty, creatorId.asEntityId))
      .toList
      .toSet

  private def creatorsNamesConcat(searchInfo: DatasetSearchInfo) =
    searchInfo.creators.map(_.name.value).intercalate(concatSeparator.toString).asTripleObject

  private def keywordsToQuads(searchInfo: DatasetSearchInfo): Set[Quad] =
    searchInfo.keywords
      .map(k => DatasetsQuad(searchInfo.topmostSameAs, keywordsProperty.id, k.asObject))
      .toSet

  private def maybeKeywordsConcatToQuad(searchInfo: DatasetSearchInfo): Option[Quad] =
    searchInfo.keywords match {
      case Nil => Option.empty[Quad]
      case k =>
        DatasetsQuad(searchInfo.topmostSameAs,
                     keywordsConcatProperty.id,
                     k.mkString(concatSeparator.toString).asTripleObject
        ).some
    }

  private def maybeDescToQuad(topmostSameAs: datasets.TopmostSameAs): Option[datasets.Description] => Set[Quad] = {
    case Some(d) =>
      Set(DatasetsQuad(topmostSameAs, descriptionProperty.id, d.asObject))
    case None =>
      Set.empty
  }

  private def maybeImagesConcatToQuad(searchInfo: DatasetSearchInfo): Option[Quad] =
    searchInfo.images match {
      case Nil => Option.empty[Quad]
      case images =>
        DatasetsQuad(searchInfo.topmostSameAs,
                     imagesConcatProperty.id,
                     images.map(i => s"${i.position}:${i.uri}").mkString(concatSeparator.toString).asTripleObject
        ).some
    }

  private def imagesToQuads(searchInfo: DatasetSearchInfo): Set[Quad] =
    searchInfo.images
      .map(i => i.asQuads + DatasetsQuad(searchInfo.topmostSameAs, imageProperty, i.resourceId.asEntityId))
      .toSet
      .flatten

  private def linksToQuads(searchInfo: DatasetSearchInfo): Set[Quad] =
    searchInfo.links
      .map(l => l.asQuads + DatasetsQuad(searchInfo.topmostSameAs, linkProperty, l.resourceId.asEntityId))
      .toList
      .toSet
      .flatten

  private def projectVisibilitiesConcat(searchInfo: DatasetSearchInfo) =
    searchInfo.links
      .map(l => s"${l.projectSlug}:${l.visibility}")
      .intercalate(concatSeparator.toString)
      .asTripleObject
}
