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

package io.renku.graph.model.entities

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.literal._
import io.circe.{DecodingFailure, Json}
import io.renku.cli.model.CliDataset
import io.renku.cli.model.diffx.CliDiffInstances.cliDatasetDiff
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model._
import GraphModelGenerators.graphClasses
import io.renku.graph.model.cli.CliGenerators
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.entities.Dataset.Provenance.{ImportedInternalAncestorExternal, ImportedInternalAncestorInternal}
import io.renku.graph.model.images.Image
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.DatasetGenFactory
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.jsonld.parser._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with DiffInstances {

  "decode" should {
    implicit val graph: GraphClass = GraphClass.Default

    "turn JsonLD Dataset entity into the Dataset object" in {
      forAll(CliGenerators.datasetGen()) { dataset =>
        JsonLD
          .arr(dataset.asJsonLD)
          .flatten
          .fold(throw _, identity)
          .cursor
          .as[List[CliDataset]] shouldMatchToRight List(dataset)
      }
    }

    "fail if originalIdentifier on an Imported External dataset is different than its identifier" in {

      val dataset = datasetEntities(provenanceImportedExternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]

      val Left(error) = parse {
        dataset.asJsonLD.toJson
          .deepMerge(
            Json.obj(
              (renku / "originalIdentifier").show -> json"""{"@value": ${datasetOriginalIdentifiers.generateOne.show}}"""
            )
          )
      }.fold(throw _, identity)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Dataset[entities.Dataset.Provenance]]]

      error shouldBe a[DecodingFailure]
      error.getMessage should startWith(
        s"Cannot decode entity with ${dataset.resourceId}: DecodingFailure at : Invalid dataset data"
      )
    }

    forAll {
      Table(
        "DS generator"                                                -> "DS type",
        datasetEntities(provenanceImportedInternalAncestorInternal()) -> "Imported Internal Ancestor External",
        datasetEntities(provenanceImportedInternalAncestorExternal)   -> "Imported Internal Ancestor Internal"
      )
    } { case (dsGen: DatasetGenFactory[Dataset.Provenance], dsType: String) =>
      "turn JsonLD Dataset entity into the Dataset object " +
        s"when originalIdentifier on an $dsType dataset is different than its identifier" in {
          val testDataset     = dsGen.decoupledFromProject.generateOne
          val prodDataset     = testDataset.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          val otherOriginalId = datasetOriginalIdentifiers.generateOne

          val dsJsonLD = parse {
            testDataset
              .to[CliDataset]
              .asJsonLD
              .toJson
              .deepMerge(
                Json.obj(
                  (renku / "originalIdentifier").show -> json"""{"@value": ${otherOriginalId.show}}"""
                )
              )
          }.fold(throw _, identity)

          val Right(actualDS :: Nil) = JsonLD
            .arr(dsJsonLD :: prodDataset.publicationEvents.map(_.asJsonLD): _*)
            .flatten
            .fold(throw _, identity)
            .cursor
            .as[List[entities.Dataset[entities.Dataset.Provenance]]]

          actualDS shouldMatchTo prodDataset
            .copy(
              provenance = prodDataset.provenance match {
                case p: entities.Dataset.Provenance.ImportedInternalAncestorExternal =>
                  p.copy(originalIdentifier = otherOriginalId)
                case p: entities.Dataset.Provenance.ImportedInternalAncestorInternal =>
                  p.copy(originalIdentifier = otherOriginalId)
                case p => fail(s"DS with provenance ${p.getClass} not expected here")
              }
            )
        }
    }

    "treat DS with originalIdentifier but no derivedFrom as Internal -" +
      "drop originalIdentifier and move its dateCreated to the oldest parts' date" in {
        val dataset = {
          val ds    = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
          val part1 = datasetPartEntities(ds.provenance.date.instant).generateOne
          val part2 = datasetPartEntities(ds.provenance.date.instant).generateOne
            .copy(dateCreated = timestamps(max = ds.provenance.date.instant).generateAs(datasets.DateCreated))
          ds.copy(parts = List(part1, part2))
        }.to[entities.Dataset[entities.Dataset.Provenance.Internal]]

        assert(dataset.parts.exists(_.dateCreated.instant isBefore dataset.provenance.date.instant))

        val datasetJson = parse {
          dataset.asJsonLD.toJson
            .deepMerge(
              Json.obj(
                (renku / "originalIdentifier").show -> json"""{"@value": ${datasetOriginalIdentifiers.generateOne.show}}"""
              )
            )
        }.flatMap(_.flatten).fold(throw _, identity)

        datasetJson.cursor.as[List[entities.Dataset[entities.Dataset.Provenance]]] shouldBe List(
          dataset.copy(
            publicationEvents = Nil,
            provenance = dataset.provenance.copy(date = dataset.parts.map(_.dateCreated).min)
          )
        ).asRight
      }

    "treat DS with originalIdentifier but no derivedFrom as Internal -" +
      "drop originalIdentifier and keep its dateCreated if parts' dates are younger than the DS" in {
        val dataset = {
          val ds    = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
          val part1 = datasetPartEntities(ds.provenance.date.instant).generateOne
          val part2 = datasetPartEntities(ds.provenance.date.instant).generateOne
          ds.copy(parts = List(part1, part2))
        }.to[entities.Dataset[entities.Dataset.Provenance.Internal]]

        assert((dataset.provenance.date.instant compareTo dataset.parts.map(_.dateCreated).min.instant) <= 0)

        val datasetJson = parse {
          dataset.asJsonLD.toJson
            .deepMerge(
              Json.obj(
                (renku / "originalIdentifier").show -> json"""{"@value": ${datasetOriginalIdentifiers.generateOne.show}}"""
              )
            )
        }.flatMap(_.flatten).fold(throw _, identity)

        datasetJson.cursor.as[List[entities.Dataset[entities.Dataset.Provenance]]] shouldBe List(
          dataset.copy(publicationEvents = Nil)
        ).asRight
      }

    forAll {
      Table(
        "DS generator"                                              -> "DS type",
        datasetEntities(provenanceInternal)                         -> "Internal",
        datasetEntities(provenanceImportedExternal)                 -> "Imported External",
        datasetEntities(provenanceImportedInternalAncestorExternal) -> "Imported Internal Ancestor External"
      )
    } { case (dsGen: DatasetGenFactory[Dataset.Provenance], dsType: String) =>
      s"fail if dataset parts are older than the dataset - case of an $dsType DS" in {
        val dataset = dsGen.decoupledFromProject.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]
        val invalidPart = updatePartDateAfter(
          datasetPartEntities(timestampsNotInTheFuture.generateOne).generateOne.to[entities.DatasetPart]
        )(dataset.provenance)
        val invalidDataset = dataset.copy(parts = invalidPart :: dataset.parts)

        val Left(error) = invalidDataset.asJsonLD.flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Dataset[entities.Dataset.Provenance]]]

        error shouldBe a[DecodingFailure]
        error.getMessage should endWith(
          s"Dataset ${invalidDataset.identification.identifier} " +
            s"Part ${invalidPart.entity.location} startTime ${invalidPart.dateCreated} is older than Dataset ${invalidDataset.provenance.date.instant}"
        )
      }
    }

    "succeed if dataset parts are older than the modified or imported internal dataset" in {
      List(
        datasetAndModificationEntities(provenanceNonModified).map(_._2),
        datasetEntities(provenanceImportedInternalAncestorInternal()).decoupledFromProject
      ) foreach { datasetGen =>
        val dataset = datasetGen.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]
        val olderPart = updatePartDateAfter(
          datasetPartEntities(timestampsNotInTheFuture.generateOne).generateOne.to[entities.DatasetPart]
        )(dataset.provenance)
        val validDataset = dataset.copy(parts = olderPart :: dataset.parts)

        JsonLD
          .arr(validDataset.asJsonLD :: validDataset.publicationEvents.map(_.asJsonLD): _*)
          .flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Dataset[entities.Dataset.Provenance]]] shouldBe List(validDataset).asRight
      }
    }

    "succeed if originalIdentifier on a modified dataset is different than its identifier" in {
      val dataset = {
        val ds = datasetAndModificationEntities(provenanceNonModified)
          .map(_._2)
          .generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
        ds.copy(provenance = ds.provenance.copy(originalIdentifier = datasetOriginalIdentifiers.generateOne))
      }

      assume(dataset.identification.identifier.value != dataset.provenance.originalIdentifier.value)

      dataset.asJsonLD.flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Dataset[entities.Dataset.Provenance]]] shouldBe List(
        dataset.copy(publicationEvents = Nil)
      ).asRight
    }

    "fail if invalidationTime is older than the dataset" in {
      val dataset = datasetAndModificationEntities(provenanceInternal).generateOne._2
        .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
      val invalidationTime = timestamps(max = dataset.provenance.date.instant).generateAs(InvalidationTime)
      val invalidatedDataset = dataset
        .copy(provenance = dataset.provenance.copy(maybeInvalidationTime = invalidationTime.some))

      val Left(error) = invalidatedDataset.asJsonLD.flatten
        .fold(fail(_), identity)
        .cursor
        .as[List[entities.Dataset[entities.Dataset.Provenance]]]

      error shouldBe a[DecodingFailure]
      error.getMessage should endWith(
        s"Dataset ${invalidatedDataset.identification.identifier} " +
          s"invalidationTime $invalidationTime is older than Dataset ${invalidatedDataset.provenance.date}"
      )
    }

    "skip publicationEvents that do not belong to a different dataset" in {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]
        .copy(publicationEvents = Nil)

      val otherDatasetPublicationEvent =
        publicationEventFactories(dataset.provenance.date.instant)
          .generateOne(datasetEntities(provenanceNonModified).decoupledFromProject.generateOne)
          .to[entities.PublicationEvent]

      JsonLD
        .arr(dataset.asJsonLD, otherDatasetPublicationEvent.asJsonLD)
        .flatten
        .fold(throw _, identity)
        .cursor
        .as[List[entities.Dataset[entities.Dataset.Provenance]]] shouldBe List(dataset).asRight
    }

    forAll {
      Table(
        "DS type"                          -> "ds",
        "Internal"                         -> datasetEntities(provenanceInternal),
        "ImportedExternal"                 -> datasetEntities(provenanceImportedExternal),
        "ImportedInternalAncestorExternal" -> datasetEntities(provenanceImportedInternalAncestorExternal),
        "ImportedInternalAncestorInternal" -> datasetEntities(provenanceImportedInternalAncestorInternal()),
        "Modified" -> datasetEntities(provenanceInternal).decoupledFromProject.generateOne.createModification()
      )
    } { (dsType, dsGenerator) =>
      s"fail if no creators - case $dsType DS" in {
        val dataset = dsGenerator.decoupledFromProject.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]

        val Left(error) = parse {
          Json.arr(
            dataset.asJsonLD.toJson.hcursor
              .downField((schema / "creator").show)
              .delete
              .top
              .getOrElse(fail("No json after removing creator"))
          )
        }.fold(throw _, identity).cursor.as[List[entities.Dataset[entities.Dataset.Provenance]]]

        error            shouldBe a[DecodingFailure]
        error.getMessage shouldBe s"No creators on dataset with id: ${dataset.identification.identifier}"
      }
    }
  }

  "from" should {

    "return a failure when initializing with a PublicationEvent belonging to another dataset" in {
      val dataset = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      val otherDatasetPublicationEvent =
        publicationEventFactories(dataset.provenance.date.instant)
          .generateOne(datasetEntities(provenanceNonModified).decoupledFromProject.generateOne)
          .to[entities.PublicationEvent]

      val errors = entities.Dataset.from(
        dataset.identification,
        dataset.provenance,
        dataset.additionalInfo,
        dataset.parts,
        List(otherDatasetPublicationEvent)
      )

      errors.isInvalid shouldBe true
      errors.swap.fold(_ => fail("Errors expected"), identity) shouldBe NonEmptyList.one {
        s"PublicationEvent ${otherDatasetPublicationEvent.resourceId} refers to ${otherDatasetPublicationEvent.about} " +
          s"that points to ${otherDatasetPublicationEvent.datasetResourceId} but should be pointing to ${dataset.resourceId}"
      }
    }
  }

  "encode" should {

    implicit val jsonLDEncoder: JsonLDEncoder[Image] = JsonLDEncoder.instance { case Image(resourceId, uri, position) =>
      JsonLD.entity(
        resourceId.asEntityId,
        EntityTypes of schema / "ImageObject",
        schema / "contentUrl" -> uri.asJsonLD,
        schema / "position"   -> position.asJsonLD
      )
    }

    "produce JsonLD with all the relevant properties and only links to Person entities " +
      "if encoding requested for the Project Graph" in {
        implicit val graph: GraphClass = GraphClass.Project

        val ds = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.Internal]]

        ds.asJsonLD shouldBe JsonLD
          .entity(
            ds.resourceId.asEntityId,
            entities.Dataset.entityTypes,
            schema / "identifier"   -> ds.identification.identifier.asJsonLD,
            schema / "name"         -> ds.identification.title.asJsonLD,
            renku / "slug"          -> ds.identification.name.asJsonLD,
            schema / "dateCreated"  -> ds.provenance.date.asJsonLD,
            schema / "creator"      -> ds.provenance.creators.map(_.resourceId.asEntityId.asJsonLD).toList.asJsonLD,
            renku / "topmostSameAs" -> ds.provenance.topmostSameAs.asJsonLD,
            renku / "topmostDerivedFrom" -> ds.provenance.topmostDerivedFrom.asJsonLD,
            renku / "originalIdentifier" -> ds.provenance.originalIdentifier.asJsonLD,
            schema / "description"       -> ds.additionalInfo.maybeDescription.asJsonLD,
            schema / "keywords"          -> ds.additionalInfo.keywords.asJsonLD,
            schema / "image"             -> ds.additionalInfo.images.asJsonLD,
            schema / "license"           -> ds.additionalInfo.maybeLicense.asJsonLD,
            schema / "version"           -> ds.additionalInfo.maybeVersion.asJsonLD,
            schema / "hasPart"           -> ds.parts.asJsonLD
          )
      }

    "produce JsonLD with all the relevant properties entities " +
      "if encoding requested for the Default Graph" in {
        implicit val graph: GraphClass = GraphClass.Default

        val ds = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.Internal]]

        ds.asJsonLD shouldBe JsonLD
          .entity(
            ds.resourceId.asEntityId,
            entities.Dataset.entityTypes,
            schema / "identifier"        -> ds.identification.identifier.asJsonLD,
            schema / "name"              -> ds.identification.title.asJsonLD,
            renku / "slug"               -> ds.identification.name.asJsonLD,
            schema / "dateCreated"       -> ds.provenance.date.asJsonLD,
            schema / "creator"           -> ds.provenance.creators.toList.asJsonLD,
            renku / "topmostSameAs"      -> ds.provenance.topmostSameAs.asJsonLD,
            renku / "topmostDerivedFrom" -> ds.provenance.topmostDerivedFrom.asJsonLD,
            renku / "originalIdentifier" -> ds.provenance.originalIdentifier.asJsonLD,
            schema / "description"       -> ds.additionalInfo.maybeDescription.asJsonLD,
            schema / "keywords"          -> ds.additionalInfo.keywords.asJsonLD,
            schema / "image"             -> ds.additionalInfo.images.asJsonLD,
            schema / "license"           -> ds.additionalInfo.maybeLicense.asJsonLD,
            schema / "version"           -> ds.additionalInfo.maybeVersion.asJsonLD,
            schema / "hasPart"           -> ds.parts.asJsonLD
          )
      }
  }

  "entityFunctions.findAllPersons" should {

    "return all Dataset's authors" in {

      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      EntityFunctions[entities.Dataset[entities.Dataset.Provenance]].findAllPersons(ds) shouldBe
        ds.provenance.creators.toList.toSet
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {

      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      implicit val graph: GraphClass = graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Dataset[entities.Dataset.Provenance]].encoder(graph)

      ds.asJsonLD(functionsEncoder) shouldBe ds.asJsonLD
    }
  }

  "update" should {

    "replace the topmostSameAs " in {
      List(
        datasetEntities(
          provenanceImportedInternalAncestorExternal.asInstanceOf[ProvenanceGen[Dataset.Provenance.ImportedInternal]]
        ).decoupledFromProject.generateOne.to[entities.Dataset[Provenance.ImportedInternal]],
        datasetEntities(
          provenanceImportedInternalAncestorInternal().asInstanceOf[ProvenanceGen[Dataset.Provenance.ImportedInternal]]
        ).decoupledFromProject.generateOne.to[entities.Dataset[Provenance.ImportedInternal]]
      ) foreach { dataset =>
        val newTopmostSameAs = datasetTopmostSameAs.generateOne
        val provenance = dataset.provenance match {
          case p: ImportedInternalAncestorExternal => p.copy(topmostSameAs = newTopmostSameAs)
          case p: ImportedInternalAncestorInternal => p.copy(topmostSameAs = newTopmostSameAs)
          case _ => fail("Cannot update topmostSameAs")
        }
        dataset.update(newTopmostSameAs) shouldBe dataset.copy(provenance = provenance)
      }
    }

    "replace the topmostDerivedFrom " in {
      val dataset = datasetAndModificationEntities(provenanceInternal).generateOne._2
        .to[entities.Dataset[Provenance.Modified]]

      val newTopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne

      dataset.update(newTopmostDerivedFrom) shouldBe dataset.copy(
        provenance = dataset.provenance.copy(topmostDerivedFrom = newTopmostDerivedFrom)
      )
    }
  }

  private def updatePartDateAfter(
      part: entities.DatasetPart
  ): entities.Dataset.Provenance => entities.DatasetPart = {
    case p: Provenance.Modified =>
      part.copy(dateCreated = timestamps(max = p.date.value).generateAs[datasets.DateCreated])
    case p: Provenance.Internal =>
      part.copy(dateCreated = timestamps(max = p.date.value).generateAs[datasets.DateCreated])
    case p: Provenance.ImportedExternal =>
      part.copy(dateCreated = timestamps(max = p.date.instant).generateAs[datasets.DateCreated])
    case p: Provenance.ImportedInternalAncestorExternal =>
      part.copy(dateCreated = timestamps(max = p.date.instant).generateAs[datasets.DateCreated])
    case p: Provenance.ImportedInternalAncestorInternal =>
      part.copy(dateCreated = timestamps(max = p.date.value).generateAs[datasets.DateCreated])
  }
}
