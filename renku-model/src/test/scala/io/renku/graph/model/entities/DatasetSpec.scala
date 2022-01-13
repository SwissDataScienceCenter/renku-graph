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

package io.renku.graph.model.entities

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.literal._
import io.circe.{DecodingFailure, Json}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{timestamps, timestampsNotInTheFuture}
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.entities.Dataset.Provenance.{ImportedInternalAncestorExternal, ImportedInternalAncestorInternal}
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.DatasetGenFactory
import io.renku.jsonld.JsonLD
import io.renku.jsonld.parser._
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatasetSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "decode" should {

    "turn JsonLD Dataset entity into the Dataset object" in {
      forAll(datasetEntities(provenanceNonModified).decoupledFromProject) { dataset =>
        JsonLD
          .arr(dataset.asJsonLD :: dataset.publicationEvents.map(_.asJsonLD): _*)
          .flatten
          .fold(throw _, identity)
          .cursor
          .as[List[entities.Dataset[entities.Dataset.Provenance]]] shouldBe List(
          dataset.to[entities.Dataset[entities.Dataset.Provenance]]
        ).asRight
      }
    }

    forAll {
      Table(
        "DS generator"                                                -> "DS type",
        datasetEntities(provenanceImportedExternal)                   -> "Imported External",
        datasetEntities(provenanceImportedInternalAncestorInternal()) -> "Imported Internal Ancestor External",
        datasetEntities(provenanceImportedInternalAncestorExternal)   -> "Imported Internal Ancestor Internal"
      )
    } { case (dsGen: DatasetGenFactory[Dataset.Provenance], dsType: String) =>
      s"fail if originalIdentifier on an $dsType dataset is different than its identifier" in {
        val dataset = dsGen.decoupledFromProject.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]

        val illegalInitialVersion = datasetInitialVersions.generateOne

        val Left(error) = parse {
          dataset.asJsonLD.toJson
            .deepMerge(
              Json.obj((renku / "originalIdentifier").show -> json"""{"@value": ${illegalInitialVersion.value}}""")
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
    }

    "treat DS as Internal if there's neither sameAs nor derivedFrom but only originalIdentifier different than this DS identifier" in {
      val dataset = datasetEntities(provenanceInternal).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Internal]]

      val datasetJson = parse {
        dataset.asJsonLD.toJson
          .deepMerge(
            Json.obj(
              (renku / "originalIdentifier").show -> json"""{"@value": ${datasetInitialVersions.generateOne.show}}"""
            )
          )
      }.fold(throw _, identity)
        .flatten
        .fold(throw _, identity)

      datasetJson.cursor.as[List[entities.Dataset[entities.Dataset.Provenance]]] shouldBe List(
        dataset.copy(publicationEvents = Nil)
      ).asRight
    }

    "fail if dataset parts are older than the internal or imported external dataset" in {
      List(
        datasetEntities(provenanceInternal).decoupledFromProject,
        datasetEntities(provenanceImportedExternal).decoupledFromProject,
        datasetEntities(provenanceImportedInternalAncestorExternal).decoupledFromProject
      ) foreach { datasetGen =>
        val dataset = datasetGen.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]
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
        ds.copy(provenance = ds.provenance.copy(initialVersion = datasetInitialVersions.generateOne))
      }

      assume(dataset.identification.identifier.value != dataset.provenance.initialVersion.value)

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
