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
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model._
import GraphModelGenerators.graphClasses
import io.renku.cli.model.CliDataset
import io.renku.cli.model.generators.DatasetFileGenerators.datasetFileGen
import io.renku.graph.model.entities.Dataset.Provenance
import io.renku.graph.model.entities.Dataset.Provenance.{ImportedInternalAncestorExternal, ImportedInternalAncestorInternal}
import io.renku.graph.model.images.Image
import io.renku.graph.model.testentities._
import io.renku.graph.model.testentities.generators.EntitiesGenerators.DatasetGenFactory
import io.renku.graph.model.tools.AdditionalMatchers
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.Instant

class DatasetSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with AdditionalMatchers
    with EitherValues
    with DiffInstances {

  "fromCli" should {

    "turn CliDataset entity into the Dataset object" in {

      forAll(datasetEntities(provenanceNonModified(cliShapedPersons)).decoupledFromProject) { testDs =>
        val modelDs = testDs.to[entities.Dataset[entities.Dataset.Provenance]]
        val cliDs   = testDs.to[CliDataset]

        entities.Dataset.fromCli(cliDs) shouldMatchToValid modelDs
      }
    }

    "fail if originalIdentifier on an Imported External dataset is different than its identifier" in {

      val modelDs = datasetEntities(
        provenanceImportedExternal(creatorsGen = cliShapedPersons)
      ).decoupledFromProject.generateOne

      val cliDs = modelDs.to[CliDataset].copy(originalIdentifier = datasetOriginalIdentifiers.generateSome)

      val result = entities.Dataset.fromCli(cliDs)
      result should beInvalidWithMessageIncluding("Invalid dataset data")
    }

    forAll {
      Table(
        "DS generator" -> "DS type",
        datasetEntities(
          provenanceImportedInternalAncestorInternal(creatorsGen = cliShapedPersons)
        ) -> "Imported Internal Ancestor External",
        datasetEntities(
          provenanceImportedInternalAncestorExternal(creatorsGen = cliShapedPersons)
        ) -> "Imported Internal Ancestor Internal"
      )
    } { case (dsGen: DatasetGenFactory[Dataset.Provenance], dsType: String) =>
      "turn CliDataset entity into the Dataset object " +
        s"when originalIdentifier on an $dsType dataset is different than its identifier" in {

          val testDs          = dsGen.decoupledFromProject.generateOne
          val modelDs         = testDs.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          val otherOriginalId = datasetOriginalIdentifiers.generateOne
          val cliDs           = testDs.to[CliDataset].copy(originalIdentifier = otherOriginalId.some)

          val result = entities.Dataset.fromCli(cliDs)

          result shouldMatchToValid modelDs.copy(provenance = modelDs.provenance match {
            case p: entities.Dataset.Provenance.ImportedInternalAncestorExternal =>
              p.copy(originalIdentifier = otherOriginalId)
            case p: entities.Dataset.Provenance.ImportedInternalAncestorInternal =>
              p.copy(originalIdentifier = otherOriginalId)
            case p => fail(s"DS with provenance ${p.getClass} not expected here")
          })
        }
    }

    "treat DS with originalIdentifier but no derivedFrom as Internal -" +
      "drop originalIdentifier and move its dateCreated to the oldest parts' date" in {

        val testDs = {
          val ds    = datasetEntities(provenanceInternal(cliShapedPersons)).decoupledFromProject.generateOne
          val part1 = datasetPartEntities(ds.provenance.date.instant).generateOne
          val part2 = datasetPartEntities(ds.provenance.date.instant).generateOne
            .copy(dateCreated = timestamps(max = ds.provenance.date.instant).generateAs(datasets.DateCreated))
          ds.copy(parts = List(part1, part2))
        }
        val modelDs = testDs.to[entities.Dataset[entities.Dataset.Provenance.Internal]]
        val cliDs   = testDs.to[CliDataset].copy(originalIdentifier = datasetOriginalIdentifiers.generateSome)

        assert(modelDs.parts.exists(_.dateCreated.instant isBefore modelDs.provenance.date.instant))

        entities.Dataset.fromCli(cliDs) shouldMatchToValid modelDs.copy(provenance =
          modelDs.provenance.copy(date = modelDs.parts.map(_.dateCreated).min)
        )
      }

    "treat DS with originalIdentifier but no derivedFrom as Internal -" +
      "drop originalIdentifier and keep its dateCreated if parts' dates are younger than the DS" in {

        val testDs = {
          val ds    = datasetEntities(provenanceInternal(cliShapedPersons)).decoupledFromProject.generateOne
          val part1 = datasetPartEntities(ds.provenance.date.instant).generateOne
          val part2 = datasetPartEntities(ds.provenance.date.instant).generateOne
          ds.copy(parts = List(part1, part2))
        }
        val modelDs = testDs.to[entities.Dataset[entities.Dataset.Provenance.Internal]]

        assert((modelDs.provenance.date.instant compareTo modelDs.parts.map(_.dateCreated).min.instant) <= 0)

        val cliDs = testDs.to[CliDataset].copy(originalIdentifier = datasetOriginalIdentifiers.generateSome)

        entities.Dataset.fromCli(cliDs) shouldMatchToValid modelDs
      }

    forAll {
      Table(
        "DS generator"                                                              -> "DS type",
        datasetEntities(provenanceInternal(cliShapedPersons))                       -> "Internal",
        datasetEntities(provenanceImportedExternal(creatorsGen = cliShapedPersons)) -> "Imported External",
        datasetEntities(
          provenanceImportedInternalAncestorExternal(cliShapedPersons)
        ) -> "Imported Internal Ancestor External"
      )
    } { case (dsGen: DatasetGenFactory[Dataset.Provenance], dsType: String) =>
      s"fail if dataset parts are older than the dataset - case of an $dsType DS" in {

        val testDs  = dsGen.decoupledFromProject.generateOne
        val modelDs = testDs.to[entities.Dataset[entities.Dataset.Provenance]]

        val invalidFile = datasetFileGen(Instant.now()).generateOne
          .copy(dateCreated = timestamps(max = modelDs.provenance.date.instant).generateAs(datasets.DateCreated),
                invalidationTime = None
          )

        val cliDs = {
          val cli = testDs.to[CliDataset]
          cli.copy(datasetFiles = invalidFile :: cli.datasetFiles)
        }

        val result = entities.Dataset.fromCli(cliDs)
        result should beInvalidWithMessageIncluding(
          s"Dataset ${modelDs.identification.identifier} " +
            s"Part ${invalidFile.entity.path} startTime ${invalidFile.dateCreated} is older than Dataset ${modelDs.provenance.date.instant}"
        )
      }
    }

    forAll {
      Table(
        "DS generator" -> "DS type",
        datasetAndModificationEntities(provenanceNonModified(cliShapedPersons),
                                       modificationCreatorGen = cliShapedPersons
        ).map(_._2) -> "Modified",
        datasetEntities(
          provenanceImportedInternalAncestorInternal(creatorsGen = cliShapedPersons)
        ).decoupledFromProject -> "Imported Ancestor Internal"
      )
    } { (dsGen, dsType) =>
      s"succeed for $dsType dataset with parts older than the dataset itself" in {

        val testDs  = dsGen.generateOne
        val modelDs = testDs.to[entities.Dataset[entities.Dataset.Provenance]]

        val olderFile = datasetFileGen(Instant.now()).generateOne
          .copy(dateCreated = timestamps(max = modelDs.provenance.date.instant).generateAs(datasets.DateCreated),
                invalidationTime = None
          )

        val cliDs = {
          val cli = testDs.to[CliDataset]
          cli.copy(datasetFiles = olderFile :: cli.datasetFiles)
        }

        val modelOlderFile = entities.DatasetPart.fromCli(olderFile).fold(err => fail(err.intercalate("; ")), identity)
        val expectedDs     = modelDs.copy(parts = modelOlderFile :: modelDs.parts)
        entities.Dataset.fromCli(cliDs) shouldMatchToValid expectedDs
      }
    }

    "skip looking into a modified dataset dateCreated" in {

      val _ -> testDs = datasetAndModificationEntities(provenanceNonModified(cliShapedPersons),
                                                       modificationCreatorGen = cliShapedPersons
      ).generateOne
      val modelDs = testDs.to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      assume(modelDs.identification.identifier.value != modelDs.provenance.originalIdentifier.value)

      val cliDs = testDs.to[CliDataset].copy(createdOrPublished = datasetCreatedOrPublished.generateOne)

      entities.Dataset.fromCli(cliDs) shouldMatchToValid modelDs
    }

    "succeed if originalIdentifier on a modified dataset is different than its identifier" in {

      val testDs = {
        val _ -> ds = datasetAndModificationEntities(provenanceNonModified(cliShapedPersons),
                                                     modificationCreatorGen = cliShapedPersons
        ).generateOne
        ds.copy(provenance = ds.provenance.copy(originalIdentifier = datasetOriginalIdentifiers.generateOne))
      }
      val modelDs = testDs.to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      assume(modelDs.identification.identifier.value != modelDs.provenance.originalIdentifier.value)

      entities.Dataset.fromCli(testDs.to[CliDataset]) shouldMatchToValid modelDs
    }

    "fail if invalidationTime is older than the dataset" in {

      val testDs = datasetAndModificationEntities(provenanceInternal(cliShapedPersons),
                                                  modificationCreatorGen = cliShapedPersons
      ).generateOne._2
      val modelDs = testDs.to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      val invalidationTime = timestamps(max = modelDs.provenance.date.instant).generateAs(InvalidationTime)
      val cliDs            = testDs.to[CliDataset].copy(invalidationTime = invalidationTime.some)

      val result = entities.Dataset.fromCli(cliDs)
      result should beInvalidWithMessageIncluding(
        s"Dataset ${modelDs.identification.identifier} " +
          s"invalidationTime $invalidationTime is older than Dataset ${modelDs.provenance.date}"
      )
    }
  }

  "from" should {

    "return a failure when initializing with a PublicationEvent belonging to another dataset" in {

      val ds = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      val otherDsPublicationEvent =
        publicationEventFactories(ds.provenance.date.instant)
          .generateOne(datasetEntities(provenanceNonModified).decoupledFromProject.generateOne)
          .to[entities.PublicationEvent]

      val errors = entities.Dataset.from(
        ds.identification,
        ds.provenance,
        ds.additionalInfo,
        ds.parts,
        List(otherDsPublicationEvent)
      )

      errors.isInvalid shouldBe true
      errors.swap.fold(_ => fail("Errors expected"), identity) shouldBe NonEmptyList.one {
        s"PublicationEvent ${otherDsPublicationEvent.resourceId} refers to ${otherDsPublicationEvent.about} " +
          s"that points to ${otherDsPublicationEvent.datasetResourceId} but should be pointing to ${ds.resourceId}"
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
            schema / "name"         -> ds.identification.name.asJsonLD,
            renku / "slug"          -> ds.identification.slug.asJsonLD,
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
            schema / "name"              -> ds.identification.name.asJsonLD,
            renku / "slug"               -> ds.identification.slug.asJsonLD,
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

    "return all Dataset's creators" in {

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
      ) foreach { ds =>
        val newTopmostSameAs = datasetTopmostSameAs.generateOne
        val provenance = ds.provenance match {
          case p: ImportedInternalAncestorExternal => p.copy(topmostSameAs = newTopmostSameAs)
          case p: ImportedInternalAncestorInternal => p.copy(topmostSameAs = newTopmostSameAs)
          case _ => fail("Cannot update topmostSameAs")
        }
        ds.update(newTopmostSameAs) shouldBe ds.copy(provenance = provenance)
      }
    }

    "replace the topmostDerivedFrom " in {

      val _ -> ds =
        datasetAndModificationEntities(provenanceInternal).generateOne.map(_.to[entities.Dataset[Provenance.Modified]])

      val newTopmostDerivedFrom = datasetTopmostDerivedFroms.generateOne

      ds.update(newTopmostDerivedFrom) shouldBe ds.copy(
        provenance = ds.provenance.copy(topmostDerivedFrom = newTopmostDerivedFrom)
      )
    }
  }
}
