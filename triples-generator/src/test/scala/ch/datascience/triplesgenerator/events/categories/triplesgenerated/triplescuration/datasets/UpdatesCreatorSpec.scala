/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.datasets.{ResourceId, SameAs, TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import ch.datascience.rdfstore.InMemoryRdfStore
import org.scalatest.matchers.should
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

class UpdatesCreatorSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers with TableDrivenPropertyChecks {

  "prepareUpdates" should {

    "generate queries which " +
      "updates topmostSameAs for all datasets whose topmostSameAs points to the current dataset" +
      "when the topmostSameAs on the current dataset is different than the value in KG" in {
        val dataset0AsTopmostSameAs = TopmostSameAs(datasetResourceIds.generateOne.value)

        val dataset1 = {
          val ds = datasetEntities(datasetProvenanceImportedInternal).generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          setTopmostSameAs(ds, dataset0AsTopmostSameAs)
        }
        val dataset2 = {
          val ds = datasetEntities(datasetProvenanceImportedInternal).generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          setTopmostSameAs(ds, TopmostSameAs(dataset1.resourceId.value))
        }

        loadToStore(dataset2)

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (dataset2.resourceId.value, TopmostSameAs(dataset1.resourceId.value).value.some)
        )

        UpdatesCreator.prepareUpdates(dataset1, None).runAll.unsafeRunSync()

        findDatasets.map(onlyTopmostSameAs) shouldBe Set(
          (dataset2.resourceId.value, dataset0AsTopmostSameAs.value.some)
        )
      }

    "generate queries which " +
      "updates topmostDerivedFrom for all datasets whose topmostDerivedFrom points to the current dataset" +
      "when the topmostDerivedFrom on the current dataset is different than the value in KG" in {
        val dataset0AsTopmostDerivedFrom = TopmostDerivedFrom(datasetResourceIds.generateOne.value)

        val dataset1 = {
          val ds = datasetEntities(datasetProvenanceModified).generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
          ds.copy(provenance = ds.provenance.copy(topmostDerivedFrom = dataset0AsTopmostDerivedFrom))
        }
        val dataset2 = {
          val ds = datasetEntities(datasetProvenanceModified).generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
          ds.copy(provenance = ds.provenance.copy(topmostDerivedFrom = TopmostDerivedFrom(dataset1.resourceId.value)))
        }

        loadToStore(dataset2)

        findDatasets.map(onlyTopmostDerivedFrom) shouldBe Set(
          (dataset2.resourceId.value, dataset2.provenance.topmostDerivedFrom.value.some)
        )

        UpdatesCreator.prepareUpdates(dataset1, None).runAll.unsafeRunSync()

        findDatasets.map(onlyTopmostDerivedFrom) shouldBe Set(
          (dataset2.resourceId.value, dataset0AsTopmostDerivedFrom.value.some)
        )
      }

    "generate queries for deleted dataset which, " +
      "in case of internal dataset, " +
      "find datasets which have sameAs pointing to the deleted dataset " +
      "update their sameAs to None " +
      "select one the dataset with the oldest date " +
      "and update all datasets which have topmostSameAs pointing to the deleted DS with the selected resourceId" in {
        val grandparent = datasetEntities(datasetProvenanceInternal).generateOne.copy(parts = Nil)
        val parent1     = grandparent.importTo(projectEntities(visibilityAny)(anyForksCount).generateOne)
        val child1      = parent1.importTo(projectEntities(visibilityAny)(anyForksCount).generateOne)
        val parent2 = {
          val ds = grandparent.importTo(projectEntities(visibilityAny)(anyForksCount).generateOne)
          ds.copy(provenance =
            ds.provenance.copy(date = datasetCreatedDates(parent1.provenance.date.instant).generateOne)
          )
        }
        val child2 = parent2.importTo(projectEntities(visibilityAny)(anyForksCount).generateOne)

        val entitiesGrandparent = grandparent.to[entities.Dataset[entities.Dataset.Provenance.Internal]]
        val entitiesParent1     = parent1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val entitiesChild1      = child1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val entitiesParent2     = parent2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val entitiesChild2      = child2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]

        loadToStore(entitiesGrandparent, entitiesParent1, entitiesChild1, entitiesParent2, entitiesChild2)

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value, None, entitiesGrandparent.provenance.topmostSameAs.value.some),
          (entitiesParent1.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
           entitiesGrandparent.resourceId.value.some
          ),
          (entitiesChild1.resourceId.value,
           entitiesParent1.resourceId.value.some,
           entitiesGrandparent.resourceId.value.some
          ),
          (entitiesParent2.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
           entitiesGrandparent.resourceId.value.some
          ),
          (entitiesChild2.resourceId.value,
           entitiesParent2.resourceId.value.some,
           entitiesGrandparent.resourceId.value.some
          )
        )

        UpdatesCreator
          .prepareUpdates(
            entitiesGrandparent.copy(
              maybeInvalidationTime = invalidationTimes(entitiesGrandparent.provenance.date).generateSome
            )
          )
          .runAll
          .unsafeRunSync()

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value, None, entitiesGrandparent.provenance.topmostSameAs.value.some),
          (entitiesParent1.resourceId.value, None, entitiesParent1.resourceId.value.some),
          (entitiesChild1.resourceId.value,
           entitiesParent1.resourceId.value.some,
           entitiesParent1.resourceId.value.some
          ),
          (entitiesParent2.resourceId.value, None, entitiesParent1.resourceId.value.some),
          (entitiesChild2.resourceId.value,
           entitiesParent2.resourceId.value.some,
           entitiesParent1.resourceId.value.some
          )
        )
      }

    "generate queries for deleted dataset which, " +
      "in case of imported external dataset, " +
      "find datasets which have sameAs pointing to the deleted dataset " +
      "update their sameAs to their topmostSameAs" in {
        val grandparent = datasetEntities(datasetProvenanceImportedExternal).generateOne.copy(parts = Nil)
        val parent1     = grandparent.importTo(projectEntities(visibilityAny)(anyForksCount).generateOne)
        val child1      = parent1.importTo(projectEntities(visibilityAny)(anyForksCount).generateOne)
        val parent2 = {
          val ds = grandparent.importTo(projectEntities(visibilityAny)(anyForksCount).generateOne)
          ds.copy(provenance = ds.provenance.copy(date = datasetPublishedDates(parent1.provenance.date).generateOne))
        }
        val child2 = parent2.importTo(projectEntities(visibilityAny)(anyForksCount).generateOne)

        val entitiesGrandparent = grandparent.to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]
        val entitiesParent1     = parent1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesChild1      = child1.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesParent2     = parent2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val entitiesChild2      = child2.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]

        loadToStore(entitiesGrandparent, entitiesParent1, entitiesChild1, entitiesParent2, entitiesChild2)

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value,
           entitiesGrandparent.provenance.sameAs.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesParent1.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesChild1.resourceId.value,
           entitiesParent1.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesParent2.resourceId.value,
           entitiesGrandparent.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesChild2.resourceId.value,
           entitiesParent2.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          )
        )

        UpdatesCreator
          .prepareUpdates(
            entitiesGrandparent.copy(
              maybeInvalidationTime = invalidationTimes(entitiesGrandparent.provenance.date.instant).generateSome
            )
          )
          .runAll
          .unsafeRunSync()

        findDatasets.map(onlySameAsAndTop) shouldBe Set(
          (entitiesGrandparent.resourceId.value,
           entitiesGrandparent.provenance.sameAs.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesParent1.resourceId.value,
           entitiesGrandparent.provenance.sameAs.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesChild1.resourceId.value,
           entitiesParent1.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesParent2.resourceId.value,
           entitiesGrandparent.provenance.sameAs.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          ),
          (entitiesChild2.resourceId.value,
           entitiesParent2.resourceId.value.some,
           entitiesGrandparent.provenance.topmostSameAs.value.some
          )
        )
      }

    forAll {
      Table(
        ("Provenance name", "Parent Dataset and SameAs"),
        ("internal", {
           val d = datasetEntities(datasetProvenanceInternal).generateOne.copy(parts = Nil)
           d -> Option.empty[SameAs]
         }
        ),
        ("imported external", {
           val d = datasetEntities(datasetProvenanceImportedExternal).generateOne.copy(parts = Nil)
           d -> d.provenance.sameAs.some
         }
        )
      )
    } { case (datasetType, (grandparent, maybeGrandparentSameAs)) =>
      "generate queries for deleted dataset which, " +
        s"in case of $datasetType dataset, " +
        "find datasets which have sameAs pointing to the deleted dataset " +
        "update their sameAs to the deleted dataset sameAs" in {
          val parent = grandparent.importTo(anyProjectEntities.generateOne)(importedInternal)
          val child1 = parent.importTo(anyProjectEntities.generateOne)(importedInternal)
          val child2 = parent.importTo(anyProjectEntities.generateOne)(importedInternal)

          val entitiesGrandparent =
            grandparent.widen[Dataset.Provenance].to[entities.Dataset[entities.Dataset.Provenance]]
          val entitiesParent = parent
            .widen[Dataset.Provenance.ImportedInternal]
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          val entitiesChild1 = child1
            .widen[Dataset.Provenance.ImportedInternal]
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]
          val entitiesChild2 = child2
            .widen[Dataset.Provenance.ImportedInternal]
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]]

          val grandparentTopmostSameAs = entitiesGrandparent.provenance.topmostSameAs.value.some

          loadToStore(entitiesGrandparent, entitiesParent, entitiesChild1, entitiesChild2)

          findDatasets.map(onlySameAsAndTop) shouldBe Set(
            (entitiesGrandparent.resourceId.value, maybeGrandparentSameAs.map(_.value), grandparentTopmostSameAs),
            (entitiesParent.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild1.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild2.resourceId.value, entitiesParent.resourceId.value.some, grandparentTopmostSameAs)
          )

          UpdatesCreator
            .prepareUpdates(
              entitiesParent.copy(
                maybeInvalidationTime = invalidationTimes(entitiesGrandparent.provenance.date.instant).generateSome
              )
            )
            .runAll
            .unsafeRunSync()

          findDatasets.map(onlySameAsAndTop) shouldBe Set(
            (entitiesGrandparent.resourceId.value, maybeGrandparentSameAs.map(_.value), grandparentTopmostSameAs),
            (entitiesParent.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild1.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs),
            (entitiesChild2.resourceId.value, entitiesGrandparent.resourceId.value.some, grandparentTopmostSameAs)
          )
        }
    }
  }

  private def setTopmostSameAs[P <: entities.Dataset.Provenance.ImportedInternal](dataset:       entities.Dataset[P],
                                                                                  topmostSameAs: TopmostSameAs
  ) = {
    val newProvenance = dataset.provenance match {
      case p: entities.Dataset.Provenance.ImportedInternalAncestorInternal => p.copy(topmostSameAs = topmostSameAs)
      case p: entities.Dataset.Provenance.ImportedInternalAncestorExternal => p.copy(topmostSameAs = topmostSameAs)
    }
    dataset.copy(provenance = newProvenance)
  }

  private def findDatasets: Set[(String, Option[String], Option[String], Option[String], Option[String])] =
    runQuery(s"""|SELECT ?id ?maybeSameAs ?maybeTopmostSameAs ?maybeDerivedFrom ?maybeTopmostDerivedFrom
                 |WHERE {
                 |  ?id a schema:Dataset .
                 |  OPTIONAL { ?id schema:sameAs/schema:url ?maybeSameAs } .
                 |  OPTIONAL { ?id renku:topmostSameAs ?maybeTopmostSameAs } .
                 |  OPTIONAL { ?id prov:wasDerivedFrom/schema:url ?maybeDerivedFrom } .
                 |  OPTIONAL { ?id renku:topmostDerivedFrom ?maybeTopmostDerivedFrom } .
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row =>
        (row("id"),
         row.get("maybeSameAs"),
         row.get("maybeTopmostSameAs"),
         row.get("maybeDerivedFrom"),
         row.get("maybeTopmostDerivedFrom")
        )
      )
      .toSet

  private lazy val onlyTopmostSameAs
      : ((String, Option[String], Option[String], Option[String], Option[String])) => (String, Option[String]) = {
    case (resourceId, _, maybeTopmostSameAs, _, _) => resourceId -> maybeTopmostSameAs
  }

  private lazy val onlyTopmostDerivedFrom
      : ((String, Option[String], Option[String], Option[String], Option[String])) => (String, Option[String]) = {
    case (resourceId, _, _, _, maybeTopmostDerivedFrom) => resourceId -> maybeTopmostDerivedFrom
  }

  private lazy val onlySameAsAndTop: (
      (String, Option[String], Option[String], Option[String], Option[String])
  ) => (String, Option[String], Option[String]) = { case (resourceId, sameAs, maybeTopmostSameAs, _, _) =>
    (resourceId, sameAs, maybeTopmostSameAs)
  }

  private def deleteDataset(id: ResourceId) =
    runQuery(s"""|DELETE { ?dsId renku:topmostDerivedFrom <${id.value}> }""")
}
