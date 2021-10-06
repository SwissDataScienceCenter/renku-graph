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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model._
import ch.datascience.graph.model.testentities.{::~, activityEntities, anyProjectEntities, anyVisibility, creatorsLens, datasetEntities, provenanceNonModified, personEntities, planEntities, projectEntities, projectWithParentEntities, provenanceLens, _}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectFunctionsSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  import ProjectFunctions._

  "findAllPersons" should {
    "collect project members, project creator, activities' authors and datasets' creators" in {
      forAll(
        anyProjectEntities
          .withActivities(activityEntities(planEntities()))
          .withDatasets(datasetEntities(provenanceNonModified))
          .map(_.to[entities.Project])
      ) { project =>
        findAllPersons(project) shouldBe project.members ++ project.maybeCreator ++ project.activities
          .map(_.author) ++ project.datasets.flatMap(_.provenance.creators)
      }
    }
  }

  "update - person" should {
    val oldPerson = personEntities().generateOne

    "replace the old person with the new on project" in {
      Set(
        projectEntities(anyVisibility)
          .modify(
            _.copy(maybeCreator = Some(oldPerson), members = Set(oldPerson, personEntities.generateOne))
          ),
        projectWithParentEntities(anyVisibility)
          .modify(
            _.copy(maybeCreator = Some(oldPerson), members = Set(oldPerson, personEntities.generateOne))
          )
      ) foreach { projectGen =>
        val project           = projectGen.generateOne.to[entities.Project]
        val entitiesOldPerson = oldPerson.to[entities.Person]

        val newPerson = personEntities().generateOne.to[entities.Person]

        val updatedProject = update(entitiesOldPerson, newPerson)(project)

        updatedProject.members      shouldBe project.members - entitiesOldPerson + newPerson
        updatedProject.maybeCreator shouldBe Some(newPerson)
      }
    }

    "replace the old person with the new on all activities" in {

      val project = anyProjectEntities
        .withActivities(
          activityEntities(planEntities()).modify(_.copy(author = oldPerson)),
          activityEntities(planEntities())
        )
        .generateOne
        .to[entities.Project]

      val entitiesOldPerson = oldPerson.to[entities.Person]
      val newPerson         = personEntities().generateOne.to[entities.Person]

      update(entitiesOldPerson, newPerson)(project).activities shouldBe project.activities.map {
        case activity if activity.author == entitiesOldPerson => activity.copy(author = newPerson)
        case activity                                         => activity
      }
    }

    "replace the old person with the new on all datasets" in {

      val project = anyProjectEntities
        .withDatasets(
          datasetEntities(provenanceNonModified).modify(
            provenanceLens.modify(creatorsLens.modify(_ => Set(oldPerson, personEntities.generateOne)))
          ),
          datasetEntities(provenanceNonModified)
        )
        .generateOne
        .to[entities.Project]

      val entitiesOldPerson = oldPerson.to[entities.Person]
      val newPerson         = personEntities().generateOne.to[entities.Person]

      val dataset1 :: dataset2 :: Nil = update(entitiesOldPerson, newPerson)(project).datasets
      dataset1.provenance.creators shouldBe project.datasets.head.provenance.creators.map {
        case creator if creator == entitiesOldPerson => newPerson
        case creator                                 => creator
      }
      dataset2.provenance.creators shouldBe project.datasets.tail.head.provenance.creators
    }
  }

  "update - dataset" should {

    "replace the old dataset with the new one" in {
      val (_, project) = anyProjectEntities
        .addDataset(datasetEntities(provenanceNonModified))
        .addDataset(datasetEntities(provenanceNonModified))
        .generateOne
        .map(_.to[entities.Project])

      val dataset1 :: dataset2Old :: Nil = project.datasets

      val dataset2New = datasetEntities(provenanceNonModified)(renkuBaseUrl)(project.dateCreated).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      update(dataset2Old, dataset2New)(project).datasets should contain theSameElementsAs List(dataset1, dataset2New)
    }
  }

  "findInternallyImportedDatasets" should {

    "return all datasets with ImportedInternalAncestorExternal or ImportedInternalAncestorInternal provenance" +
      "which are not invalidated" in {
        val (importedInternalAncestorInternal ::~ importedInternalAncestorExternal ::~ _ ::~ _ ::~ _, project) =
          anyProjectEntities
            .addDataset(datasetEntities(provenanceImportedInternalAncestorInternal))
            .addDataset(datasetEntities(provenanceImportedInternalAncestorExternal))
            .addDataset(datasetEntities(provenanceInternal))
            .addDataset(datasetEntities(provenanceImportedExternal))
            .addDataset(datasetEntities(provenanceModified))
            .generateOne

        val originalImportedInternal =
          datasetEntities(provenanceImportedInternalAncestorInternal)(renkuBaseUrl)(project.dateCreated).generateOne
        val projectWithAllDatasets =
          project.addDatasets(originalImportedInternal, originalImportedInternal.invalidateNow).to[entities.Project]

        findInternallyImportedDatasets(projectWithAllDatasets) should contain theSameElementsAs List(
          importedInternalAncestorInternal,
          importedInternalAncestorExternal
        ).map(_.to[entities.Dataset[entities.Dataset.Provenance.ImportedInternal]])
      }
  }

  "findModifiedDatasets" should {
    "return all datasets with Provenance.Modified which are not invalidations" in {
      val (modified ::~ _ ::~ _ ::~ _ ::~ _, project) = anyProjectEntities
        .addDataset(datasetEntities(provenanceModified))
        .addDataset(datasetEntities(provenanceInternal))
        .addDataset(datasetEntities(provenanceImportedExternal))
        .addDataset(datasetEntities(provenanceImportedInternalAncestorInternal))
        .addDataset(datasetEntities(provenanceImportedInternalAncestorExternal))
        .generateOne

      val originalModified = datasetEntities(provenanceModified)(renkuBaseUrl)(project.dateCreated).generateOne
      val projectWithAllDatasets =
        project.addDatasets(originalModified, originalModified.invalidateNow).to[entities.Project]

      findModifiedDatasets(projectWithAllDatasets) should contain theSameElementsAs List(
        modified.to[entities.Dataset[entities.Dataset.Provenance.Modified]]
      )
    }
  }

  "findInvalidatedDatasets" should {
    "find all invalidated datasets and not valid datasets or invalidation datasets" in {

      val (internal ::~ _ ::~ importedExternal ::~ _ ::~ ancestorInternal ::~ _ ::~ ancestorExternal ::~ _ ::~ modified ::~ _ ::~ _,
           project
      ) = anyProjectEntities
        .addDatasetAndInvalidation(datasetEntities(provenanceInternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedExternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorInternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceImportedInternalAncestorExternal))
        .addDatasetAndInvalidation(datasetEntities(provenanceModified))
        .addDataset(datasetEntities(provenanceModified))
        .generateOne

      findInvalidatedDatasets(project.to[entities.Project]) should contain theSameElementsAs List(
        internal,
        importedExternal,
        ancestorInternal,
        ancestorExternal,
        modified
      ).map(_.to[entities.Dataset[entities.Dataset.Provenance]])
    }
  }
}
