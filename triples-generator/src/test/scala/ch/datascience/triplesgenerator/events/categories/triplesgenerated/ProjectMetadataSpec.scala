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

import cats.data.NonEmptyList
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.entities.Dataset.Provenance
import ch.datascience.graph.model.{InvalidationTime, activities, datasets, entities, projects}
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators.projectMetadatas
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import io.renku.jsonld.syntax._
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.util.Random

class ProjectMetadataSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "from" should {

    "return a ProjectMetadata object if all the data is valid" in {
      val project    = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne
      val activities = activityEntities(fixed(project)).generateList().map(_.to[entities.Activity])
      val datasets = datasetEntities(ofAnyProvenance, fixed(project))
        .generateList()
        .map(_.to[entities.Dataset[entities.Dataset.Provenance]])

      val maybeMetadata = ProjectMetadata.from(project.to[entities.Project], activities, datasets)

      maybeMetadata.map(_.project)    shouldBe project.to[entities.Project].validNel[String]
      maybeMetadata.map(_.activities) shouldBe activities.validNel[String]
      maybeMetadata.map(_.datasets)   shouldBe datasets.validNel[String]
    }

    "align project's person entities across activities and datasets" in {
      val onlyGLMember  = personEntities(withGitLabId, withoutEmail).generateOne
      val gLAndKGMember = personEntities(withGitLabId, withEmail).generateOne
      val creator       = personEntities(withGitLabId, withEmail).generateOne
      val project = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne
        .copy(maybeCreator = Some(creator), members = Set(onlyGLMember, gLAndKGMember))

      val activity = activityEntities(fixed(project)).generateOne
        .copy(author = creator.copy(maybeGitLabId = None))
        .to[entities.Activity]

      val dataset = {
        val d = datasetEntities(datasetProvenanceInternal, fixed(project)).generateOne
        d.copy(provenance = d.provenance.copy(creators = Set(gLAndKGMember.copy(maybeGitLabId = None))))
      }.to[entities.Dataset[entities.Dataset.Provenance.Internal]]

      val maybeMetadata = ProjectMetadata.from(project.to[entities.Project], List(activity), List(dataset))

      maybeMetadata.map(_.project.maybeCreator) shouldBe creator.to[entities.Person].some.validNel[String]
      maybeMetadata.map(_.project.members) shouldBe Set(onlyGLMember, gLAndKGMember)
        .map(_.to[entities.Person])
        .validNel[String]
      maybeMetadata.map(_.activities.map(_.author)) shouldBe List(creator.to[entities.Person]).validNel[String]
      maybeMetadata.map(
        _.datasets.toSet.flatMap((d: entities.Dataset[entities.Dataset.Provenance]) => d.provenance.creators)
      ) shouldBe Set(
        gLAndKGMember.to[entities.Person]
      ).validNel[String]
    }

    "return a failure if activity or dataset points to a different project" in {
      val project = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne
      val otherProject = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne
        .copy(dateCreated =
          timestamps(min = project.dateCreated.value, max = Instant.now).generateAs[projects.DateCreated]
        )
      val invalidActivity = activityEntities(fixed(otherProject)).generateOne.to[entities.Activity]
      val activities = Random.shuffle {
        invalidActivity :: activityEntities(fixed(project)).generateList().map(_.to[entities.Activity])
      }

      val invalidDataset = datasetEntities(ofAnyProvenance, fixed(otherProject)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]
      val datasets = Random.shuffle {
        invalidDataset :: datasetEntities(ofAnyProvenance, fixed(project))
          .generateList()
          .map(_.to[entities.Dataset[entities.Dataset.Provenance]])
      }

      ProjectMetadata.from(project.to[entities.Project], activities, datasets) shouldBe NonEmptyList
        .of(
          s"Activity ${invalidActivity.resourceId} points to a wrong project ${otherProject.asEntityId}",
          s"Dataset ${invalidDataset.resourceId} points to a wrong project ${otherProject.asEntityId}"
        )
        .invalid
    }

    "return a failure if activity's startTime is before project creation date - case of project without a parent" in {
      val project = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne
      val invalidActivity = activityEntities(fixed(project)).generateOne
        .to[entities.Activity]
        .copy(startTime = timestamps(max = project.dateCreated.value).generateAs[activities.StartTime])

      ProjectMetadata.from(project.to[entities.Project], List(invalidActivity), datasets = Nil) shouldBe
        s"Activity ${invalidActivity.resourceId} startTime ${invalidActivity.startTime} is older than project ${project.dateCreated}".invalidNel
    }

    "return a failure if internal or modified dataset's provenance date is before project creation date - case of project without a parent" in {
      val project = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne
      List(
        datasetEntities(datasetProvenanceInternal, fixed(project)),
        datasetEntities(datasetProvenanceModified, fixed(project))
      ).foreach { datasetGen =>
        val invalidDataset = {
          val d = datasetGen.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]
          d.copy(provenance = updateProvenanceDateAfter(project.dateCreated)(d.provenance))
        }

        ProjectMetadata.from(project.to[entities.Project], activities = Nil, datasets = List(invalidDataset)) shouldBe
          s"Dataset ${invalidDataset.identification.identifier} startTime ${invalidDataset.provenance.date} is older than project ${project.dateCreated}".invalidNel
      }
    }

    "succeeds for datasets with external provenance and date older than project - case project without a parent" in {
      val project = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne

      List(
        datasetEntities(datasetProvenanceImportedExternal, fixed(project)),
        datasetEntities(datasetProvenanceImportedInternalAncestorExternal, fixed(project)),
        datasetEntities(datasetProvenanceImportedInternalAncestorInternal, fixed(project))
      ).foreach { datasetGen =>
        val dataset = {
          val d = datasetGen.generateOne.to[entities.Dataset[entities.Dataset.Provenance]]
          d.copy(provenance = updateProvenanceDateAfter(project.dateCreated)(d.provenance))
        }
        val maybeMetadata =
          ProjectMetadata.from(project.to[entities.Project], activities = Nil, datasets = List(dataset))
        maybeMetadata.map(_.datasets) shouldBe List(dataset).validNel[String]
      }
    }

    "succeeds for datasets with any provenance and date older than project - case project with a parent" in {
      val project = projectWithParentEntities(visibilityAny).generateOne
      forAll(datasetEntities(ofAnyProvenance, fixed(project))) { dataset =>
        val validDataset = {
          val d = dataset.to[entities.Dataset[entities.Dataset.Provenance]]
          d.copy(provenance = updateProvenanceDateAfter(project.dateCreated)(d.provenance))
        }
        val maybeMetadata =
          ProjectMetadata.from(project.to[entities.Project], activities = Nil, datasets = List(validDataset))
        maybeMetadata.map(_.datasets) shouldBe List(validDataset).validNel[String]
      }
    }
  }

  "findAllPersons" should {
    "collect project members, project creator, activities' authors and datasets' creators" in {
      val metadata = projectMetadatas.generateOne
      metadata.findAllPersons shouldBe metadata.project.members ++ metadata.project.maybeCreator ++ metadata.activities
        .map(_.author) ++ metadata.datasets.flatMap(_.provenance.creators)
    }
  }

  "update - person" should {
    val oldPerson = personEntities().generateOne.to[entities.Person]
    val project   = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne

    "replace the old person with the new on project" in {
      val entitiesProject = project.to[entities.Project] match {
        case p: entities.ProjectWithoutParent =>
          p.copy(maybeCreator = Some(oldPerson),
                 members = Set(oldPerson, personEntities.generateOne.to[entities.Person])
          )
        case p: entities.ProjectWithParent =>
          p.copy(maybeCreator = Some(oldPerson),
                 members = Set(oldPerson, personEntities.generateOne.to[entities.Person])
          )
      }

      val metadata = ProjectMetadata
        .from(entitiesProject, activities = Nil, datasets = Nil)
        .fold(errors => fail(errors.intercalate("; ")), identity)

      val newPerson = personEntities().generateOne.to[entities.Person]

      val updatedMetadata = metadata.update(oldPerson, newPerson)

      updatedMetadata.project.members      shouldBe entitiesProject.members - oldPerson + newPerson
      updatedMetadata.project.maybeCreator shouldBe Some(newPerson)
    }

    "replace the old person with the new on all activities" in {
      val activity1 = activityEntities(fixed(project)).generateOne
        .to[entities.Activity]
        .copy(author = oldPerson)
      val activity2 = activityEntities(fixed(project)).generateOne.to[entities.Activity]

      val metadata = ProjectMetadata
        .from(project.to[entities.Project], List(activity1, activity2), datasets = Nil)
        .fold(errors => fail(errors.intercalate("; ")), identity)

      val newPerson = personEntities().generateOne.to[entities.Person]

      metadata.update(oldPerson, newPerson).activities shouldBe List(activity1.copy(author = newPerson), activity2)
    }

    "replace the old person with the new on all datasets" in {
      val dataset1 = {
        val d = datasetEntities(datasetProvenanceInternal, fixed(project)).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.Internal]]
        d.copy(provenance =
          d.provenance.copy(creators = Set(oldPerson, personEntities.generateOne.to[entities.Person]))
        )
      }

      val dataset2 = datasetEntities(ofAnyProvenance, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      val metadata = ProjectMetadata
        .from(project.to[entities.Project], activities = Nil, datasets = List(dataset1, dataset2))
        .fold(errors => fail(errors.intercalate("; ")), identity)

      val newPerson = personEntities().generateOne.to[entities.Person]

      metadata.update(oldPerson, newPerson).datasets shouldBe List(
        dataset1.copy(provenance =
          dataset1.provenance.copy(creators = dataset1.provenance.creators - oldPerson + newPerson)
        ),
        dataset2
      )
    }
  }

  "update - dataset" should {

    "replace the old dataset with the new one" in {
      val project = projectEntities[ForksCount](visibilityAny)(anyForksCount).generateOne

      val dataset1 = datasetEntities(ofAnyProvenance, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]
      val dataset2Old = datasetEntities(ofAnyProvenance, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]
      val dataset2New = datasetEntities(ofAnyProvenance, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance]]

      val metadata = ProjectMetadata
        .from(project.to[entities.Project], activities = Nil, datasets = List(dataset1, dataset2Old))
        .fold(errors => fail(errors.intercalate("; ")), identity)

      metadata.update(dataset2Old, dataset2New).datasets should contain theSameElementsAs List(dataset1, dataset2New)
    }
  }

  "findInternallyImportedDatasets" should {

    "return all datasets with ImportedInternalAncestorExternal or ImportedInternalAncestorInternal provenance" +
      "which are not invalidated" in {
        val project = projectEntities(visibilityAny)(anyForksCount).generateOne

        val internal = datasetEntities(datasetProvenanceInternal, fixed(project)).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.Internal]]
        val importedExternal = datasetEntities(datasetProvenanceImportedExternal, fixed(project)).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]
        val importedInternalAncestorInternalInvalidated =
          datasetEntities(datasetProvenanceImportedInternalAncestorInternal, fixed(project)).generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
            .copy(maybeInvalidationTime = InvalidationTime(Instant.now).some)
        val importedInternalAncestorInternal =
          datasetEntities(datasetProvenanceImportedInternalAncestorInternal, fixed(project)).generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
        val importedInternalAncestorExternal =
          datasetEntities(datasetProvenanceImportedInternalAncestorExternal, fixed(project)).generateOne
            .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
        val modified = datasetEntities(datasetProvenanceModified, fixed(project)).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.Modified]]

        val metadata = ProjectMetadata
          .from(
            project.to[entities.Project],
            activities = Nil,
            datasets = List(
              internal,
              importedExternal,
              importedInternalAncestorInternal,
              importedInternalAncestorInternalInvalidated,
              importedInternalAncestorExternal,
              modified
            )
          )
          .fold(errors => fail(errors.intercalate(", ")), identity)

        metadata.findInternallyImportedDatasets should contain theSameElementsAs List(importedInternalAncestorInternal,
                                                                                      importedInternalAncestorExternal
        )
      }
  }

  "findModifiedDatasets" should {
    "return all datasets with Provenence.Modified which are not invalidated" in {
      val project = projectEntities(visibilityAny)(anyForksCount).generateOne

      val internal = datasetEntities(datasetProvenanceInternal, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Internal]]
      val importedExternal = datasetEntities(datasetProvenanceImportedExternal, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]
      val importedInternalAncestorInternal =
        datasetEntities(datasetProvenanceImportedInternalAncestorInternal, fixed(project)).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
      val importedInternalAncestorExternal =
        datasetEntities(datasetProvenanceImportedInternalAncestorExternal, fixed(project)).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
      val modified = datasetEntities(datasetProvenanceModified, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
      val modifiedInvalidated = datasetEntities(datasetProvenanceModified, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
        .copy(maybeInvalidationTime = InvalidationTime(Instant.now).some)

      val metadata = ProjectMetadata
        .from(
          project.to[entities.Project],
          activities = Nil,
          datasets = List(
            internal,
            importedExternal,
            importedInternalAncestorInternal,
            modifiedInvalidated,
            importedInternalAncestorExternal,
            modified
          )
        )
        .fold(errors => fail(errors.intercalate(", ")), identity)

      metadata.findModifiedDatasets should contain theSameElementsAs List(modified)
    }
  }

  "findInvalidatedDatasets" should {
    "find all invalidated datasets and not valid datasets" in {
      val project = projectEntities(visibilityAny)(anyForksCount).generateOne

      val internal = datasetEntities(datasetProvenanceInternal, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Internal]]
        .copy(maybeInvalidationTime = InvalidationTime(Instant.now).some)

      val importedExternal = datasetEntities(datasetProvenanceImportedExternal, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.ImportedExternal]]
        .copy(maybeInvalidationTime = InvalidationTime(Instant.now).some)

      val importedInternalAncestorInternal =
        datasetEntities(datasetProvenanceImportedInternalAncestorInternal, fixed(project)).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorInternal]]
          .copy(maybeInvalidationTime = InvalidationTime(Instant.now).some)

      val importedInternalAncestorExternal =
        datasetEntities(datasetProvenanceImportedInternalAncestorExternal, fixed(project)).generateOne
          .to[entities.Dataset[entities.Dataset.Provenance.ImportedInternalAncestorExternal]]
          .copy(maybeInvalidationTime = InvalidationTime(Instant.now).some)

      val modified = datasetEntities(datasetProvenanceModified, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Modified]]
        .copy(maybeInvalidationTime = InvalidationTime(Instant.now).some)

      val modifiedValid = datasetEntities(datasetProvenanceModified, fixed(project)).generateOne
        .to[entities.Dataset[entities.Dataset.Provenance.Modified]]

      val metadata = ProjectMetadata
        .from(
          project.to[entities.Project],
          activities = Nil,
          datasets = List(
            internal,
            importedExternal,
            importedInternalAncestorInternal,
            modifiedValid,
            importedInternalAncestorExternal,
            modified
          )
        )
        .fold(errors => fail(errors.intercalate(", ")), identity)

      metadata.findInvalidatedDatasets should contain theSameElementsAs List(internal,
                                                                             importedExternal,
                                                                             importedInternalAncestorInternal,
                                                                             importedInternalAncestorExternal,
                                                                             modified
      )
    }
  }

  "encoder" should {
    "encode all the metadata properties into JsonLD" in {
      fail("BOOM!")
    }
  }

  private def activityEntities(projectsGen: Gen[Project[ForksCount]]): Gen[Activity] = for {
    executionPlanner <- executionPlanners(planEntities(), projectsGen)
  } yield executionPlanner.buildProvenanceUnsafe()

  private def updateProvenanceDateAfter(
      projectDate: projects.DateCreated
  ): entities.Dataset.Provenance => entities.Dataset.Provenance = {
    case p: Provenance.Modified =>
      p.copy(date = timestamps(max = projectDate.value).generateAs[datasets.DateCreated])
    case p: Provenance.Internal =>
      p.copy(date = timestamps(max = projectDate.value).generateAs[datasets.DateCreated])
    case p: Provenance.ImportedExternal =>
      p.copy(date =
        timestamps(max = projectDate.value)
          .map(LocalDateTime.ofInstant(_, ZoneOffset.UTC))
          .map(_.toLocalDate)
          .generateAs[datasets.DatePublished]
      )
    case p: Provenance.ImportedInternalAncestorExternal =>
      p.copy(date =
        timestamps(max = projectDate.value)
          .map(LocalDateTime.ofInstant(_, ZoneOffset.UTC))
          .map(_.toLocalDate)
          .generateAs[datasets.DatePublished]
      )
    case p: Provenance.ImportedInternalAncestorInternal =>
      p.copy(date = timestamps(max = projectDate.value).generateAs[datasets.DateCreated])
  }
}
