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
import ch.datascience.graph.model.{activities, datasets, entities, projects}
import ch.datascience.graph.model.projects.ForksCount
import ch.datascience.graph.model.testentities._
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

  private def activityEntities(projectsGen: Gen[Project[ForksCount]]): Gen[Activity] = for {
    executionPlanner <- executionPlanners(runPlanEntities(), projectsGen)
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
