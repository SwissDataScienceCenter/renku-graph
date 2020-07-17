/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets

import cats.data.NonEmptyList
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.sortBys
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonBlankStrings
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators.datasetInProjectCreationDates
import ch.datascience.graph.model.datasets.{DateCreated, DateCreatedInProject, DerivedFrom, SameAs}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.knowledgegraph.datasets.model.{DatasetCreator, DatasetPart, DatasetProject, ModifiedDataset, NonModifiedDataset}
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.bundles.{modifiedDataSetCommit, nonModifiedDataSetCommit}
import ch.datascience.rdfstore.entities.{DataSet, Person}
import eu.timepit.refined.auto._
import io.renku.jsonld.{EntityId, JsonLD}
import org.scalacheck.Gen

package object rest {
  val phrases:                      Gen[Phrase]                         = nonBlankStrings(minLength = 5) map (_.value) map Phrase.apply
  implicit val searchEndpointSorts: Gen[DatasetsSearchEndpoint.Sort.By] = sortBys(DatasetsSearchEndpoint.Sort)

  implicit class EntityIdOps(entityId: EntityId) {
    lazy val asSameAs:      SameAs      = SameAs.fromId(entityId.value.toString).fold(throw _, identity)
    lazy val asDerivedFrom: DerivedFrom = DerivedFrom(entityId.value.toString)
  }

  implicit class NonModifiedDatasetOps(
      dataSet:             NonModifiedDataset
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl) {
    lazy val entityId: EntityId = DataSet.entityId(dataSet.id)

    def toJsonLD(
        noSameAs:    Boolean  = false,
        commitId:    CommitId = commitIds.generateOne
    )(topmostSameAs: SameAs   = if (noSameAs) dataSet.entityId.asSameAs else dataSet.sameAs): JsonLD =
      dataSet.projects match {
        case project +: Nil =>
          nonModifiedDataSetCommit(
            commitId      = commitId,
            committedDate = CommittedDate(project.created.date.value),
            committer     = Person(project.created.agent.name, project.created.agent.maybeEmail)
          )(
            projectPath = project.path,
            projectName = project.name
          )(
            datasetIdentifier         = dataSet.id,
            datasetName               = dataSet.name,
            datasetUrl                = dataSet.url,
            maybeDatasetSameAs        = if (noSameAs) None else dataSet.sameAs.some,
            maybeDatasetDescription   = dataSet.maybeDescription,
            maybeDatasetPublishedDate = dataSet.published.maybeDate,
            datasetCreatedDate        = DateCreated(project.created.date.value),
            datasetCreators           = dataSet.published.creators map toPerson,
            datasetParts              = dataSet.parts.map(part => (part.name, part.atLocation)),
            overrideTopmostSameAs     = topmostSameAs.some
          )
        case _ => throw new Exception("Not prepared to work datasets having multiple projects")
      }
  }

  implicit class ModifiedDatasetOps(
      dataSet:             ModifiedDataset
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl) {

    lazy val entityId: EntityId = DataSet.entityId(dataSet.id)

    def toJsonLD(commitId:           CommitId    = commitIds.generateOne,
                 topmostDerivedFrom: DerivedFrom = dataSet.derivedFrom): JsonLD = dataSet.projects match {
      case project +: Nil =>
        modifiedDataSetCommit(
          commitId      = commitId,
          committedDate = CommittedDate(project.created.date.value),
          committer     = Person(project.created.agent.name, project.created.agent.maybeEmail)
        )(
          projectPath = project.path,
          projectName = project.name
        )(
          datasetIdentifier          = dataSet.id,
          datasetName                = dataSet.name,
          datasetUrl                 = dataSet.url,
          datasetDerivedFrom         = dataSet.derivedFrom,
          maybeDatasetDescription    = dataSet.maybeDescription,
          maybeDatasetPublishedDate  = dataSet.published.maybeDate,
          datasetCreatedDate         = DateCreated(project.created.date.value),
          datasetCreators            = dataSet.published.creators map toPerson,
          datasetParts               = dataSet.parts.map(part => (part.name, part.atLocation)),
          overrideTopmostDerivedFrom = topmostDerivedFrom.some
        )
      case _ => throw new Exception("Not prepared to work datasets having multiple projects")
    }
  }

  private lazy val toPerson: DatasetCreator => Person =
    creator => Person(creator.name, creator.maybeEmail, creator.maybeAffiliation)

  implicit class DatasetProjectOps(datasetProject: DatasetProject) {
    def shiftDateAfter(project: DatasetProject): DatasetProject =
      datasetProject.copy(
        created = datasetProject.created.copy(
          date = datasetInProjectCreationDates generateGreaterThan project.created.date
        )
      )

    lazy val toGenerator: Gen[NonEmptyList[DatasetProject]] = Gen.const(NonEmptyList of datasetProject)
  }

  implicit lazy val partsAlphabeticalOrdering: Ordering[DatasetPart] =
    (part1: DatasetPart, part2: DatasetPart) => part1.name.value compareTo part2.name.value

  implicit lazy val projectsAlphabeticalOrdering: Ordering[DatasetProject] =
    (project1: DatasetProject, project2: DatasetProject) => project1.name.value compareTo project2.name.value

  implicit val dateCreatedInProjectOrdering: Ordering[DateCreatedInProject] =
    (x: DateCreatedInProject, y: DateCreatedInProject) => x.value compareTo y.value
}
