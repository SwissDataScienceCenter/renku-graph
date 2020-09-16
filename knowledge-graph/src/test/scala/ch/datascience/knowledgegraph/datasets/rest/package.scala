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
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.sortBys
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.{nonBlankStrings, nonEmptyList, positiveInts, sentenceContaining}
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.EventsGenerators.commitIds
import ch.datascience.graph.model.GraphModelGenerators.{datasetIdentifiers, datasetInProjectCreationDates, userAffiliations, userEmails}
import ch.datascience.graph.model.datasets.{DateCreated, DateCreatedInProject, DerivedFrom, Description, Name, PublishedDate, SameAs, Title, TopmostSameAs}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.graph.model.users.{Name => UserName}
import ch.datascience.knowledgegraph.datasets.DatasetsGenerators.datasetProjects
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.bundles.{modifiedDataSetCommit, nonModifiedDataSetCommit}
import ch.datascience.rdfstore.entities.{DataSet, Person}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.jsonld.{EntityId, JsonLD}
import org.scalacheck.Gen

package object rest {
  val phrases:                      Gen[Phrase]                         = nonBlankStrings(minLength = 5) map (_.value) map Phrase.apply
  implicit val searchEndpointSorts: Gen[DatasetsSearchEndpoint.Sort.By] = sortBys(DatasetsSearchEndpoint.Sort)

  implicit class EntityIdOps(entityId: EntityId) {
    lazy val asSameAs:        SameAs        = SameAs.fromId(entityId.value.toString).fold(throw _, identity)
    lazy val asTopmostSameAs: TopmostSameAs = TopmostSameAs(entityId)
    lazy val asDerivedFrom:   DerivedFrom   = DerivedFrom(entityId.value.toString)
  }

  implicit class NonModifiedDatasetOps(
      dataSet:             NonModifiedDataset
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl) {

    lazy val entityId: EntityId = DataSet.entityId(dataSet.id)

    def changeCreationOnProject(to: AddedToProject): NonModifiedDataset = {
      val project +: other = dataSet.projects
      if (other != Nil) throw new IllegalStateException("Don't know on which project creation data should be changed")

      dataSet.copy(projects = List(project.copy(created = to)))
    }

    def toJsonLD(
        noSameAs:    Boolean       = false,
        commitId:    CommitId      = commitIds.generateOne
    )(topmostSameAs: TopmostSameAs = if (noSameAs) TopmostSameAs(dataSet.entityId) else TopmostSameAs(dataSet.sameAs))
        : JsonLD =
      toJsonLDsAndDatasets(
        firstDatasetDateCreated = DateCreated(dataSet.projects.map(_.created.date.value).min),
        noSameAs                = noSameAs,
        commitId                = commitId
      )(topmostSameAs) match {
        case (json, _) +: Nil => json
        case _                => throw new Exception("Not prepared to work datasets having multiple projects")
      }

    def toJsonLDsAndDatasets(
        firstDatasetDateCreated: DateCreated = DateCreated(dataSet.projects.map(_.created.date.value).min),
        noSameAs:                Boolean,
        commitId:                CommitId = commitIds.generateOne
    )(topmostSameAs:             TopmostSameAs = if (noSameAs) TopmostSameAs(dataSet.entityId) else TopmostSameAs(dataSet.sameAs))
        : List[(JsonLD, Dataset)] =
      dataSet.projects match {
        case firstProject +: otherProjects =>
          val firstTuple = nonModifiedDataSetCommit(
            commitId      = commitId,
            committedDate = CommittedDate(firstDatasetDateCreated.value),
            committer     = Person(firstProject.created.agent.name, firstProject.created.agent.maybeEmail)
          )(
            projectPath = firstProject.path,
            projectName = firstProject.name
          )(
            datasetIdentifier         = dataSet.id,
            datasetTitle              = dataSet.title,
            datasetName               = dataSet.name,
            datasetUrl                = dataSet.url,
            maybeDatasetSameAs        = if (noSameAs) None else dataSet.sameAs.some,
            maybeDatasetDescription   = dataSet.maybeDescription,
            maybeDatasetPublishedDate = dataSet.published.maybeDate,
            datasetCreatedDate        = firstDatasetDateCreated,
            datasetCreators           = dataSet.published.creators map toPerson,
            datasetParts              = dataSet.parts.map(part => (part.name, part.atLocation)),
            datasetKeywords           = dataSet.keywords,
            overrideTopmostSameAs     = topmostSameAs.some
          ) -> dataSet

          val sameAs =
            if (noSameAs) DataSet.entityId(dataSet.id).asSameAs
            else dataSet.sameAs
          val otherTuples = otherProjects.map { project =>
            val projectDateCreated = firstDatasetDateCreated.shiftToFuture
            val dataSetId          = datasetIdentifiers.generateOne
            nonModifiedDataSetCommit(
              committedDate = CommittedDate(projectDateCreated.value)
            )(
              projectPath = project.path,
              projectName = project.name
            )(
              datasetIdentifier         = dataSetId,
              datasetTitle              = dataSet.title,
              datasetName               = dataSet.name,
              datasetUrl                = dataSet.url,
              maybeDatasetSameAs        = sameAs.some,
              maybeDatasetDescription   = dataSet.maybeDescription,
              maybeDatasetPublishedDate = dataSet.published.maybeDate,
              datasetCreatedDate        = projectDateCreated,
              datasetCreators           = dataSet.published.creators map toPerson,
              datasetParts              = dataSet.parts.map(part => (part.name, part.atLocation)),
              datasetKeywords           = dataSet.keywords,
              overrideTopmostSameAs     = topmostSameAs.some
            ) -> dataSet.copy(
              id     = dataSetId,
              sameAs = sameAs,
              projects = List(
                project.copy(created = project.created.copy(date = DateCreatedInProject(projectDateCreated.value)))
              )
            )
          }

          firstTuple +: otherTuples
      }

    def changePublishedDateTo(maybeDate: Option[PublishedDate]): NonModifiedDataset =
      dataSet.copy(published = dataSet.published.copy(maybeDate = maybeDate))

    def addAll(projects: List[DatasetProject]): NonModifiedDataset =
      dataSet.copy(projects = dataSet.projects ++ projects)

    def makeNameContaining(phrase: Phrase): NonModifiedDataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataSet.copy(
        name = sentenceContaining(nonEmptyPhrase).map(_.value).map(Name.apply).generateOne
      )
    }

    def makeTitleContaining(phrase: Phrase): NonModifiedDataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataSet.copy(
        title = sentenceContaining(nonEmptyPhrase).map(_.value).map(Title.apply).generateOne
      )
    }

    def makeCreatorNameContaining(phrase: Phrase): NonModifiedDataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataSet.copy(
        published = dataSet.published.copy(
          creators = Set(
            DatasetCreator(
              userEmails.generateOption,
              sentenceContaining(nonEmptyPhrase).map(_.value).map(UserName.apply).generateOne,
              userAffiliations.generateOption
            )
          )
        )
      )
    }

    def makeDescContaining(maybePhrase: Option[Phrase]): NonModifiedDataset =
      maybePhrase map makeDescContaining getOrElse dataSet

    def makeDescContaining(phrase: Phrase): NonModifiedDataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataSet.copy(
        maybeDescription = sentenceContaining(nonEmptyPhrase).map(_.value).map(Description.apply).generateSome
      )
    }
  }

  implicit class ModifiedDatasetOps(
      dataSet:             ModifiedDataset
  )(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl) {

    lazy val entityId: EntityId = DataSet.entityId(dataSet.id)

    def toJsonLD(firstDatasetDateCreated: DateCreated = DateCreated(dataSet.projects.map(_.created.date.value).min),
                 commitId:                CommitId    = commitIds.generateOne,
                 topmostDerivedFrom:      DerivedFrom = dataSet.derivedFrom): JsonLD =
      toJsonLDs(firstDatasetDateCreated, commitId, topmostDerivedFrom) match {
        case first +: Nil => first
        case _ =>
          throw new IllegalStateException(
            "Modified dataset contains more than one project; are you working with forks?"
          )
      }

    def toJsonLDs(firstDatasetDateCreated: DateCreated = DateCreated(dataSet.projects.map(_.created.date.value).min),
                  commitId:                CommitId    = commitIds.generateOne,
                  topmostDerivedFrom:      DerivedFrom = dataSet.derivedFrom): List[JsonLD] =
      dataSet.projects match {
        case firstProject +: otherProjects =>
          val firstJsonLd = modifiedDataSetCommit(
            commitId      = commitId,
            committedDate = CommittedDate(firstDatasetDateCreated.value),
            committer     = Person(firstProject.created.agent.name, firstProject.created.agent.maybeEmail)
          )(
            projectPath = firstProject.path,
            projectName = firstProject.name
          )(
            datasetIdentifier          = dataSet.id,
            datasetTitle               = dataSet.title,
            datasetName                = dataSet.name,
            datasetUrl                 = dataSet.url,
            datasetDerivedFrom         = dataSet.derivedFrom,
            maybeDatasetDescription    = dataSet.maybeDescription,
            maybeDatasetPublishedDate  = dataSet.published.maybeDate,
            datasetCreatedDate         = DateCreated(firstDatasetDateCreated.value),
            datasetCreators            = dataSet.published.creators map toPerson,
            datasetParts               = dataSet.parts.map(part => (part.name, part.atLocation)),
            datasetKeywords            = dataSet.keywords,
            overrideTopmostDerivedFrom = topmostDerivedFrom.some
          )

          val otherJsonLds = otherProjects.map { project =>
            val projectDateCreated = datasetInProjectCreationDates generateGreaterThan DateCreatedInProject(
              firstDatasetDateCreated.value
            )

            modifiedDataSetCommit(
              committedDate = CommittedDate(projectDateCreated.value)
            )(
              projectPath = project.path,
              projectName = project.name
            )(
              datasetTitle               = dataSet.title,
              datasetName                = dataSet.name,
              datasetUrl                 = dataSet.url,
              datasetDerivedFrom         = dataSet.derivedFrom,
              maybeDatasetDescription    = dataSet.maybeDescription,
              maybeDatasetPublishedDate  = dataSet.published.maybeDate,
              datasetCreatedDate         = DateCreated(projectDateCreated.value),
              datasetCreators            = dataSet.published.creators map toPerson,
              datasetParts               = dataSet.parts.map(part => (part.name, part.atLocation)),
              datasetKeywords            = dataSet.keywords,
              overrideTopmostDerivedFrom = topmostDerivedFrom.some
            )
          }
          firstJsonLd +: otherJsonLds
      }

    def makeTitleContaining(phrase: Phrase): ModifiedDataset = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase.toString)
      dataSet.copy(
        title = sentenceContaining(nonEmptyPhrase).map(_.value).map(Title.apply).generateOne
      )
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

  implicit class DateCreatedOps(dateCreated: DateCreated) {
    lazy val shiftToFuture: DateCreated = DateCreated(dateCreated.value plusSeconds positiveInts().generateOne.value)
  }

  implicit lazy val partsAlphabeticalOrdering: Ordering[DatasetPart] =
    (part1: DatasetPart, part2: DatasetPart) => part1.name.value compareTo part2.name.value

  implicit lazy val projectsAlphabeticalOrdering: Ordering[DatasetProject] =
    (project1: DatasetProject, project2: DatasetProject) => project1.name.value compareTo project2.name.value

  implicit val dateCreatedInProjectOrdering: Ordering[DateCreatedInProject] =
    (x: DateCreatedInProject, y: DateCreatedInProject) => x.value compareTo y.value

  lazy val byName: Ordering[Dataset] =
    (ds1: Dataset, ds2: Dataset) => ds1.name.value compareTo ds2.name.value

  val single: Gen[NonEmptyList[DatasetProject]] = nonEmptyList(datasetProjects, maxElements = 1)
  val two:    Gen[NonEmptyList[DatasetProject]] = nonEmptyList(datasetProjects, minElements = 2, maxElements = 2)
}
