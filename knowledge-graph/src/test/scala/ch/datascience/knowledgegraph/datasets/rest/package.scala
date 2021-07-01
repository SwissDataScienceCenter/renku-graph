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

package ch.datascience.knowledgegraph.datasets

import ch.datascience.generators.CommonGraphGenerators.sortBys
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.{RenkuBaseUrl, testentities}
import ch.datascience.graph.model.datasets._
import ch.datascience.graph.model.testentities.{Dataset, Person, Project}
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import eu.timepit.refined.auto._
import org.scalacheck.Gen

package object rest {
  val phrases:                      Gen[Phrase]                         = nonBlankStrings(minLength = 5) map (_.value) map Phrase.apply
  implicit val searchEndpointSorts: Gen[DatasetsSearchEndpoint.Sort.By] = sortBys(DatasetsSearchEndpoint.Sort)

  implicit lazy val personToCreator: Person => DatasetCreator =
    person => DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)

  implicit lazy val projectToDatasetProject: Project[_] => DatasetProject =
    project => DatasetProject(project.path, project.name)

  implicit def internalToNonModified(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): testentities.Dataset[Dataset.Provenance.Internal] => NonModifiedDataset =
    dataset =>
      NonModifiedDataset(
        dataset.identification.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.url,
        SameAs(dataset.entityId),
        DatasetVersions(dataset.provenance.initialVersion),
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(person =>
          DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
        ),
        dataset.provenance.date,
        dataset.parts.map(part => model.DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
        DatasetProject(dataset.project.path, dataset.project.name),
        usedIn = List(DatasetProject(dataset.project.path, dataset.project.name)),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )

  implicit def importedExternalToNonModified(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): testentities.Dataset[Dataset.Provenance.ImportedExternal] => NonModifiedDataset =
    dataset =>
      NonModifiedDataset(
        dataset.identification.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.url,
        dataset.provenance.sameAs,
        DatasetVersions(dataset.provenance.initialVersion),
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(person =>
          DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
        ),
        dataset.provenance.date,
        dataset.parts.map(part => model.DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
        DatasetProject(dataset.project.path, dataset.project.name),
        usedIn = List(DatasetProject(dataset.project.path, dataset.project.name)),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )

  implicit def importedInternalToNonModified[P <: Dataset.Provenance.ImportedInternal](implicit
      renkuBaseUrl: RenkuBaseUrl
  ): testentities.Dataset[P] => NonModifiedDataset =
    dataset =>
      NonModifiedDataset(
        dataset.identification.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.url,
        dataset.provenance.sameAs,
        DatasetVersions(dataset.provenance.initialVersion),
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(person =>
          DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
        ),
        dataset.provenance.date,
        dataset.parts.map(part => model.DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
        DatasetProject(dataset.project.path, dataset.project.name),
        usedIn = List(DatasetProject(dataset.project.path, dataset.project.name)),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )

  implicit def modifiedToModified(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): testentities.Dataset[Dataset.Provenance.Modified] => ModifiedDataset =
    dataset =>
      ModifiedDataset(
        dataset.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.url,
        dataset.provenance.derivedFrom,
        DatasetVersions(dataset.provenance.initialVersion),
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(person =>
          DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
        ),
        dataset.provenance.date,
        dataset.parts.map(part => model.DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
        DatasetProject(dataset.project.path, dataset.project.name),
        usedIn = List(DatasetProject(dataset.project.path, dataset.project.name)),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )
}
