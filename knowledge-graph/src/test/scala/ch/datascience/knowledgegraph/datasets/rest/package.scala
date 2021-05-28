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
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.datasets._
import ch.datascience.knowledgegraph.datasets.model._
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.rdfstore.entities
import ch.datascience.rdfstore.entities._
import eu.timepit.refined.auto._
import org.scalacheck.Gen

package object rest {
  val phrases:                      Gen[Phrase]                         = nonBlankStrings(minLength = 5) map (_.value) map Phrase.apply
  implicit val searchEndpointSorts: Gen[DatasetsSearchEndpoint.Sort.By] = sortBys(DatasetsSearchEndpoint.Sort)

  lazy val toCreator: Person => DatasetCreator =
    person => DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)

  implicit def internalToNonModified(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): entities.Dataset[Dataset.Provenance.Internal] => NonModifiedDataset =
    dataset =>
      NonModifiedDataset(
        dataset.identification.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.url,
        SameAs(dataset.entityId),
        DatasetVersions(InitialVersion(dataset.provenance.topmostSameAs.value)),
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(person =>
          DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
        ),
        dataset.provenance.date,
        dataset.parts.map(part => model.DatasetPart(PartLocation(part.entity.location.value))),
        DatasetProject(dataset.project.path, dataset.project.name),
        usedIn = List(DatasetProject(dataset.project.path, dataset.project.name)),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )

  implicit def importedExternalToNonModified(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): entities.Dataset[Dataset.Provenance.ImportedExternal] => NonModifiedDataset =
    dataset =>
      NonModifiedDataset(
        dataset.identification.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.url,
        dataset.provenance.sameAs,
        DatasetVersions(InitialVersion(dataset.provenance.topmostSameAs.value)),
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(person =>
          DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
        ),
        dataset.provenance.date,
        dataset.parts.map(part => model.DatasetPart(PartLocation(part.entity.location.value))),
        DatasetProject(dataset.project.path, dataset.project.name),
        usedIn = List(DatasetProject(dataset.project.path, dataset.project.name)),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )

  implicit def importedInternalToNonModified[P <: Dataset.Provenance.ImportedInternal](implicit
      renkuBaseUrl: RenkuBaseUrl
  ): entities.Dataset[P] => NonModifiedDataset =
    dataset =>
      NonModifiedDataset(
        dataset.identification.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.url,
        dataset.provenance.sameAs,
        DatasetVersions(InitialVersion(dataset.provenance.topmostSameAs.value)),
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(person =>
          DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
        ),
        dataset.provenance.date,
        dataset.parts.map(part => model.DatasetPart(PartLocation(part.entity.location.value))),
        DatasetProject(dataset.project.path, dataset.project.name),
        usedIn = List(DatasetProject(dataset.project.path, dataset.project.name)),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )

  implicit def modifiedToModified(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): entities.Dataset[Dataset.Provenance.Modified] => ModifiedDataset =
    dataset =>
      ModifiedDataset(
        dataset.identifier,
        dataset.identification.title,
        dataset.identification.name,
        dataset.additionalInfo.url,
        dataset.provenance.derivedFrom,
        DatasetVersions(InitialVersion(dataset.provenance.topmostSameAs.value)),
        dataset.additionalInfo.maybeDescription,
        dataset.provenance.creators.map(person =>
          DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
        ),
        dataset.provenance.date,
        dataset.parts.map(part => model.DatasetPart(PartLocation(part.entity.location.value))),
        DatasetProject(dataset.project.path, dataset.project.name),
        usedIn = List(DatasetProject(dataset.project.path, dataset.project.name)),
        dataset.additionalInfo.keywords.sorted,
        dataset.additionalInfo.images
      )
}
