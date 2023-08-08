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

package io.renku.knowledgegraph.datasets

import cats.syntax.all._
import io.renku.graph.model.datasets.{CreatedOrPublished, DateModified, PartLocation, ResourceId, SameAs}
import io.renku.graph.model.testentities.{HavingInvalidationTime, RenkuProject, Dataset => ModelDataset}
import io.renku.graph.model.{RenkuUrl, testentities}
import io.renku.jsonld.syntax._

package object details {
  import Dataset._

  private[details] def internalToNonModified(dataset: ModelDataset[ModelDataset.Provenance.Internal],
                                             project: RenkuProject
  )(implicit renkuUrl: RenkuUrl): NonModifiedDataset = NonModifiedDataset(
    ResourceId(dataset.asEntityId.show),
    dataset.identification.title,
    dataset.identification.name,
    SameAs(dataset.entityId),
    DatasetVersions(dataset.provenance.originalIdentifier),
    maybeInitialTag = None,
    dataset.additionalInfo.maybeDescription,
    dataset.provenance.creators.map(personToCreator).sortBy(_.name).toList,
    dataset.provenance.date,
    dataset.parts.map(part => DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
    toDatasetProject(project, dataset),
    usedIn = List(toDatasetProject(project, dataset)),
    dataset.additionalInfo.keywords.sorted,
    dataset.additionalInfo.images
  )

  private[details] def importedExternalToNonModified(dataset: ModelDataset[ModelDataset.Provenance.ImportedExternal],
                                                     project: RenkuProject
  )(implicit renkuUrl: RenkuUrl): NonModifiedDataset = NonModifiedDataset(
    ResourceId(dataset.asEntityId.show),
    dataset.identification.title,
    dataset.identification.name,
    dataset.provenance.sameAs,
    DatasetVersions(dataset.provenance.originalIdentifier),
    maybeInitialTag = None,
    dataset.additionalInfo.maybeDescription,
    dataset.provenance.creators.map(personToCreator).sortBy(_.name).toList,
    dataset.provenance.date,
    dataset.parts.map(part => DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
    toDatasetProject(project, dataset),
    usedIn = List(toDatasetProject(project, dataset)),
    dataset.additionalInfo.keywords.sorted,
    dataset.additionalInfo.images
  )

  private[details] def importedInternalToNonModified(dataset: ModelDataset[ModelDataset.Provenance.ImportedInternal],
                                                     project: RenkuProject
  )(implicit renkuUrl: RenkuUrl): NonModifiedDataset = NonModifiedDataset(
    ResourceId(dataset.asEntityId.show),
    dataset.identification.title,
    dataset.identification.name,
    dataset.provenance.sameAs,
    DatasetVersions(dataset.provenance.originalIdentifier),
    maybeInitialTag = None,
    dataset.additionalInfo.maybeDescription,
    dataset.provenance.creators.map(personToCreator).sortBy(_.name).toList,
    dataset.provenance.date,
    dataset.parts.map(part => DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
    toDatasetProject(project, dataset),
    usedIn = List(toDatasetProject(project, dataset)),
    dataset.additionalInfo.keywords.sorted,
    dataset.additionalInfo.images
  )

  private[details] def modifiedToModified(dataset:            ModelDataset[ModelDataset.Provenance.Modified],
                                          createdOrPublished: CreatedOrPublished,
                                          project:            RenkuProject
  )(implicit renkuUrl: RenkuUrl): ModifiedDataset = ModifiedDataset(
    ResourceId(dataset.asEntityId.show),
    dataset.identification.title,
    dataset.identification.name,
    dataset.provenance.derivedFrom,
    DatasetVersions(dataset.provenance.originalIdentifier),
    maybeInitialTag = None,
    dataset.additionalInfo.maybeDescription,
    dataset.provenance.creators.map(personToCreator).sortBy(_.name).toList,
    createdOrPublished,
    DateModified(dataset.provenance.date.value),
    dataset.parts
      .filterNot { case _: testentities.DatasetPart with HavingInvalidationTime => true; case _ => false }
      .map(part => DatasetPart(PartLocation(part.entity.location.value)))
      .sortBy(_.location),
    toDatasetProject(project, dataset),
    usedIn = List(toDatasetProject(project, dataset)),
    dataset.additionalInfo.keywords.sorted,
    dataset.additionalInfo.images
  )

  private[details] def toDatasetProject(project: RenkuProject,
                                        dataset: testentities.Dataset[testentities.Dataset.Provenance]
  )(implicit ru: RenkuUrl): DatasetProject =
    DatasetProject(project.resourceId,
                   project.slug,
                   project.name,
                   project.visibility,
                   dataset.identification.identifier
    )
}
