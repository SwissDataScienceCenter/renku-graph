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
import io.renku.graph.model.datasets.{PartLocation, ResourceId, SameAs}
import io.renku.graph.model.testentities.Project._
import io.renku.graph.model.testentities.{Dataset => ModelDataset, HavingInvalidationTime, RenkuProject}
import io.renku.graph.model.{RenkuUrl, testentities}
import io.renku.jsonld.syntax._

package object details {
  import Dataset._

  private[details] implicit def projectToDatasetProject(implicit renkuUrl: RenkuUrl): RenkuProject => DatasetProject =
    project => DatasetProject(project.resourceId, project.path, project.name, project.visibility)

  private[details] def internalToNonModified(dataset: ModelDataset[ModelDataset.Provenance.Internal],
                                             project: RenkuProject
  )(implicit renkuUrl: RenkuUrl): NonModifiedDataset = NonModifiedDataset(
    ResourceId(dataset.asEntityId.show),
    dataset.identification.identifier,
    dataset.identification.title,
    dataset.identification.name,
    SameAs(dataset.entityId),
    DatasetVersions(dataset.provenance.originalIdentifier),
    maybeInitialTag = None,
    dataset.additionalInfo.maybeDescription,
    dataset.provenance.creators.map(personToCreator).sortBy(_.name).toList,
    dataset.provenance.date,
    dataset.parts.map(part => DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
    project.to[DatasetProject],
    usedIn = List(project.to[DatasetProject]),
    dataset.additionalInfo.keywords.sorted,
    dataset.additionalInfo.images
  )

  private[details] def importedExternalToNonModified(dataset: ModelDataset[ModelDataset.Provenance.ImportedExternal],
                                                     project: RenkuProject
  )(implicit renkuUrl: RenkuUrl): NonModifiedDataset = NonModifiedDataset(
    ResourceId(dataset.asEntityId.show),
    dataset.identification.identifier,
    dataset.identification.title,
    dataset.identification.name,
    dataset.provenance.sameAs,
    DatasetVersions(dataset.provenance.originalIdentifier),
    maybeInitialTag = None,
    dataset.additionalInfo.maybeDescription,
    dataset.provenance.creators.map(personToCreator).sortBy(_.name).toList,
    dataset.provenance.date,
    dataset.parts.map(part => DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
    project.to[DatasetProject],
    usedIn = List(project.to[DatasetProject]),
    dataset.additionalInfo.keywords.sorted,
    dataset.additionalInfo.images
  )

  private[details] def importedInternalToNonModified(dataset: ModelDataset[ModelDataset.Provenance.ImportedInternal],
                                                     project: RenkuProject
  )(implicit renkuUrl: RenkuUrl): NonModifiedDataset = NonModifiedDataset(
    ResourceId(dataset.asEntityId.show),
    dataset.identification.identifier,
    dataset.identification.title,
    dataset.identification.name,
    dataset.provenance.sameAs,
    DatasetVersions(dataset.provenance.originalIdentifier),
    maybeInitialTag = None,
    dataset.additionalInfo.maybeDescription,
    dataset.provenance.creators.map(personToCreator).sortBy(_.name).toList,
    dataset.provenance.date,
    dataset.parts.map(part => DatasetPart(PartLocation(part.entity.location.value))).sortBy(_.location),
    project.to[DatasetProject],
    usedIn = List(project.to[DatasetProject]),
    dataset.additionalInfo.keywords.sorted,
    dataset.additionalInfo.images
  )

  private[details] def modifiedToModified(dataset: ModelDataset[ModelDataset.Provenance.Modified],
                                          project: RenkuProject
  )(implicit renkuUrl: RenkuUrl): ModifiedDataset = ModifiedDataset(
    ResourceId(dataset.asEntityId.show),
    dataset.identifier,
    dataset.identification.title,
    dataset.identification.name,
    dataset.provenance.derivedFrom,
    DatasetVersions(dataset.provenance.originalIdentifier),
    maybeInitialTag = None,
    dataset.additionalInfo.maybeDescription,
    dataset.provenance.creators.map(personToCreator).sortBy(_.name).toList,
    dataset.provenance.date,
    dataset.parts
      .filterNot { case _: testentities.DatasetPart with HavingInvalidationTime => true; case _ => false }
      .map(part => DatasetPart(PartLocation(part.entity.location.value)))
      .sortBy(_.location),
    project.to[DatasetProject],
    usedIn = List(project.to[DatasetProject]),
    dataset.additionalInfo.keywords.sorted,
    dataset.additionalInfo.images
  )
}
