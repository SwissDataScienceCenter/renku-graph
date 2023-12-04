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

package io.renku.graph.model.cli

import cats.syntax.all._
import io.renku.cli.model._
import io.renku.graph.model.testentities.Dataset.DatasetImagesOps
import io.renku.graph.model.testentities.HavingInvalidationTime
import io.renku.graph.model.{RenkuUrl, datasets, publicationEvents, testentities}
import io.renku.jsonld.syntax._

trait CliDatasetConverters extends CliCommonConverters {

  def from(dataset: testentities.Dataset[testentities.Dataset.Provenance])(implicit renkuUrl: RenkuUrl): CliDataset = {
    val id = datasets.ResourceId(dataset.asEntityId.show)
    CliDataset(
      resourceId = id,
      identifier = dataset.identification.identifier,
      name = dataset.identification.name,
      slug = dataset.identification.slug,
      createdOrPublished = dataset.provenance.date,
      dateModified = datasets.DateModified(dataset.provenance.date),
      creators = dataset.provenance.creators.map(from),
      description = dataset.additionalInfo.maybeDescription,
      keywords = dataset.additionalInfo.keywords,
      images = dataset.additionalInfo.images.toEntitiesImages(id).toList,
      license = dataset.additionalInfo.maybeLicense,
      version = dataset.additionalInfo.maybeVersion,
      datasetFiles = dataset.parts.map(from),
      sameAs = dataset.provenance match {
        case p: testentities.Dataset.Provenance.ImportedExternal => p.sameAs.some
        case p: testentities.Dataset.Provenance.ImportedInternal => p.sameAs.some
        case _: testentities.Dataset.Provenance.Internal         => None
        case _: testentities.Dataset.Provenance.Modified         => None
      },
      derivedFrom = dataset.provenance match {
        case m: testentities.Dataset.Provenance.Modified => m.derivedFrom.some
        case _ => None
      },
      originalIdentifier = dataset.provenance.originalIdentifier.some,
      invalidationTime = dataset.provenance match {
        case m: testentities.Dataset.Provenance.Modified => m.maybeInvalidationTime
        case _ => None
      },
      dataset.publicationEvents.map(from)
    )
  }

  def from(part: testentities.DatasetPart)(implicit renkuUrl: RenkuUrl): CliDatasetFile =
    CliDatasetFile(
      datasets.PartResourceId(part.asEntityId.show),
      part.external,
      from(part.entity),
      part.dateCreated,
      part.maybeSource,
      part match {
        case p: HavingInvalidationTime =>
          p.invalidationTime.some
        case _ => None
      }
    )

  def from(pe: testentities.PublicationEvent)(implicit renkuUrl: RenkuUrl): CliPublicationEvent = {
    val id    = publicationEvents.ResourceId(pe.asEntityId.show)
    val about = publicationEvents.About((renkuUrl / "urls" / "datasets" / pe.dataset.identification.identifier).show)
    val resId = datasets.ResourceId(pe.dataset.asEntityId.show)
    CliPublicationEvent(id, about, resId, pe.maybeDescription, pe.name, pe.startDate)
  }
}

object CliDatasetConverters extends CliDatasetConverters
