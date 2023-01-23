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

package io.renku.cli.model.generators

import io.renku.cli.model.CliDataset
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.images.{Image, ImagePosition, ImageResourceId}
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalacheck.Gen

trait DatasetGenerators {

  def datasetGen(implicit renkuUrl: RenkuUrl): Gen[CliDataset] =
    for {
      identifier         <- RenkuTinyTypeGenerators.datasetIdentifiers
      resourceId         <- RenkuTinyTypeGenerators.datasetResourceIds(identifier)
      title              <- RenkuTinyTypeGenerators.datasetTitles
      name               <- RenkuTinyTypeGenerators.datasetNames
      createdOrPublished <- RenkuTinyTypeGenerators.datasetDates
      creators           <- PersonGenerators.cliPersonGen.toGeneratorOfNonEmptyList(max = 3)
      descr              <- RenkuTinyTypeGenerators.datasetDescriptions.toGeneratorOfOptions
      keywords           <- RenkuTinyTypeGenerators.datasetKeywords.toGeneratorOfList(max = 3)
      imageUris          <- RenkuTinyTypeGenerators.imageUris.toGeneratorOfList(max = 3)
      images = imageUris.zipWithIndex.map { case (uri, index) =>
                 Image(ImageResourceId(s"${resourceId.value}/images/$index"), uri, ImagePosition(index))
               }
      license       <- Gen.option(RenkuTinyTypeGenerators.datasetLicenses)
      version       <- Gen.option(RenkuTinyTypeGenerators.datasetVersions)
      files         <- DatasetFileGenerators.datasetFileGen(createdOrPublished.instant).toGeneratorOfList(max = 3)
      modifiedDate  <- RenkuTinyTypeGenerators.datasetModifiedDates(createdOrPublished).toGeneratorOfOptions
      sameAs        <- BaseGenerators.datasetSameAs.toGeneratorOfOptions
      derivedFrom   <- RenkuTinyTypeGenerators.datasetDerivedFroms.toGeneratorOfOptions
      originalIdent <- RenkuTinyTypeGenerators.datasetOriginalIdentifiers.toGeneratorOfOptions
      invalidTime   <- RenkuTinyTypeGenerators.invalidationTimes(createdOrPublished.instant).toGeneratorOfOptions
    } yield CliDataset(
      resourceId = resourceId,
      identifier = identifier,
      title = title,
      name = name,
      createdOrPublished = createdOrPublished,
      creators = creators,
      description = descr,
      keywords = keywords.sorted,
      images = images,
      license = license,
      version = version,
      datasetFiles = files,
      dateModified = modifiedDate,
      sameAs = sameAs,
      derivedFrom = derivedFrom,
      originalIdentifier = originalIdent,
      invalidationTime = invalidTime,
      publicationEvents = Nil
    )
}

object DatasetGenerators extends DatasetGenerators
