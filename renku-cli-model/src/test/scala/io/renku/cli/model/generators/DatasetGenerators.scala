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

import cats.data.NonEmptyList
import io.renku.cli.model.CliDataset
import io.renku.graph.model.images.{Image, ImagePosition, ImageResourceId}
import io.renku.graph.model.{RenkuTinyTypeGenerators, RenkuUrl}
import org.scalacheck.Gen

trait DatasetGenerators {

  def datasetGen(implicit renkuUrl: RenkuUrl): Gen[CliDataset] =
    for {
      resourceId <- RenkuTinyTypeGenerators.datasetResourceIds
      ident      <- RenkuTinyTypeGenerators.datasetIdentifiers
      title      <- RenkuTinyTypeGenerators.datasetTitles
      name       <- RenkuTinyTypeGenerators.datasetNames
      date       <- RenkuTinyTypeGenerators.datasetDates
      creators   <- Gen.choose(1, 3).flatMap(n => Gen.listOfN(n, PersonGenerators.cliPersonGen))
      descr      <- Gen.option(RenkuTinyTypeGenerators.datasetDescriptions)
      keywords   <- Gen.choose(0, 3).flatMap(n => Gen.listOfN(n, RenkuTinyTypeGenerators.datasetKeywords))
      imageUris  <- Gen.choose(0, 3).flatMap(n => Gen.listOfN(n, RenkuTinyTypeGenerators.imageUris))
      images = imageUris.zipWithIndex.map { case (uri, index) =>
                 Image(ImageResourceId(s"${resourceId.value}/images/$index"), uri, ImagePosition(index))
               }
      license       <- Gen.option(RenkuTinyTypeGenerators.datasetLicenses)
      version       <- Gen.option(RenkuTinyTypeGenerators.datasetVersions)
      files         <- Gen.choose(0, 3).flatMap(n => Gen.listOfN(n, DatasetFileGenerators.datasetFileGen(date.instant)))
      modified      <- Gen.option(BaseGenerators.dateModified)
      sameAs        <- Gen.option(BaseGenerators.datasetSameAs)
      derivedFrom   <- Gen.option(RenkuTinyTypeGenerators.datasetDerivedFroms)
      originalIdent <- Gen.option(RenkuTinyTypeGenerators.datasetOriginalIdentifiers)
      invalidTime   <- Gen.option(RenkuTinyTypeGenerators.invalidationTimes(date.instant))
    } yield CliDataset(
      resourceId = resourceId,
      identifier = ident,
      title = title,
      name = name,
      createdOrPublished = date,
      creators = NonEmptyList.fromListUnsafe(creators),
      description = descr,
      keywords = keywords.sorted,
      images = images,
      license = license,
      version = version,
      datasetFiles = files,
      dateModified = modified,
      sameAs = sameAs,
      derivedFrom = derivedFrom,
      originalIdentifier = originalIdent,
      invalidationTime = invalidTime
    )
}

object DatasetGenerators extends DatasetGenerators
