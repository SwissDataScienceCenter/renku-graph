/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import io.renku.cli.model.{CliDataset, CliPublicationEvent}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.{RenkuUrl, datasets}
import org.scalacheck.Gen

import java.time.Instant

trait PublicationEventGenerators {

  def publicationEventGen(dataset: CliDataset)(implicit renkuUrl: RenkuUrl): Gen[CliPublicationEvent] =
    publicationEventGen(dataset.identifier, dataset.dateModified.value)

  def publicationEventGen(
      datasetIdentifier: datasets.Identifier,
      dsDate:            Instant
  )(implicit renkuUrl: RenkuUrl): Gen[CliPublicationEvent] = for {
    name              <- publicationEventNames
    datasetResourceId <- datasetResourceIds(datasetIdentifier)
    resourceId        <- publicationEventResourceIds(name, datasetResourceId)
    about             <- publicationEventAbout(datasetIdentifier)
    maybeDesc         <- publicationEventDesc.toGeneratorOfOptions
    startDate         <- timestampsNotInTheFuture(butYoungerThan = dsDate)
  } yield CliPublicationEvent(resourceId, about, datasetResourceId, maybeDesc, name, startDate)
}

object PublicationEventGenerators extends PublicationEventGenerators
