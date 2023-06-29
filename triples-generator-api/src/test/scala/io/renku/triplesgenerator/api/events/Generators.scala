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

package io.renku.triplesgenerator.api.events

import cats.effect.Sync
import cats.syntax.all._
import io.renku.compression.Zip
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.generators.jsonld.JsonLDGenerators.jsonLDEntities
import io.renku.graph.model.RenkuTinyTypeGenerators._
import io.renku.graph.model.events.ZippedEventPayload
import org.scalacheck.Gen

object Generators {

  val cleanUpEvents: Gen[CleanUpEvent] =
    consumerProjects.map(CleanUpEvent.apply)

  val datasetViewedEvents: Gen[DatasetViewedEvent] =
    (datasetIdentifiers, datasetViewedDates(), personGitLabIds.toGeneratorOfOptions).mapN(DatasetViewedEvent.apply)

  val projectActivatedEvents: Gen[ProjectActivated] =
    (projectPaths -> timestampsNotInTheFuture.toGeneratorOf(ProjectActivated.DateActivated))
      .mapN(ProjectActivated.apply)

  val userIds: Gen[UserId] = Gen.oneOf(personGitLabIds.map(UserId(_)), personEmails.map(UserId(_)))

  val projectViewedEvents: Gen[ProjectViewedEvent] =
    (projectPaths, projectViewedDates(), userIds.toGeneratorOfOptions).mapN(ProjectViewedEvent.apply)

  val projectViewingDeletions: Gen[ProjectViewingDeletion] =
    projectPaths.map(ProjectViewingDeletion.apply)

  def zippedEventPayloads[F[_]: Sync]: F[Gen[ZippedEventPayload]] =
    jsonLDEntities.map(_.toJson.noSpaces).map(Zip.zip[F](_).map(ZippedEventPayload)).sequence

  def syncRepoMetadataEvents[F[_]: Sync]: F[Gen[SyncRepoMetadata]] =
    zippedEventPayloads[F]
      .map(payloadsGen => (projectPaths -> payloadsGen.toGeneratorOfOptions).mapN(SyncRepoMetadata.apply))

  def syncRepoMetadataWithPayloadEvents[F[_]: Sync]: F[Gen[SyncRepoMetadata]] =
    zippedEventPayloads[F]
      .map(payloadsGen => (projectPaths -> payloadsGen.toGeneratorOfSomes).mapN(SyncRepoMetadata.apply))

  val syncRepoMetadataWithoutPayloadEvents: Gen[SyncRepoMetadata] =
    projectPaths.map(SyncRepoMetadata(_, maybePayload = None))
}
