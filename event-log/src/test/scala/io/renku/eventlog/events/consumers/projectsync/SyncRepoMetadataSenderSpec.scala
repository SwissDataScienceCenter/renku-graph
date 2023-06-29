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

package io.renku.eventlog.events.consumers.projectsync

import Generators.projectSyncEvents
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.EventsGenerators.zippedEventPayloads
import io.renku.graph.model.events.ZippedEventPayload
import io.renku.graph.model.projects
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.SyncRepoMetadata
import org.scalamock.scalatest.MockFactory
import org.scalatest.TryValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.util.Try

class SyncRepoMetadataSenderSpec extends AnyFlatSpec with should.Matchers with MockFactory with TryValues {

  it should "find latest project event payload and send a SYNC_REPO_METADATA event to TG" in {

    val event = projectSyncEvents.generateOne

    val payload = zippedEventPayloads.generateOption
    givenPayloadFinding(event.projectId, returning = payload.pure[Try])

    givenEventSending(event.projectPath, payload, returning = ().pure[Try])

    sender.sendSyncRepoMetadata(event).success.value shouldBe ()
  }

  private lazy val payloadFinder = mock[PayloadFinder[Try]]
  private lazy val tgClient      = mock[triplesgenerator.api.events.Client[Try]]
  private lazy val sender        = new SyncRepoMetadataSenderImpl[Try](payloadFinder, tgClient)

  private def givenPayloadFinding(projectId: projects.GitLabId, returning: Try[Option[ZippedEventPayload]]) =
    (payloadFinder.findLatestPayload _).expects(projectId).returning(returning)

  private def givenEventSending(projectPath:  projects.Path,
                                maybePayload: Option[ZippedEventPayload],
                                returning:    Try[Unit]
  ) = (tgClient
    .send(_: SyncRepoMetadata))
    .expects(SyncRepoMetadata(projectPath, maybePayload))
    .returning(returning)
}
