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

package io.renku.entities.viewings.collector
package datasets

import cats.syntax.all._
import io.renku.entities.viewings.collector
import io.renku.entities.viewings.collector.persons.{GLUserViewedDataset, PersonViewedDatasetPersister}
import io.renku.generators.Generators.Implicits._
import projects.viewed.EventPersister
import io.renku.graph.model.{datasets, projects}
import io.renku.graph.model.RenkuTinyTypeGenerators.{datasetIdentifiers, datasetResourceIds, personGitLabIds, projectSlugs}
import io.renku.triplesgenerator.api.events.{DatasetViewedEvent, ProjectViewedEvent, UserId}
import io.renku.triplesgenerator.api.events.Generators.datasetViewedEvents
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.TryValues

import scala.util.Try

class EventUploaderSpec extends AnyWordSpec with should.Matchers with MockFactory with TryValues {

  "upload" should {

    "find the project which the event DS viewing should be accounted for, " +
      "store a relevant ProjectViewedEvent and " +
      "a UserViewedDataset event if userId given" in new TestCase {

        val event = datasetViewedEvents.generateOne.copy(maybeUserId = personGitLabIds.generateSome)

        val dsInfo = dsInfos.generateOne
        givenProjectFinding(event.identifier, returning = dsInfo.some.pure[Try])

        givenEventPersisting(
          ProjectViewedEvent(dsInfo.projectSlug,
                             projects.DateViewed(event.dateViewed.value),
                             event.maybeUserId.map(UserId(_))
          ),
          returning = ().pure[Try]
        )

        givenPersistingUserViewedEvent(event, dsInfo, returning = ().pure[Try])

        uploader.upload(event).success.value shouldBe ()
      }

    "find the project which the event DS viewing should be accounted for " +
      "and store a relevant ProjectViewedEvent " +
      "without sending a UserViewedDataset event if userId not given" in new TestCase {

        val event = datasetViewedEvents.generateOne.copy(maybeUserId = None)

        val dsInfo = dsInfos.generateOne
        givenProjectFinding(event.identifier, returning = dsInfo.some.pure[Try])

        givenEventPersisting(
          ProjectViewedEvent(dsInfo.projectSlug,
                             projects.DateViewed(event.dateViewed.value),
                             event.maybeUserId.map(UserId(_))
          ),
          returning = ().pure[Try]
        )

        uploader.upload(event).success.value shouldBe ()
      }

    "don't store any event if no Project can be found for the DS" in new TestCase {

      val event = datasetViewedEvents.generateOne

      givenProjectFinding(event.identifier, returning = None.pure[Try])

      uploader.upload(event).success.value shouldBe ()
    }
  }

  private trait TestCase {

    private val projectFinder                = mock[DSInfoFinder[Try]]
    private val eventPersister               = mock[EventPersister[Try]]
    private val personViewedDatasetPersister = mock[PersonViewedDatasetPersister[Try]]
    val uploader = new EventUploaderImpl[Try](projectFinder, eventPersister, personViewedDatasetPersister)

    def givenProjectFinding(identifier: datasets.Identifier, returning: Try[Option[DSInfo]]) =
      (projectFinder.findDSInfo _).expects(identifier).returning(returning)

    def givenEventPersisting(event: ProjectViewedEvent, returning: Try[Unit]) =
      (eventPersister.persist _).expects(event).returning(returning)

    def givenPersistingUserViewedEvent(event: DatasetViewedEvent, dsInfo: DSInfo, returning: Try[Unit]) =
      event.maybeUserId
        .map(UserId(_))
        .map(GLUserViewedDataset(_, dsInfo.dataset, event.dateViewed))
        .map((personViewedDatasetPersister.persist _).expects(_).returning(returning))
  }

  private lazy val dsInfos: Gen[DSInfo] =
    (projectSlugs         ->
      (datasetResourceIds -> datasetIdentifiers).mapN(collector.persons.Dataset))
      .mapN(DSInfo)
}
