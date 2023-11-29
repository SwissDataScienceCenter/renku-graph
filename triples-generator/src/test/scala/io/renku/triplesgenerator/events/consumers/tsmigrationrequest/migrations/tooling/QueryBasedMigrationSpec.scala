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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations.tooling

import Generators._
import QueryBasedMigration.EventData
import cats.MonadThrow
import cats.effect.IO
import cats.syntax.all._
import io.renku.events.EventRequestContent
import io.renku.events.Generators._
import io.renku.events.producers.EventSender
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.errors.ErrorGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class QueryBasedMigrationSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "QueryBasedMigration" should {

    "be the RegisteredMigration" in new TestCase {
      migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[IO]]
    }
  }

  "migrate" should {

    "run find records and send an event for each of the found projects" in new TestCase {
      val records = projectSlugs.generateNonEmptyList().toList

      (() => projectsFinder.findProjects).expects().returning(records.pure[IO])

      records foreach { record =>
        val event = eventRequestContentNoPayloads.generateOne

        eventProducer.expects(record).returning((record, event, eventCategoryName))

        (eventSender
          .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
          .expects(event,
                   EventSender.EventContext(eventCategoryName,
                                            show"$categoryName: ${migration.name} cannot send event for $record"
                   )
          )
          .returning(().pure[IO])
      }

      migration.migrate().value.unsafeRunSync() shouldBe ().asRight
    }

    "return a Recoverable Error if in case of an exception while finding project " +
      "the given strategy returns one" in new TestCase {
        val exception = exceptions.generateOne

        (() => projectsFinder.findProjects).expects().returning(exception.raiseError[IO, List[projects.Slug]])

        migration.migrate().value.unsafeRunSync() shouldBe recoverableError.asLeft
      }

    "return a Recoverable Error if in case of an exception while sending events " +
      "the given strategy returns one" in new TestCase {

        val record = projectSlugs.generateOne
        (() => projectsFinder.findProjects).expects().returning(List(record).pure[IO])

        val event = eventRequestContentNoPayloads.generateOne
        eventProducer.expects(record).returning((record, event, eventCategoryName))

        val exception = exceptions.generateOne
        (eventSender
          .sendEvent(_: EventRequestContent.NoPayload, _: EventSender.EventContext))
          .expects(
            event,
            EventSender.EventContext(eventCategoryName,
                                     show"$categoryName: ${migration.name} cannot send event for $record"
            )
          )
          .returning(exception.raiseError[IO, Unit])

        migration.migrate().value.unsafeRunSync() shouldBe recoverableError.asLeft
      }
  }

  private trait TestCase {

    val eventCategoryName = categoryNames.generateOne

    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val projectsFinder    = mock[ProjectsFinder[IO]]
    val eventProducer     = mockFunction[projects.Slug, EventData]
    val eventSender       = mock[EventSender[IO]]
    val executionRegister = mock[MigrationExecutionRegister[IO]]
    val recoverableError  = processingRecoverableErrors.generateOne
    val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new QueryBasedMigration[IO](migrationNames.generateOne,
                                                exclusive = false,
                                                projectsFinder,
                                                eventProducer,
                                                eventSender,
                                                executionRegister,
                                                recoveryStrategy
    )
  }
}
