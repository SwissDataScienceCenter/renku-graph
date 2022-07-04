/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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
import cats.syntax.all._
import io.renku.events.EventRequestContent
import io.renku.events.Generators._
import io.renku.events.producers.EventSender
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.generators.ErrorGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class QueryBasedMigrationSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "QueryBasedMigration" should {

    "be the RegisteredMigration" in new TestCase {
      migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[Try]]
    }
  }

  "migrate" should {

    "run find records and send an event for each of the found projects" in new TestCase {
      val records = projectPaths.generateNonEmptyList().toList

      (projectsFinder.findProjects _).expects().returning(records.pure[Try])

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
          .returning(().pure[Try])
      }

      migration.migrate().value shouldBe ().asRight.pure[Try]
    }

    "return a Recoverable Error if in case of an exception while finding project " +
      "the given strategy returns one" in new TestCase {
        val exception = exceptions.generateOne

        (projectsFinder.findProjects _).expects().returning(exception.raiseError[Try, List[projects.Path]])

        migration.migrate().value shouldBe recoverableError.asLeft.pure[Try]
      }

    "return a Recoverable Error if in case of an exception while sending events " +
      "the given strategy returns one" in new TestCase {

        val record = projectPaths.generateOne
        (projectsFinder.findProjects _).expects().returning(List(record).pure[Try])

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
          .returning(exception.raiseError[Try, Unit])

        migration.migrate().value shouldBe recoverableError.asLeft.pure[Try]
      }
  }

  private trait TestCase {

    val eventCategoryName = categoryNames.generateOne

    private implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val projectsFinder    = mock[ProjectsFinder[Try]]
    val eventProducer     = mockFunction[projects.Path, EventData]
    val eventSender       = mock[EventSender[Try]]
    val executionRegister = mock[MigrationExecutionRegister[Try]]
    val recoverableError  = processingRecoverableErrors.generateOne
    val recoveryStrategy = new RecoverableErrorsRecovery {
      override def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = { _ =>
        recoverableError.asLeft[OUT].pure[F]
      }
    }
    val migration = new QueryBasedMigration[Try](migrationNames.generateOne,
                                                 projectsFinder,
                                                 eventProducer,
                                                 eventSender,
                                                 executionRegister,
                                                 recoveryStrategy
    )
  }
}
