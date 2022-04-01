package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations.tooling

import ConditionedMigration.MigrationRequired
import Generators._
import cats.syntax.all._
import io.renku.events.EventRequestContent
import io.renku.events.Generators._
import io.renku.events.producers.EventSender
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import io.renku.graph.model.projects
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling.QueryBasedMigration.EventData
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class QueryBasedMigrationSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "required" should {

    "return Yes if Migration Execution Register cannot find any past executions" in new TestCase {
      (executionRegister.findExecution _)
        .expects(migration.name)
        .returning(None.pure[Try])

      migration.required.value shouldBe MigrationRequired.Yes("was not executed yet").asRight.pure[Try]
    }

    "return No if Migration Execution Register finds a past version" in new TestCase {
      val version = serviceVersions.generateOne
      (executionRegister.findExecution _)
        .expects(migration.name)
        .returning(version.some.pure[Try])

      migration.required.value shouldBe MigrationRequired.No(s"was executed on $version").asRight.pure[Try]
    }
  }

  "migrate" should {

    "run find records and send an event for each of the records" in new TestCase {
      val records = projectPaths.generateNonEmptyList().toList

      (recordsFinder.findRecords _).expects().returning(records.pure[Try])

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
  }

  "postMigration" should {

    "update the Execution Register" in new TestCase {

      (executionRegister.registerExecution _)
        .expects(migration.name)
        .returning(().pure[Try])

      migration.postMigration().value shouldBe ().asRight.pure[Try]
    }
  }

  private trait TestCase {

    val eventCategoryName = categoryNames.generateOne

    private implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val recordsFinder     = mock[RecordsFinder[Try]]
    val eventProducer     = mockFunction[projects.Path, EventData]
    val eventSender       = mock[EventSender[Try]]
    val executionRegister = mock[MigrationExecutionRegister[Try]]
    val migration = new QueryBasedMigration[Try](migrationNames.generateOne,
                                                 recordsFinder,
                                                 eventProducer,
                                                 eventSender,
                                                 executionRegister
    )
  }
}
