package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations.tooling

import ConditionedMigration.MigrationRequired
import QueryBasedMigration.EventData
import cats.MonadThrow
import cats.data.EitherT
import cats.syntax.all._
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.model.projects
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private[migrations] class QueryBasedMigration[F[_]: MonadThrow: Logger](
    val name:          Migration.Name,
    recordsFinder:     RecordsFinder[F],
    eventProducer:     projects.Path => EventData,
    eventSender:       EventSender[F],
    executionRegister: MigrationExecutionRegister[F],
    recoveryStrategy:  RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends ConditionedMigration[F] {

  import recordsFinder._
  import recoveryStrategy._

  protected[tooling] override def required: EitherT[F, ProcessingRecoverableError, MigrationRequired] = EitherT {
    executionRegister
      .findExecution(name)
      .map {
        case Some(serviceVersion) => MigrationRequired.No(s"was executed on $serviceVersion")
        case None                 => MigrationRequired.Yes("was not executed yet")
      }
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, MigrationRequired])
  }

  protected[tooling] override def migrate(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    (findRecords().map(toEvents) >>= sendEvents)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }

  lazy val toEvents: List[projects.Path] => List[EventData] = _.map(eventProducer)

  lazy val sendEvents: List[EventData] => F[Unit] = _.map { case (path, event, eventCategory) =>
    eventSender.sendEvent(
      event,
      EventSender.EventContext(eventCategory, show"$categoryName: $name cannot send event for $path")
    )
  }.sequence.void

  protected[tooling] override def postMigration(): EitherT[F, ProcessingRecoverableError, Unit] = EitherT {
    executionRegister
      .registerExecution(name)
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(maybeRecoverableError[F, Unit])
  }
}

private[migrations] object QueryBasedMigration {
  type EventData = (projects.Path, EventRequestContent.NoPayload, CategoryName)
}
