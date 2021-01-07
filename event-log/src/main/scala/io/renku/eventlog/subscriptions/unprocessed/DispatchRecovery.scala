package io.renku.eventlog.subscriptions.unprocessed

import cats.effect.{Bracket, IO, Timer}
import cats.syntax.all._
import ch.datascience.db.{DbTransactor, SqlQuery}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import doobie.util.transactor.Transactor
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.statuschange.{IOUpdateCommandsRunner, StatusUpdatesRunner}
import io.renku.eventlog.statuschange.commands.{ChangeStatusCommand, ToGenerationNonRecoverableFailure, UpdateResult}
import io.renku.eventlog.subscriptions.IOEventsDistributor.{NoEventSleep, OnErrorSleep}
import io.renku.eventlog.subscriptions.{DispatchRecovery, EventsDistributorImpl, IOEventsSender, SubscriberUrl}
import io.renku.eventlog.{EventLogDB, EventMessage, subscriptions}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

private class DispatchRecoveryImpl[Interpretation[_]](
    underTriplesGenerationGauge: LabeledGauge[Interpretation, projects.Path],
    statusUpdatesRunner:         StatusUpdatesRunner[Interpretation],
    logger:                      Logger[Interpretation]
)(implicit ME:                   Bracket[Interpretation, Throwable], timer: Timer[Interpretation])
    extends subscriptions.DispatchRecovery[Interpretation, UnprocessedEvent] {

  private val OnErrorSleep: FiniteDuration = 1 seconds

  override def recover(
      url:           SubscriberUrl,
      categoryEvent: UnprocessedEvent
  ): PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    val markEventFailed =
      ToGenerationNonRecoverableFailure[Interpretation](categoryEvent.id,
                                                        EventMessage(exception),
                                                        underTriplesGenerationGauge
      )
    for {
      _ <- statusUpdatesRunner run markEventFailed recoverWith retry(markEventFailed)
      _ <- logger.error(exception)(s"Event $categoryEvent, url = $url -> ${markEventFailed.status}")
    } yield ()
  }

  private def retry(
      command: ChangeStatusCommand[Interpretation]
  ): PartialFunction[Throwable, Interpretation[UpdateResult]] = { case NonFatal(exception) =>
    {
      for {
        _      <- logger.error(exception)(s"Marking event as ${command.status} failed")
        _      <- timer sleep OnErrorSleep
        result <- statusUpdatesRunner run command
      } yield result
    } recoverWith retry(command)
  }
}

private object DispatchRecovery {
  def apply(transactor:                  DbTransactor[IO, EventLogDB],
            underTriplesGenerationGauge: LabeledGauge[IO, projects.Path],
            queriesExecTimes:            LabeledHistogram[IO, SqlQuery.Name],
            logger:                      Logger[IO]
  )(implicit timer:                      Timer[IO]): IO[DispatchRecovery[IO, UnprocessedEvent]] = for {
    updateCommandRunner <- IOUpdateCommandsRunner(transactor, queriesExecTimes, logger)
  } yield new DispatchRecoveryImpl[IO](underTriplesGenerationGauge, updateCommandRunner, logger)
}
