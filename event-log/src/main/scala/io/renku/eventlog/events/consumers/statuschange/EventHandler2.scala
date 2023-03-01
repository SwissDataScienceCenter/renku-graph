package io.renku.eventlog.events.consumers.statuschange

import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.eventlog.events.consumers.statuschange.alleventstonew.AllEventsToNew
import io.renku.eventlog.events.consumers.statuschange.projecteventstonew.ProjectEventsToNew
import io.renku.eventlog.events.consumers.statuschange.redoprojecttransformation.RedoProjectTransformation
import io.renku.eventlog.events.consumers.statuschange.rollbacktoawaitingdeletion.RollbackToAwaitingDeletion
import io.renku.eventlog.events.consumers.statuschange.rollbacktonew.RollbackToNew
import io.renku.eventlog.events.consumers.statuschange.rollbacktotriplesgenerated.RollbackToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange.toawaitingdeletion.ToAwaitingDeletion
import io.renku.eventlog.events.consumers.statuschange.tofailure.ToFailure
import io.renku.eventlog.events.consumers.statuschange.totriplesgenerated.ToTriplesGenerated
import io.renku.eventlog.events.consumers.statuschange.totriplesstore.ToTriplesStore
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.consumers.EventSchedulingResult.UnsupportedEventType
import io.renku.events.consumers.{EventSchedulingResult, ProcessExecutor}
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent, consumers}
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger
import skunk.Session

class EventHandler2[F[_]: Async: Logger: MetricsRegistry: QueriesExecutionTimes](
    processExecutor: ProcessExecutor[F],
    statusChanger:   StatusChanger[F],
    eventSender:     EventSender[F],
    eventsQueue:     StatusChangeEventsQueue[F]
) extends consumers.EventHandlerWithProcessLimiter[F](processExecutor) {

  override val categoryName: CategoryName = io.renku.eventlog.events.consumers.statuschange.categoryName

  protected override type Event = StatusChangeEvent

  protected override def createHandlingDefinition(): EventHandlingDefinition =
    EventHandlingDefinition(
      decode = eventDecoder,
      process = statusChanger.updateStatuses(dbUpdater)
    )

  private val dbUpdater: DBUpdater[F, StatusChangeEvent] =
    new DBUpdater[F, StatusChangeEvent] {
      override def updateDB(event: StatusChangeEvent): UpdateResult[F] = event match {
        case ev @ AllEventsToNew               => new alleventstonew.DbUpdater[F](eventSender).updateDB(ev)
        case ev @ ProjectEventsToNew(_)        => new projecteventstonew.DbUpdater[F](eventsQueue).updateDB(ev)
        case ev @ RedoProjectTransformation(_) => new redoprojecttransformation.DbUpdater[F](eventsQueue).updateDB(ev)
        case RollbackToAwaitingDeletion(_)     => ???
        case RollbackToNew(_, _)               => ???
        case RollbackToTriplesGenerated(_, _)  => ???
        case ToAwaitingDeletion(_, _)          => ???
        case ToFailure(_, _, _, _, _, _)       => ???
        case ToTriplesGenerated(_, _, _, _)    => ???
        case ToTriplesStore(_, _, _)           => ???
        // the list must be kept in sync wth subEventDecoders, one slight disadvantage
        // could make a sealed trait in theory
      }

      override def onRollback(event: StatusChangeEvent): Kleisli[F, Session[F], Unit] = ???
    }

  private val subEventDecoders: List[EventRequestContent => Either[DecodingFailure, StatusChangeEvent]] = List(
    RollbackToNew.decoder,
    ToTriplesGenerated.decoder,
    RollbackToTriplesGenerated.decoder,
    ToTriplesStore.decoder,
    ToFailure.decoder,
    ToAwaitingDeletion.decoder,
    RollbackToAwaitingDeletion.decoder,
    RedoProjectTransformation.decoder,
    ProjectEventsToNew.decoder,
    AllEventsToNew.decoder
  )

  private val eventDecoder: EventRequestContent => Either[DecodingFailure, StatusChangeEvent] = req =>
    subEventDecoders.tail.foldLeft(subEventDecoders.head(req)) { (res, f) =>
      res match {
        case Left(_)      => f(req)
        case r @ Right(_) => r
      }
    }

}
