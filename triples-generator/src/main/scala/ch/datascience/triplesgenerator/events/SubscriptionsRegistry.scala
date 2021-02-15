package ch.datascience.triplesgenerator.events

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import cats.{MonadError, Parallel}
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.triplesgenerator.events.EventSchedulingResult.UnsupportedEventType
import ch.datascience.triplesgenerator.events.subscriptions.SubscriptionMechanism
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

trait SubscriptionsRegistry[Interpretation[_]] {
  def handle(requestContent: EventRequestContent): Interpretation[EventSchedulingResult]
  def renewAllSubscriptions(): Interpretation[Unit]
  def run():                   Interpretation[Unit]
}

class SubscriptionsRegistryImpl[Interpretation[_]](eventHandlers:           List[EventHandler[Interpretation]],
                                                   subscriptionsMechanisms: List[SubscriptionMechanism[Interpretation]]
)(implicit
    ME:       MonadError[Interpretation, Throwable],
    parallel: Parallel[Interpretation]
) extends SubscriptionsRegistry[Interpretation] {
  override def handle(requestContent: EventRequestContent): Interpretation[EventSchedulingResult] =
    tryNextHandler(requestContent, eventHandlers)

  private def tryNextHandler(requestContent: EventRequestContent,
                             handlers:       List[EventHandler[Interpretation]]
  ): Interpretation[EventSchedulingResult] =
    handlers.headOption match {
      case Some(handler) =>
        handler.handle(requestContent).flatMap {
          case UnsupportedEventType => tryNextHandler(requestContent, handlers.tail)
          case otherResult          => otherResult.pure[Interpretation]
        }
      case None =>
        (UnsupportedEventType: EventSchedulingResult).pure[Interpretation]
    }

  def subscriptionMechanism(categoryName: CategoryName): Interpretation[SubscriptionMechanism[Interpretation]] =
    subscriptionsMechanisms
      .find(_.categoryName == categoryName)
      .map(ME.pure)
      .getOrElse(ME.raiseError(new IllegalStateException(s"No SubscriptionMechanism for $categoryName")))

  def run(): Interpretation[Unit] = subscriptionsMechanisms.map(_.run()).parSequence.void

  def renewAllSubscriptions(): Interpretation[Unit] =
    subscriptionsMechanisms.map(_.renewSubscription()).parSequence.void
}

object SubscriptionsRegistry {
  def apply(logger:     Logger[IO], subscriptionFactories: (EventHandler[IO], SubscriptionMechanism[IO])*)(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[SubscriptionsRegistry[IO]] =
    IO(
      new SubscriptionsRegistryImpl[IO](subscriptionFactories.toList.map(_._1), subscriptionFactories.toList.map(_._2))
    )
}
