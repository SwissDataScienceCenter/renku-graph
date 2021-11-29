package io.renku.triplesgenerator.events.categories.cleanup

import cats.effect.{Async, MonadCancelThrow}
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.http.client.RestClient
import org.typelevel.log4cats.Logger

private trait EventLogNotifier[F[_]] {
  def notifyEventLog(project: Project): F[Unit]
}
private object EventLogNotifier {
  def apply[F[_]: Async: Logger] = MonadCancelThrow[F].catchNonFatal(new EventLogNotifierImpl[F])
}
private class EventLogNotifierImpl[F[_]: Async: Logger]()
    extends RestClient(Throttler.noThrottling[F])
    with EventLogNotifier[F] {
  override def notifyEventLog(project: Project): F[Unit] = ???
}
