package io.renku.triplesgenerator.events.categories.cleanup

private trait EventProcessor[F[_]] {
  def process(event: CleanUpEvent): F[Unit]
}

private object CleanUpEventProcessor {
  def apply[F[_]](): F[EventProcessor[F]] = ???
}
