package io.renku.eventlog.init

import cats.effect.Bracket
import ch.datascience.db.DbTransactor
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

trait EventStatusRenamer[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private case class EventStatusRenamerImpl[Interpretation[_]](transactor: DbTransactor[Interpretation, EventLogDB],
                                                             logger:     Logger[Interpretation]
) extends EventStatusRenamer[Interpretation] {
  override def run(): Interpretation[Unit] = ???
}

private object EventStatusRenamer {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventStatusRenamer[Interpretation] =
    new EventStatusRenamerImpl(transactor, logger)
}
