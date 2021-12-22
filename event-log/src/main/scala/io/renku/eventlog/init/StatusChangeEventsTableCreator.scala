package io.renku.eventlog.init

import cats.data.Kleisli
import cats.effect.MonadCancelThrow
import io.renku.db.SessionResource
import io.renku.eventlog.EventLogDB
import org.typelevel.log4cats.Logger

private trait StatusChangeEventsTableCreator[F[_]] {
  def run(): F[Unit]
}

private object StatusChangeEventsTableCreator {
  def apply[F[_]: MonadCancelThrow: Logger](
      sessionResource: SessionResource[F, EventLogDB]
  ): StatusChangeEventsTableCreator[F] = new StatusChangeEventsTableCreatorImpl(sessionResource)
}

private class StatusChangeEventsTableCreatorImpl[F[_]: MonadCancelThrow: Logger](
    sessionResource: SessionResource[F, EventLogDB]
) extends StatusChangeEventsTableCreator[F] {

  import cats.syntax.all._
  import skunk._
  import skunk.codec.all._
  import skunk.implicits._

  override def run(): F[Unit] = sessionResource.useK {
    checkTableExists flatMap {
      case true  => Kleisli.liftF(Logger[F] info "'status_change_events_queue' table exists")
      case false => createTable()
    }
  }

  private def checkTableExists: Kleisli[F, Session[F], Boolean] = {
    val query: Query[Void, Boolean] =
      sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'status_change_events_queue')".query(bool)
    Kleisli(_.unique(query).recover { case _ => false })
  }

  private def createTable(): Kleisli[F, Session[F], Unit] = for {
    _ <- execute(createTableSql)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_date ON status_change_events_queue(date)".command)
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_type ON status_change_events_queue(event_type)".command)
    _ <- Kleisli.liftF(Logger[F] info "'status_change_events_queue' table created")
  } yield ()

  private lazy val createTableSql: Command[Void] = sql"""
    CREATE TABLE IF NOT EXISTS status_change_events_queue(
      id         SERIAL    PRIMARY KEY,
      date       TIMESTAMP NOT NULL,
      event_type VARCHAR   NOT NULL,
      payload    TEXT      NOT NULL
    );
    """.command
}
