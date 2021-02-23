package io.renku.eventlog.init

import cats.effect.{Bracket, IO}
import ch.datascience.db.DbTransactor
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait EventDeliveryTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class EventDeliveryTableCreatorImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends EventDeliveryTableCreator[Interpretation] {

  import cats.syntax.all._
  import doobie.implicits._
  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true  => logger info "'event_delivery' table exists"
      case false => createTable
    }

  private def checkTableExists: Interpretation[Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'event_delivery')"
      .query[Boolean]
      .unique
      .transact(transactor.get)
      .recover { case _ => false }

  private def createTable = for {
    _ <- createTableSql.run transact transactor.get
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_event_id       ON status_processing_time(event_id)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_project_id     ON status_processing_time(project_id)")
    _ <- logger info "'event_delivery' table created"
    _ <- foreignKeySql.run transact transactor.get
  } yield ()

  private lazy val createTableSql = sql"""
    CREATE TABLE IF NOT EXISTS event_delivery(
      event_id          varchar    NOT NULL,
      project_id        int4       NOT NULL,
      delivery_url      varchar    NOT NULL,
      PRIMARY KEY (event_id, project_id)
    );
    """.update

  private lazy val foreignKeySql = sql"""
    ALTER TABLE event_delivery
    ADD CONSTRAINT fk_event FOREIGN KEY (event_id, project_id) REFERENCES event (event_id, project_id);
  """.update
}

private object EventDeliveryTableCreator {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): EventDeliveryTableCreator[Interpretation] =
    new EventDeliveryTableCreatorImpl(transactor, logger)
}
