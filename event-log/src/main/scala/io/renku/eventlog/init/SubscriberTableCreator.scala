package io.renku.eventlog.init

import cats.effect.Bracket
import ch.datascience.db.DbTransactor
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB

private trait SubscriberTableCreator[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private class SubscriberTableCreatorImpl[Interpretation[_]](
    transactor: DbTransactor[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
)(implicit ME:  Bracket[Interpretation, Throwable])
    extends SubscriberTableCreator[Interpretation] {

  import cats.syntax.all._
  import doobie.implicits._
  private implicit val transact: DbTransactor[Interpretation, EventLogDB] = transactor

  override def run(): Interpretation[Unit] =
    checkTableExists flatMap {
      case true  => logger info "'subscriber' table exists"
      case false => createTable
    }

  private def checkTableExists: Interpretation[Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'subscriber')"
      .query[Boolean]
      .unique
      .transact(transactor.get)
      .recover { case _ => false }

  private def createTable = for {
    _ <- createTableSql.run transact transactor.get
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_delivery_url ON subscriber(delivery_url)")
    _ <- execute(sql"CREATE INDEX IF NOT EXISTS idx_source_url ON subscriber(source_url)")
    _ <- logger info "'subscriber' table created"
  } yield ()

  private lazy val createTableSql = sql"""
    CREATE TABLE IF NOT EXISTS subscriber(
      delivery_url VARCHAR NOT NULL,
      source_url   VARCHAR NOT NULL,
      PRIMARY KEY (delivery_url, source_url)
    );
    """.update

}

private object SubscriberTableCreator {
  def apply[Interpretation[_]](
      transactor: DbTransactor[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  )(implicit ME:  Bracket[Interpretation, Throwable]): SubscriberTableCreator[Interpretation] =
    new SubscriberTableCreatorImpl(transactor, logger)
}
