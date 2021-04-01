package io.renku.eventlog.init

import cats.effect.{Async, Bracket}
import cats.syntax.all._
import ch.datascience.db.SessionResource
import io.chrisdavenport.log4cats.Logger
import io.renku.eventlog.EventLogDB
import skunk._
import skunk.implicits._
import skunk.codec.all._

private trait TimestampZoneAdder[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

private object TimestampZoneAdder {
  def apply[Interpretation[_]: Async: Bracket[*[_], Throwable]](
      transactor: SessionResource[Interpretation, EventLogDB],
      logger:     Logger[Interpretation]
  ): TimestampZoneAdder[Interpretation] =
    TimestampZoneAdderImpl(transactor, logger)
}
private case class TimestampZoneAdderImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]](
    transactor: SessionResource[Interpretation, EventLogDB],
    logger:     Logger[Interpretation]
) extends TimestampZoneAdder[Interpretation]
    with EventTableCheck {
  override def run(): Interpretation[Unit] =
    checkIfAlreadyTimestampz flatMap {
      case true =>
        logger.info("Fields are already in timestampz type")
      case false =>
        transactor.use { implicit session =>
          session.transaction.use { xa =>
            for {
              sp <- xa.savepoint
              _ <- migrateTimestampToCEST recoverWith { e =>
                     xa.rollback(sp).flatMap(_ => e.raiseError[Interpretation, Unit])
                   }
            } yield ()
          }
        }
    }

  private val columnsToMigrate = List("batch_date", "created_date", "execution_date", "event_date")

  private def checkIfAlreadyTimestampz = transactor.use { session =>
    val query: Query[Void, String ~ String] = sql"""SELECT column_name, data_type FROM information_schema.columns WHERE
         table_name = event;""".query(varchar ~ varchar)

    session
      .execute(query)
      .map(_.filter { case (columnName, _) => columnsToMigrate.contains(columnName) }.forall { case (_, columnType) =>
        columnType == "timestampz"
      })
      .recover(_ => false)
  }

  private def migrateTimestampToCEST(implicit session: Session[Interpretation]) = for {
    _ <- execute(sql"ALTER table event ALTER batch_date TYPE timestamptz USING batch_date AT TIME ZONE 'CEST' ".command)
    _ <- execute(
           sql"ALTER table event ALTER created_date TYPE timestamptz USING created_date AT TIME ZONE 'CEST' ".command
         )
    _ <-
      execute(
        sql"ALTER table event ALTER execution_date TYPE timestamptz USING execution_date AT TIME ZONE 'CEST' ".command
      )
    _ <- execute(sql"ALTER table event ALTER event_date TYPE timestamptz USING event_date AT TIME ZONE 'CEST' ".command)
  } yield ()
}
