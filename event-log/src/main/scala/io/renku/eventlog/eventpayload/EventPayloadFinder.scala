package io.renku.eventlog.eventpayload

import cats.Monad
import fs2.Stream
import io.renku.db.DbClient
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.eventpayload.EventPayloadFinder.PayloadData
import io.renku.graph.model.events.EventId
import io.renku.graph.model.projects.{Path => ProjectPath}
import io.renku.metrics.LabeledHistogram

trait EventPayloadFinder[F[_]] {

  /** Finds the payload for the given event and project and returns it as a byte-array. */
  def findEventPayload(eventId: EventId, projectPath: ProjectPath): Option[PayloadData[F]]
}

object EventPayloadFinder {

  final case class PayloadData[F[_]](data: Stream[F, Byte], length: Long)

  def apply[F[_]: Monad: SessionResource](queriesExecTimes: LabeledHistogram[F]): EventPayloadFinder[F] =
    new DbClient[F](Some(queriesExecTimes)) with EventPayloadFinder[F] with TypeSerializers {

      override def findEventPayload(eventId: EventId, projectPath: ProjectPath): Option[PayloadData[F]] = ???
    }
}
