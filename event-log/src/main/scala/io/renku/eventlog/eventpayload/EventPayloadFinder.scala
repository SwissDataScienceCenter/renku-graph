/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.eventlog.eventpayload

import cats.effect.MonadCancelThrow
import eu.timepit.refined.auto._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.eventpayload.EventPayloadFinder.PayloadData
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.graph.model.events.EventId
import io.renku.graph.model.projects.{Path => ProjectPath}
import scodec.bits.ByteVector
import skunk._
import skunk.implicits._

trait EventPayloadFinder[F[_]] {

  /** Finds the payload for the given event and project and returns it as a byte-array. */
  def findEventPayload(eventId: EventId, projectPath: ProjectPath): F[Option[PayloadData]]
}

object EventPayloadFinder {

  final case class PayloadData(data: ByteVector) {
    def length: Long = data.length
  }

  def apply[F[_]: MonadCancelThrow: SessionResource: QueriesExecutionTimes]: EventPayloadFinder[F] =
    new DbClient[F](Some(QueriesExecutionTimes[F])) with EventPayloadFinder[F] with TypeSerializers {

      override def findEventPayload(eventId: EventId, projectPath: ProjectPath): F[Option[PayloadData]] =
        SessionResource[F].useK(measureExecutionTime(findStatement(eventId, projectPath)))

      def findStatement(eventId: EventId, projectPath: ProjectPath): SqlStatement[F, Option[PayloadData]] =
        SqlStatement("find event payload")
          .select(selectPayload)
          .arguments(eventId ~ projectPath)
          .build(_.option)

      def selectPayload: Query[EventId ~ ProjectPath, PayloadData] =
        sql"""
              SELECT ep.payload
              FROM event_payload ep
              INNER JOIN project p USING (project_id)
              WHERE ep.event_id = $eventIdEncoder AND p.project_path = $projectPathEncoder
             """
          .query(byteVectorDecoder)
          .map(PayloadData)
    }
}
