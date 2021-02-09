/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.statuschange

import cats.MonadError
import cats.data.EitherT
import cats.effect.Sync
import cats.syntax.all._
import ch.datascience.graph.model.events.EventStatus
import io.circe.Json
import io.renku.eventlog.statuschange.commands.CommandFindingResult.{NotSupported, PayloadMalformed}
import io.renku.eventlog.{EventMessage, EventProcessingTime}
import org.http4s.circe.jsonOf
import org.http4s.{MediaType, Request}

package object commands {
  private[statuschange] sealed trait CommandFindingResult extends Product with Serializable

  private[statuschange] object CommandFindingResult {
    case class CommandFound[Interpretation[_]](command: ChangeStatusCommand[Interpretation])
        extends CommandFindingResult
    case object NotSupported extends CommandFindingResult
    case class PayloadMalformed(message: String) extends CommandFindingResult
  }

  def when[Interpretation[_]](request: Request[Interpretation], has: MediaType)(
      f:                               => Interpretation[CommandFindingResult]
  )(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Interpretation[CommandFindingResult] =
    if (
      request.contentType
        .exists(p => p.mediaType.mainType == has.mainType && p.mediaType.subType == has.subType)
    ) f
    else (NotSupported: CommandFindingResult).pure[Interpretation]

  implicit class RequestOps[Interpretation[_]: Sync](request: Request[Interpretation]) {
    private implicit val jsonEntityDecoder = jsonOf[Interpretation, Json]

    def validate(status: EventStatus): EitherT[Interpretation, CommandFindingResult, Unit] = EitherT(
      request.as[Json].map(_.validate(status))
    )

    lazy val getProcessingTime: EitherT[Interpretation, CommandFindingResult, Option[EventProcessingTime]] = EitherT(
      request.as[Json].map(_.getProcessingTime)
    )

    lazy val getMessage: EitherT[Interpretation, CommandFindingResult, Option[EventMessage]] = EitherT(
      request.as[Json].map(_.getMessage)
    )
  }

  implicit class JsonOps(json: Json) {
    def validate(status: EventStatus): Either[CommandFindingResult, Unit] = json.hcursor
      .downField("status")
      .as[EventStatus]
      .leftMap(_ => PayloadMalformed("No status property in status change payload"))
      .flatMap {
        case `status` => Right(())
        case _        => Left(NotSupported)
      }

    lazy val getProcessingTime: Either[CommandFindingResult, Option[EventProcessingTime]] = json.hcursor
      .downField("processingTime")
      .as[Option[EventProcessingTime]]
      .leftMap(error => PayloadMalformed(error.getMessage()): CommandFindingResult)

    lazy val getMessage: Either[CommandFindingResult, Option[EventMessage]] = json.hcursor
      .downField("message")
      .as[Option[EventMessage]]
      .leftMap(error => PayloadMalformed(error.getMessage()): CommandFindingResult)
  }

}
