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

package ch.datascience.triplesgenerator.events

import cats.MonadError
import cats.data.EitherT
import cats.syntax.all._
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.triplesgenerator.events.EventSchedulingResult.{Accepted, SchedulingError}
import io.chrisdavenport.log4cats.Logger
import io.circe.{Decoder, DecodingFailure, HCursor}
import org.http4s.Request

import scala.util.control.NonFatal

private trait EventHandler[Interpretation[_]] {
  val categoryName: CategoryName
  def handle(request: Request[Interpretation]): Interpretation[EventSchedulingResult]

  protected def validateCategoryName(implicit cursor: HCursor): Decoder.Result[CategoryName] =
    cursor.downField("categoryName").as[CategoryName] flatMap checkCategoryName

  private lazy val checkCategoryName: CategoryName => Decoder.Result[CategoryName] = {
    case name @ `categoryName` => Right(name)
    case other                 => Left(DecodingFailure(s"$other not supported by $categoryName", Nil))
  }

  protected implicit class EitherTOps[T](
      operation: Interpretation[T]
  )(implicit ME: MonadError[Interpretation, Throwable]) {

    def toRightT(
        recoverTo: EventSchedulingResult
    ): EitherT[Interpretation, EventSchedulingResult, T] = EitherT {
      operation map (_.asRight[EventSchedulingResult]) recover as(recoverTo)
    }

    lazy val toRightT: EitherT[Interpretation, EventSchedulingResult, T] = EitherT {
      operation map (_.asRight[EventSchedulingResult]) recover asSchedulingError
    }

    private def as(
        result: EventSchedulingResult
    ): PartialFunction[Throwable, Either[EventSchedulingResult, T]] = { case NonFatal(e) =>
      e.printStackTrace()
      Left(result)
    }

    private lazy val asSchedulingError: PartialFunction[Throwable, Either[EventSchedulingResult, T]] = {
      case NonFatal(exception) => Left(SchedulingError(exception))
    }
  }

  protected implicit class LoggerOps(
      logger:    Logger[Interpretation]
  )(implicit ME: MonadError[Interpretation, Throwable]) {

    def log[EventInfo](
        eventInfo: EventInfo
    )(result:      EventSchedulingResult)(implicit toString: EventInfo => String): Interpretation[Unit] =
      result match {
        case Accepted => logger.info(s"$categoryName: ${toString(eventInfo)} -> $result")
        case SchedulingError(exception) =>
          logger.error(exception)(s"$categoryName: ${toString(eventInfo)} -> $SchedulingError")
        case _ => ME.unit
      }
  }
}
