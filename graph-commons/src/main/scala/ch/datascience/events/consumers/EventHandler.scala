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

package ch.datascience.events.consumers

import cats.MonadError
import cats.data.EitherT
import cats.syntax.all._
import ch.datascience.events.consumers.EventSchedulingResult.{Accepted, BadRequest, SchedulingError, UnsupportedEventType}
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventId}
import ch.datascience.graph.model.projects
import org.typelevel.log4cats.Logger
import io.circe.{Decoder, DecodingFailure, Json}

import scala.util.control.NonFatal

trait EventHandler[Interpretation[_]] {
  val categoryName: CategoryName

  def handle(request: EventRequestContent): Interpretation[EventSchedulingResult]

  implicit class JsonOps(json: Json) {

    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    lazy val validateCategoryName: Either[EventSchedulingResult, Unit] =
      (json.hcursor.downField("categoryName").as[CategoryName] flatMap checkCategoryName)
        .leftMap(_ => UnsupportedEventType)
        .void

    lazy val getProject: Either[EventSchedulingResult, Project] = json.as[Project].leftMap(_ => BadRequest)

    lazy val getEventId: Either[EventSchedulingResult, CompoundEventId] =
      json.as[CompoundEventId].leftMap(_ => BadRequest)

    lazy val getProjectPath: Either[EventSchedulingResult, projects.Path] =
      json.hcursor.downField("project").downField("path").as[projects.Path].leftMap(_ => BadRequest)

    private lazy val checkCategoryName: CategoryName => Decoder.Result[CategoryName] = {
      case name @ `categoryName` => Right(name)
      case other =>
        Left(DecodingFailure(s"$other not supported by $categoryName", Nil))
    }

    private implicit val projectDecoder: Decoder[Project] = { implicit cursor =>
      for {
        projectId   <- cursor.downField("project").downField("id").as[projects.Id]
        projectPath <- cursor.downField("project").downField("path").as[projects.Path]
      } yield Project(projectId, projectPath)
    }

    private implicit val eventIdDecoder: Decoder[CompoundEventId] = { implicit cursor =>
      for {
        id        <- cursor.downField("id").as[EventId]
        projectId <- cursor.downField("project").downField("id").as[projects.Id]
      } yield CompoundEventId(id, projectId)
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

    def logInfo[EventInfo](eventInfo: EventInfo, message: String)(implicit
        toString:                     EventInfo => String
    ): Interpretation[Unit] = logger.info(s"$categoryName: ${toString(eventInfo)} -> $message")

    def logError[EventInfo](eventInfo: EventInfo, exception: Throwable)(implicit
        toString:                      EventInfo => String
    ): Interpretation[Unit] = logger.error(exception)(s"$categoryName: ${toString(eventInfo)} -> Failure")
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
      Left(result)
    }

    private lazy val asSchedulingError: PartialFunction[Throwable, Either[EventSchedulingResult, T]] = {
      case NonFatal(exception) => Left(SchedulingError(exception))
    }
  }
}
