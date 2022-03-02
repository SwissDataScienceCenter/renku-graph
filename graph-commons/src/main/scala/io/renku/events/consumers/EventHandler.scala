/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.events.consumers

import cats.data.EitherT
import cats.data.EitherT._
import cats.syntax.all._
import cats.{Monad, MonadThrow, Show}
import io.circe.{Decoder, DecodingFailure, Json}
import io.renku.events.EventRequestContent
import io.renku.events.consumers.EventSchedulingResult._
import io.renku.graph.model.events.{CategoryName, CompoundEventId, EventId}
import io.renku.graph.model.projects
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait EventHandler[F[_]] {
  def tryHandling(request:                     EventRequestContent): F[EventSchedulingResult]
  protected def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]]
}

abstract class EventHandlerWithProcessLimiter[F[_]: Monad](processesLimiter: ConcurrentProcessesLimiter[F])
    extends EventHandler[F] {

  val categoryName: CategoryName

  final override def tryHandling(request: EventRequestContent): F[EventSchedulingResult] = {
    for {
      _                    <- fromEither[F](request.event.validateCategoryName)
      eventHandlingProcess <- right(createHandlingProcess(request))
      result               <- right[EventSchedulingResult](processesLimiter tryExecuting eventHandlingProcess)
    } yield result
  }.merge

  protected def createHandlingProcess(request: EventRequestContent): F[EventHandlingProcess[F]]

  implicit class JsonOps(json: Json) {

    import io.renku.tinytypes.json.TinyTypeDecoders._

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
      case other                 => Left(DecodingFailure(s"$other not supported by $categoryName", Nil))
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

  protected implicit class LoggerOps(logger: Logger[F])(implicit ME: MonadThrow[F]) {

    def log[EventInfo](
        eventInfo: EventInfo
    )(result:      EventSchedulingResult)(implicit show: Show[EventInfo]): F[Unit] = result match {
      case Accepted                           => logger.info(show"$categoryName: $eventInfo -> $result")
      case error @ SchedulingError(exception) => logger.error(exception)(show"$categoryName: $eventInfo -> $error")
      case _                                  => ME.unit
    }

    def logInfo[EventInfo](eventInfo: EventInfo, message: String)(implicit
        show:                         Show[EventInfo]
    ): F[Unit] = logger.info(show"$categoryName: $eventInfo -> $message")

    def logError[EventInfo](eventInfo: EventInfo, exception: Throwable)(implicit
        show:                          Show[EventInfo]
    ): F[Unit] = logger.error(exception)(show"$categoryName: $eventInfo -> Failure")
  }

  protected implicit class EitherTOps[T](operation: F[T])(implicit ME: MonadThrow[F]) {

    def toRightT(
        recoverTo: EventSchedulingResult
    ): EitherT[F, EventSchedulingResult, T] = EitherT {
      operation.map(_.asRight[EventSchedulingResult]) recover as(recoverTo)
    }

    lazy val toRightT: EitherT[F, EventSchedulingResult, T] = EitherT {
      operation.map(_.asRight[EventSchedulingResult]) recover asSchedulingError
    }

    private def as(result: EventSchedulingResult): PartialFunction[Throwable, Either[EventSchedulingResult, T]] = {
      case NonFatal(_) => Left(result)
    }

    private lazy val asSchedulingError: PartialFunction[Throwable, Either[EventSchedulingResult, T]] = {
      case NonFatal(exception) => Left(SchedulingError(exception))
    }
  }
}
