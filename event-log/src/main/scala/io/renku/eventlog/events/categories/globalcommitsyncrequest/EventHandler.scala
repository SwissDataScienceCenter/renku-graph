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

package io.renku.eventlog.events.categories.globalcommitsyncrequest


import cats.MonadError
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import ch.datascience.events.consumers
import ch.datascience.events.consumers.{EventRequestContent, EventSchedulingResult}
import ch.datascience.graph.model.events.CategoryName
import io.circe.Decoder

import scala.concurrent.ExecutionContext

private class EventHandler[Interpretation[_]: MonadError[*[_], Throwable]](
                                                                            override val categoryName: CategoryName,
//                                                                            logger:                    Logger[Interpretation]
                                                                          )(implicit
                                                                            contextShift: ContextShift[Interpretation],
                                                                            concurrent:   Concurrent[Interpretation]
                                                                          ) extends consumers.EventHandler[Interpretation] {

  import ch.datascience.graph.model.projects
  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  override def handle(request: EventRequestContent): Interpretation[EventSchedulingResult] = ???
//    {
//    for {
//      _ <- fromEither[Interpretation](request.event.validateCategoryName)
//      event <-
//        fromEither[Interpretation](
//          request.event.as[GlobalCommitSyncEvent].leftMap(_ => BadRequest).leftWiden[EventSchedulingResult]
//        )
//      result <- forceCommitSync(event._1, event._2).toRightT
//        .map(_ => Accepted)
//        .semiflatTap(logger.log(event))
//        .leftSemiflatTap(logger.log(event))
//    } yield result
//  }.merge

  private implicit lazy val eventInfoToString: ((projects.Id, projects.Path)) => String = {
    case (projectId, projectPath) => s"projectId = $projectId, projectPath = $projectPath"
  }

  private implicit val eventDecoder: Decoder[(projects.Id, projects.Path)] = { cursor =>
    for {
      projectId   <- cursor.downField("project").downField("id").as[projects.Id]
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
    } yield projectId -> projectPath
  }
}

private object EventHandler {
  def apply(
//             sessionResource:  SessionResource[IO, EventLogDB],
//            queriesExecTimes: LabeledHistogram[IO, SqlStatement.Name],
//            logger:           Logger[IO]
           )(implicit
             executionContext: ExecutionContext,
             contextShift:     ContextShift[IO],
             timer:            Timer[IO]
           ): IO[EventHandler[IO]] = IO {new EventHandler[IO](categoryName
//    , logger
  )}
}
