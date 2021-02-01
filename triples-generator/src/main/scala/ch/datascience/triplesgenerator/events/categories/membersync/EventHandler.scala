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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.MonadError
import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.http.EventRequest.EventRequestContent
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import ch.datascience.triplesgenerator.events
import ch.datascience.triplesgenerator.events.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private[events] class EventHandler[Interpretation[_]](
    membersSynchronizer: MembersSynchronizer[Interpretation],
    logger:              Logger[Interpretation]
)(implicit
    ME:           MonadError[Interpretation, Throwable],
    contextShift: ContextShift[Interpretation],
    concurrent:   Concurrent[Interpretation]
) extends events.EventHandler[Interpretation] {

  import ch.datascience.graph.model.projects
  import io.circe.Decoder
  import membersSynchronizer._

  override val categoryName: CategoryName = EventHandler.categoryName

  override def handle(request: EventRequestContent) = {
    for {
      _           <- EitherT(request.event.as[CategoryName].pure[Interpretation]).leftMap(_ => UnsupportedEventType)
      projectPath <- EitherT(request.event.as[projects.Path].pure[Interpretation]).leftMap(_ => BadRequest)
      result <- (contextShift.shift *> concurrent
                  .start(synchronizeMembers(projectPath))).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger.log(projectPath))
                  .leftSemiflatTap(logger.log(projectPath))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: projects.Path => String = { path =>
    s"projectPath = $path"
  }

  implicit lazy val categoryNameDecoder: Decoder[CategoryName] = { implicit cursor => validateCategoryName }

  implicit lazy val eventDecoder: Decoder[projects.Path] = { implicit cursor =>
    import ch.datascience.tinytypes.json.TinyTypeDecoders._
    cursor.downField("project").downField("path").as[projects.Path](relativePathDecoder(projects.Path))
  }

}

private[events] object EventHandler {
  val categoryName: CategoryName = CategoryName("MEMBER_SYNC")

  def apply(gitLabThrottler: Throttler[IO, GitLab], logger: Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(
      implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[EventHandler[IO]] = for {
    membersSynchronizer <- MembersSynchronizer(gitLabThrottler, logger, timeRecorder)
  } yield new EventHandler[IO](membersSynchronizer, logger)
}
