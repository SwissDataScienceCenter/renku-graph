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

package ch.datascience.knowledgegraph.graphql

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.knowledgegraph.lineage.LineageFinder
import ch.datascience.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

trait QueryContext[QueryContextT] {
  self: QueryContextT =>
  type Params
  def updateContext(p: Params): QueryContextT
}

class LineageQueryContext[Interpretation[_]](
    val lineageFinder: LineageFinder[Interpretation],
    val maybeUser:     Option[AuthUser] = None
) extends QueryContext[LineageQueryContext[Interpretation]] {

  override type Params = Option[AuthUser]
  override def updateContext(p: Option[AuthUser]): LineageQueryContext[Interpretation] =
    new LineageQueryContext[Interpretation](lineageFinder, p)
}

object LineageQueryContext {
  def apply(
      timeRecorder: SparqlQueryTimeRecorder[IO],
      logger:       Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[LineageQueryContext[IO]] = LineageFinder(timeRecorder, logger) map (new LineageQueryContext[IO](_))
}
