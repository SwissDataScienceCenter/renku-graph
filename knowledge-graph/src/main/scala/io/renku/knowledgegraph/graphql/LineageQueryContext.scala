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

package io.renku.knowledgegraph.graphql

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.lineage.LineageFinder
import io.renku.rdfstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger

trait QueryContext[QueryContextT] {
  self: QueryContextT =>
  type Params

  def updateContext(p: Params): QueryContextT
}

class LineageQueryContext[F[_]](
    val lineageFinder: LineageFinder[F],
    val maybeUser:     Option[AuthUser] = None
) extends QueryContext[LineageQueryContext[F]] {

  override type Params = Option[AuthUser]

  override def updateContext(p: Option[AuthUser]): LineageQueryContext[F] =
    new LineageQueryContext[F](lineageFinder, p)
}

object LineageQueryContext {
  def apply[F[_]: Async: Parallel: Logger](
      timeRecorder: SparqlQueryTimeRecorder[F]
  ): F[LineageQueryContext[F]] = LineageFinder[F](timeRecorder) map (new LineageQueryContext[F](_))
}
