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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.tooling

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.graph.model.projects
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private trait ProjectsFinder[F[_]] {
  def findProjects(): F[List[projects.Path]]
}

private object ProjectsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](query: SparqlQuery): F[ProjectsFinder[F]] =
    RenkuConnectionConfig[F]().map(new ProjectsFinderImpl(query, _))
}

private class ProjectsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    query:                 SparqlQuery,
    renkuConnectionConfig: RenkuConnectionConfig
) extends RdfStoreClientImpl[F](renkuConnectionConfig,
                                idleTimeoutOverride = (16 minutes).some,
                                requestTimeoutOverride = (15 minutes).some
    )
    with ProjectsFinder[F] {

  override def findProjects(): F[List[projects.Path]] = queryExpecting[List[projects.Path]](query)

  private implicit lazy val pathsDecoder: Decoder[List[projects.Path]] = ResultsDecoder[List, projects.Path] {
    implicit cursor =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[projects.Path]("path")
  }
}
