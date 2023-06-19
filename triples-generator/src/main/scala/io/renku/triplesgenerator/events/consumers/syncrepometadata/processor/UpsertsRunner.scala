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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.ApplicativeThrow
import cats.effect.Async
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQuery, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait UpsertsRunner[F[_]] {
  def run(queries: List[SparqlQuery]): F[Unit]
}

private object UpsertsRunner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](config: Config): F[UpsertsRunner[F]] =
    ProjectsConnectionConfig[F](config).map(TSClient[F](_)).map(new UpsertsRunnerImpl(_))
}

private class UpsertsRunnerImpl[F[_]: ApplicativeThrow: Logger](tsClient: TSClient[F]) extends UpsertsRunner[F] {

  override def run(queries: List[SparqlQuery]): F[Unit] =
    queries.map(runAndLogError).sequence.void

  private def runAndLogError(query: SparqlQuery) =
    tsClient
      .updateWithNoResult(query)
      .handleErrorWith(Logger[F].error(_)(show"$categoryName: running '${query.name.value}'"))
}
