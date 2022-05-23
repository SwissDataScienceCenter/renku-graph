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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling

import cats.effect.Async
import cats.syntax.all._
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private[migrations] trait UpdateQueryRunner[F[_]] {
  def run(query: SparqlQuery): F[Unit]
}

private class UpdateQueryRunnerImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl[F](rdfStoreConfig)
    with UpdateQueryRunner[F] {

  def run(query: SparqlQuery): F[Unit] = updateWithNoResult(query)
}

private[migrations] object UpdateQueryRunner {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[UpdateQueryRunner[F]] =
    RdfStoreConfig[F]().map(new UpdateQueryRunnerImpl(_))
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](rdfStoreConfig: RdfStoreConfig): UpdateQueryRunner[F] =
    new UpdateQueryRunnerImpl(rdfStoreConfig)
}
