/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package tooling

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private[migrations] trait RecordsFinder[F[_]] {
  def findRecords[OUT](query: SparqlQuery)(implicit decoder: Decoder[List[OUT]]): F[List[OUT]]
}

private[migrations] object RecordsFinder {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[RecordsFinder[F]] =
    ProjectsConnectionConfig.fromConfig[F]().map(new RecordsFinderImpl(_))

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](connectionConfig: DatasetConnectionConfig): RecordsFinder[F] =
    new RecordsFinderImpl(connectionConfig)
}

private class RecordsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    storeConfig: DatasetConnectionConfig
) extends TSClientImpl[F](storeConfig)
    with RecordsFinder[F] {

  override def findRecords[OUT](query: SparqlQuery)(implicit decoder: Decoder[List[OUT]]): F[List[OUT]] =
    queryExpecting[List[OUT]](query)
}
