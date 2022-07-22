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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations
package tooling

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

private[migrations] trait RecordsFinder[F[_]] {
  def findRecords[OUT](query: SparqlQuery)(implicit decoder: Decoder[List[OUT]]): F[List[OUT]]
}

private[migrations] object RecordsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[RecordsFinder[F]] =
    RenkuConnectionConfig[F]().map(new RecordsFinderImpl(_))
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      renkuConnectionConfig: RenkuConnectionConfig
  ): RecordsFinder[F] = new RecordsFinderImpl(renkuConnectionConfig)
}

private class RecordsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    renkuConnectionConfig: RenkuConnectionConfig
) extends TSClientImpl[F](renkuConnectionConfig,
                          idleTimeoutOverride = (16 minutes).some,
                          requestTimeoutOverride = (15 minutes).some
    )
    with RecordsFinder[F] {

  override def findRecords[OUT](query: SparqlQuery)(implicit decoder: Decoder[List[OUT]]): F[List[OUT]] =
    queryExpecting[List[OUT]](query)
}
