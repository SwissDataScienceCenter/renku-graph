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

package io.renku.rdfstore

import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive

object RdfStoreServer extends IOApp {

  val renkuDatasetName:      DatasetName          = DatasetName("renku")
  val migrationsDatasetName: DatasetName          = DatasetName("migrations")
  val fusekiPort:            Int Refined Positive = 3030

  override def run(args: List[String]): IO[ExitCode] = for {
    _ <- new RdfStoreServer(fusekiPort, renkuDatasetName, migrationsDatasetName, muteLogging = false).start
  } yield ExitCode.Success
}

class RdfStoreServer(
    port:                  Int Refined Positive,
    datasetName:           DatasetName,
    migrationsDatasetName: DatasetName,
    muteLogging:           Boolean = true
)(implicit temporal:       Temporal[IO]) {

  if (!muteLogging) {
    println(s"$port $datasetName $migrationsDatasetName")
  }

  def start: IO[Unit] = ().pure[IO]

  def stop: IO[Unit] = ().pure[IO]

  def clearDatasets(): IO[Unit] = ().pure[IO]
}
