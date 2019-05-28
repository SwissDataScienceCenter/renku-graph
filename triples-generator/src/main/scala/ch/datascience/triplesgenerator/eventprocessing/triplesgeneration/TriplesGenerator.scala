/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplesgeneration

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.config.ConfigLoader
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog.RenkuLogTriplesGenerator
import ch.datascience.triplesgenerator.eventprocessing.{Commit, RDFTriples}
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait TriplesGenerator[Interpretation[_]] {
  def generateTriples(commit: Commit, maybeAccessToken: Option[AccessToken]): Interpretation[RDFTriples]
}

class TriplesGeneratorProvider(
    configuration: Config = ConfigFactory.load()
)(implicit ME:     MonadError[IO, Throwable])
    extends ConfigLoader[IO] {

  def get(implicit contextShift: ContextShift[IO],
          executionContext:      ExecutionContext,
          timer:                 Timer[IO]): IO[TriplesGenerator[IO]] =
    find[String]("triples-generator", configuration) flatMap {
      case "renku-log"        => RenkuLogTriplesGenerator()
      case "remote-generator" => RemoteTriplesGenerator(configuration)
    }
}
