/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.config.TriplesGeneration
import ch.datascience.triplesgenerator.config.TriplesGeneration.{RemoteTriplesGeneration, RenkuLog}
import ch.datascience.triplesgenerator.eventprocessing.Commit
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.TriplesGenerator.GenerationRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplesgeneration.renkulog.RenkuLogTriplesGenerator
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

trait TriplesGenerator[Interpretation[_]] {
  def generateTriples(
      commit:                  Commit
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[Interpretation, GenerationRecoverableError, JsonLDTriples]
}

object TriplesGenerator {

  final case class GenerationRecoverableError(message: String) extends Exception(message)

  def apply(
      triplesGeneration:   TriplesGeneration,
      config:              Config = ConfigFactory.load()
  )(implicit contextShift: ContextShift[IO],
    executionContext:      ExecutionContext,
    timer:                 Timer[IO]): IO[TriplesGenerator[IO]] = triplesGeneration match {
    case RenkuLog                => RenkuLogTriplesGenerator()
    case RemoteTriplesGeneration => RemoteTriplesGenerator(config)
  }
}
