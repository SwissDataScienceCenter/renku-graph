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

package io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration

import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.http.client.AccessToken
import io.renku.jsonld.JsonLD
import io.renku.triplesgenerator.config.TriplesGeneration
import io.renku.triplesgenerator.config.TriplesGeneration.{RemoteTriplesGeneration, RenkuLog}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.awaitinggeneration.CommitEvent
import io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog.RenkuLogTriplesGenerator

import scala.concurrent.ExecutionContext

private[awaitinggeneration] trait TriplesGenerator[Interpretation[_]] {
  def generateTriples(
      commit: CommitEvent
  )(implicit
      maybeAccessToken: Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, JsonLD]
}

private[awaitinggeneration] object TriplesGenerator {

  final case class GenerationRecoverableError(message: String)
      extends Exception(message)
      with ProcessingRecoverableError

  def apply(config:     Config = ConfigFactory.load)(implicit
      contextShift:     ContextShift[IO],
      executionContext: ExecutionContext,
      timer:            Timer[IO]
  ): IO[TriplesGenerator[IO]] =
    TriplesGeneration[IO](config) flatMap {
      case RenkuLog                => RenkuLogTriplesGenerator()
      case RemoteTriplesGeneration => RemoteTriplesGenerator(config)
    }
}
