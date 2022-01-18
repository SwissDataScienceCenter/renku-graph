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

package io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration

import cats.data.EitherT
import cats.effect.Async
import cats.implicits.toFlatMapOps
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.http.client.AccessToken
import io.renku.jsonld.JsonLD
import io.renku.triplesgenerator.config.TriplesGeneration
import io.renku.triplesgenerator.config.TriplesGeneration.{RemoteTriplesGeneration, RenkuLog}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.awaitinggeneration.CommitEvent
import io.renku.triplesgenerator.events.categories.awaitinggeneration.triplesgeneration.renkulog.RenkuLogTriplesGenerator
import org.typelevel.log4cats.Logger

private[awaitinggeneration] trait TriplesGenerator[F[_]] {
  def generateTriples(commit: CommitEvent)(implicit
      maybeAccessToken:       Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, JsonLD]
}

private[awaitinggeneration] object TriplesGenerator {

  def apply[F[_]: Async: Logger](config: Config = ConfigFactory.load): F[TriplesGenerator[F]] =
    TriplesGeneration[F](config) flatMap {
      case RenkuLog                => RenkuLogTriplesGenerator()
      case RemoteTriplesGeneration => RemoteTriplesGenerator(config)
    }
}
