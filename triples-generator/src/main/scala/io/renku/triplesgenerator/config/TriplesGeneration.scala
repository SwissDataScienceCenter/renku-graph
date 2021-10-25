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

package io.renku.triplesgenerator.config

import cats.effect.Async
import com.typesafe.config.{Config, ConfigFactory}
import org.typelevel.log4cats.Logger

trait TriplesGeneration extends Product with Serializable

object TriplesGeneration {
  final case object RenkuLog                extends TriplesGeneration
  final case object RemoteTriplesGeneration extends TriplesGeneration

  import cats.syntax.all._
  import io.renku.config.ConfigLoader._

  def apply[F[_]: Async: Logger](config: Config = ConfigFactory.load): F[TriplesGeneration] =
    find[F, String]("triples-generation", config) map {
      case "renku-log"        => RenkuLog
      case "remote-generator" => RemoteTriplesGeneration
    }
}
