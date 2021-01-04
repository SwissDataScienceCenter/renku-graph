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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import ch.datascience.graph.model.CliVersion
import ch.datascience.triplesgenerator.config.TriplesGeneration
import ch.datascience.triplesgenerator.config.TriplesGeneration._
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader

private object CliVersionFinder {

  import ch.datascience.config.ConfigLoader._

  private implicit val cliVersionLoader: ConfigReader[CliVersion] = stringTinyTypeReader(CliVersion)

  def apply[Interpretation[_]](
      triplesGeneration: TriplesGeneration
  )(implicit ME:         MonadError[Interpretation, Throwable]): Interpretation[CliVersion] =
    apply(triplesGeneration, findRenkuVersion, ConfigFactory.load())

  private[reprovisioning] def apply[Interpretation[_]](
      triplesGeneration:  TriplesGeneration,
      renkuVersionFinder: Interpretation[CliVersion],
      config:             Config
  )(implicit ME:          MonadError[Interpretation, Throwable]): Interpretation[CliVersion] = triplesGeneration match {
    case RenkuLog => renkuVersionFinder
    case RemoteTriplesGeneration =>
      find[Interpretation, CliVersion]("services.triples-generator.cli-version", config)
  }

  private def findRenkuVersion[Interpretation[_]](implicit
      ME: MonadError[Interpretation, Throwable]
  ): Interpretation[CliVersion] = {
    import ammonite.ops._
    import cats.syntax.all._

    for {
      versionAsString <- ME.catchNonFatal(%%("renku", "--version")(pwd).out.string.trim)
      version         <- ME.fromEither(CliVersion.from(versionAsString))
    } yield version
  }
}
