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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.triplesgenerator.config.TriplesGeneration
import ch.datascience.triplesgenerator.config.TriplesGeneration._
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader

import scala.language.higherKinds
import scala.util.Try

private object SchemaVersionFinder {

  import ch.datascience.config.ConfigLoader._

  private implicit val schemaVersionLoader: ConfigReader[SchemaVersion] = stringTinyTypeReader(SchemaVersion)

  def apply[Interpretation[_]](
      triplesGeneration: TriplesGeneration
  )(implicit ME:         MonadError[Interpretation, Throwable]): Interpretation[SchemaVersion] =
    apply(triplesGeneration, findRenkuVersion, ConfigFactory.load())

  private[reprovisioning] def apply[Interpretation[_]](
      triplesGeneration:  TriplesGeneration,
      renkuVersionFinder: Interpretation[SchemaVersion],
      config:             Config
  )(implicit ME:          MonadError[Interpretation, Throwable]): Interpretation[SchemaVersion] = triplesGeneration match {
    case RenkuLog => renkuVersionFinder
    case RemoteTriplesGeneration =>
      find[Interpretation, SchemaVersion]("services.triples-generator.schema-version", config)
  }

  private def findRenkuVersion[Interpretation[_]](
      implicit ME: MonadError[Interpretation, Throwable]
  ): Interpretation[SchemaVersion] = {
    import ammonite.ops._
    import cats.implicits._

    for {
      versionAsString <- ME.fromTry { Try(%%('renku, "--version")(pwd).out.string.trim) }
      version         <- ME.fromEither(SchemaVersion.from(versionAsString))
    } yield version
  }
}
