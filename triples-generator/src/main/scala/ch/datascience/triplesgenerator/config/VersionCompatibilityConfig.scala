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

package ch.datascience.triplesgenerator.config

import cats.MonadError
import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.projects.SchemaVersion
import com.typesafe.config.{Config, ConfigException, ConfigFactory, ConfigList}
import pureconfig.ConfigReader
import cats.syntax.all._

final case class VersionCompatibilityConfig(matrix: List[(CliVersion, SchemaVersion)]) extends Product with Serializable
final case class VersionSchemaPair(cliVersion: CliVersion, schemaVersion: SchemaVersion)
    extends Product
    with Serializable

object VersionCompatibilityConfig {

  import cats.syntax.all._
  import ch.datascience.config.ConfigLoader._
  import pureconfig.generic.auto._

  private val separator = "->"

  implicit val reader = ConfigReader[List[String]].map(_.map { pair =>
    val (cliVersion, schemaVersion): (String, String) = pair.split(separator).toList match {
      case List(cliVersion, schemaVersion) => (cliVersion, schemaVersion)
      case _                               => throw new Exception(s"Did not find exactly two elements: ${pair}")
    }
    VersionSchemaPair(CliVersion(cliVersion.trim), SchemaVersion(schemaVersion.trim))
  })

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[List[VersionSchemaPair]] =
    find[Interpretation, List[VersionSchemaPair]]("compatibility-matrix", config)(reader, ME).flatMap {
      case Nil =>
        ME.raiseError[List[VersionSchemaPair]](new Exception("No compatibility matrix provided for schema version"))
      case list => list.pure[Interpretation]
    }
}
