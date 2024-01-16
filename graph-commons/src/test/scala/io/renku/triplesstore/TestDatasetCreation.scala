/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore

import cats.effect.{Clock, IO}
import cats.effect.std.Random
import cats.syntax.all._
import io.renku.http.client.BasicAuthCredentials
import io.renku.triplesstore.client.http.ConnectionConfig

import scala.util.matching.Regex

private object TestDatasetCreation {

  def loadTtl[CF <: DatasetConfigFile](configFactory: DatasetConfigFileFactory[CF]): IO[CF] =
    IO.fromEither(configFactory.fromTtlFile())

  def generateName(ttl: DatasetConfigFile, testClass: Class[_]) =
    (Random.scalaUtilRandom[IO].flatMap(_.nextIntBounded(1000)) -> Clock[IO].realTime.map(_.toMillis))
      .mapN((l, t) => s"${ttl.datasetName}_${testClass.getSimpleName.toLowerCase}_${l}_$t")

  def updateDSConfig[C <: DatasetConfigFile](
      config:  C,
      newName: String,
      factory: (DatasetName, String) => C
  ): C = {
    val newBody =
      (updateResourceIds(config.name, newName) >>>
        updateDSLocation(config.name, newName) >>>
        updateDSName(config.name, newName))(config.value)
    factory(DatasetName(newName), newBody)
  }

  private def updateDSName(oldName: String, newName: String): String => String =
    _.replace(s""""$oldName"""", s""""$newName"""")

  private def updateDSLocation(oldName: String, newName: String): String => String =
    updateLocation("""(?s).*tdb2:location\W+"([\w-/:]+)".*""".r, oldName, newName)

  private def updateLocation(propertyExtractor: Regex, oldName: String, newName: String): String => String = {
    case configBody @ propertyExtractor(oldLocation) =>
      val newLocation = oldLocation.replace(oldName, newName)
      configBody.replace(oldLocation, newLocation)
    case configBody => configBody
  }

  private def updateResourceIds(oldName: String, newName: String): String => String =
    _.replaceAll(s"_$oldName", s"_$newName")

  def dsConnectionConfig[DCC <: DatasetConnectionConfig](config:        DatasetConfigFile,
                                                         serverCConfig: ConnectionConfig,
                                                         factory: (FusekiUrl, BasicAuthCredentials, DatasetName) => DCC
  ): DCC =
    factory(
      FusekiUrl(serverCConfig.baseUrl.renderString),
      BasicAuthCredentials.from(
        serverCConfig.basicAuth
          .getOrElse(throw new Exception(s"No AuthCredentials for '${config.datasetName}' dataset"))
      ),
      config.datasetName
    )
}
