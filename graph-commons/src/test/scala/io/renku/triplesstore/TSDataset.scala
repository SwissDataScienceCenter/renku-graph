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

import cats.effect.IO
import cats.effect.std.Random
import io.renku.http.client.BasicAuthCredentials
import io.renku.triplesstore.client.util.JenaServerSupport

trait TSDataset {
  self: JenaServerSupport =>

  protected[triplesstore] def loadTtl[CF <: DatasetConfigFile](configFactory: DatasetConfigFileFactory[CF]): IO[CF] =
    IO.fromEither(configFactory.fromTtlFile())

  protected[triplesstore] def generateName =
    Random
      .scalaUtilRandom[IO]
      .flatMap(_.nextIntBounded(1000))
      .map(v => s"${getClass.getSimpleName.toLowerCase}_$v")

  protected[triplesstore] def updateDSName[C <: DatasetConfigFile](
      config:  C,
      newName: String,
      factory: (DatasetName, String) => C
  ): C =
    factory(
      DatasetName(newName),
      config.value.replace(s""""${config.name}"""", s""""$newName"""")
    )

  protected[triplesstore] def dsConnectionConfig[DCC <: DatasetConnectionConfig](config: DatasetConfigFile)(
      factory: (FusekiUrl, BasicAuthCredentials, DatasetName) => DCC
  ): DCC =
    factory(
      FusekiUrl(server.ccConfig.baseUrl.renderString),
      BasicAuthCredentials.from(
        server.ccConfig.basicAuth
          .getOrElse(throw new Exception(s"No AuthCredentials for '${config.datasetName}' dataset"))
      ),
      config.datasetName
    )
}
