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

package ch.datascience.triplesgenerator

import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.projects.SchemaVersion
import io.circe.Decoder
import io.circe.Decoder.decodeList

package object models {
  final case class RenkuVersionPair(cliVersion: CliVersion, schemaVersion: SchemaVersion)

  object RenkuVersionPair {
    implicit lazy val versionPairDecoder: Decoder[List[RenkuVersionPair]] = { topCursor =>
      val renkuVersionPairs: Decoder[RenkuVersionPair] = { cursor =>
        for {
          cliVersion    <- cursor.downField("cliVersion").downField("value").as[CliVersion]
          schemaVersion <- cursor.downField("schemaVersion").downField("value").as[SchemaVersion]
        } yield RenkuVersionPair(cliVersion, schemaVersion)
      }
      topCursor.downField("results").downField("bindings").as(decodeList(renkuVersionPairs))
    }
  }

}
