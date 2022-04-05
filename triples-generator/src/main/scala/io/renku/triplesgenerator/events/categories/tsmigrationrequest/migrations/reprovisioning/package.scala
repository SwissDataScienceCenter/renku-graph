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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations

import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.{RenkuBaseUrl, RenkuVersionPair}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityTypes}

package object reprovisioning {

  import io.renku.jsonld.{JsonLD, JsonLDEncoder}

  private[reprovisioning] implicit def jsonLDEncoder(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): JsonLDEncoder[RenkuVersionPair] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      EntityId.of((renkuBaseUrl / "version-pair").toString),
      EntityTypes of renku / "VersionPair",
      renku / "schemaVersion" -> entity.schemaVersion.asJsonLD,
      renku / "cliVersion"    -> entity.cliVersion.asJsonLD
    )
  }
}
