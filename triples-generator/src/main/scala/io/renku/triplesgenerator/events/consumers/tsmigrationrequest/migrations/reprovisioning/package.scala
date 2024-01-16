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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest
package migrations

import cats.syntax.all._
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.versions.RenkuVersionPair
import io.renku.jsonld.{EntityId, EntityTypes}
import io.renku.jsonld.syntax._

package object reprovisioning {

  private[reprovisioning] val migrationName: Migration.Name = Migration.Name("re-provisioning")

  private[reprovisioning] def formMessage(message: String): String = show"$categoryName: $migrationName $message"

  import io.renku.jsonld.{JsonLD, JsonLDEncoder}

  private[reprovisioning] implicit def jsonLDEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[RenkuVersionPair] =
    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        EntityId.of((renkuUrl / "version-pair").toString),
        EntityTypes of renku / "VersionPair",
        renku / "schemaVersion" -> entity.schemaVersion.asJsonLD,
        renku / "cliVersion"    -> entity.cliVersion.asJsonLD
      )
    }
}
