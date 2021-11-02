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

package io.renku.triplesgenerator.reprovisioning

import cats.data.NonEmptyList
import io.renku.graph.model.{CliVersion, RenkuVersionPair, SchemaVersion}

trait ReProvisionJudge {

  def reProvisioningNeeded(maybeCurrentVersionPair:   Option[RenkuVersionPair],
                           versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
  ): Boolean
}

private class ReProvisionJudgeImpl extends ReProvisionJudge {

  override def reProvisioningNeeded(maybeCurrentVersionPair:   Option[RenkuVersionPair],
                                    versionCompatibilityPairs: NonEmptyList[RenkuVersionPair]
  ): Boolean =
    `is current schema version different from latest`(maybeCurrentVersionPair.map(_.schemaVersion),
                                                      versionCompatibilityPairs.head.schemaVersion
    ) || `are latest schema versions same but cli versions different`(versionCompatibilityPairs.toList,
                                                                      maybeCurrentVersionPair.map(_.cliVersion)
    )

  private def `is current schema version different from latest`(maybeCurrent: Option[SchemaVersion],
                                                                latest:       SchemaVersion
  ) = !(maybeCurrent contains latest)

  private def `are latest schema versions same but cli versions different`(
      versionCompatibilityPairs: List[RenkuVersionPair],
      maybeCurrentCliVersion:    Option[CliVersion]
  ) = versionCompatibilityPairs match {
    case RenkuVersionPair(latestCliVersion, latestSchemaVersion) :: RenkuVersionPair(_, oldSchemaVersion) :: _
        if latestSchemaVersion == oldSchemaVersion =>
      !(maybeCurrentCliVersion contains latestCliVersion)
    case _ => false
  }
}
