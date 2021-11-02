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

import io.renku.graph.model.RenkuBaseUrl
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.jsonld._
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import io.renku.triplesgenerator.reprovisioning.ReProvisioningInfo.Status.Running

private final case class ReProvisioningInfo(status: ReProvisioningInfo.Status)

private object ReProvisioningInfo {

  sealed trait Status extends StringTinyType with Product with Serializable
  implicit object Status extends TinyTypeFactory[Status](StatusInstantiator) with TinyTypeJsonLDOps[Status] {
    case object Running extends Status {
      override val value: String = "running"
    }
  }

  private object StatusInstantiator extends (String => Status) {
    override def apply(value: String): Status = value match {
      case Running.value => Running
      case other         => throw new IllegalArgumentException(s"'$other' unknown ReProvisioningInfo.Status")
    }
  }

  val reProvisioningInfoEntityType: EntityType = EntityType of renku / "ReProvisioning"

  import io.renku.jsonld.syntax._

  implicit def jsonLDEncoder(implicit
      renkuBaseUrl: RenkuBaseUrl
  ): JsonLDEncoder[ReProvisioningInfo] = JsonLDEncoder.instance { entity =>
    JsonLD.entity(
      EntityId.of((renkuBaseUrl / "re-provisioning").toString),
      EntityTypes of reProvisioningInfoEntityType,
      renku / "status" -> entity.status.asJsonLD
    )
  }
}
