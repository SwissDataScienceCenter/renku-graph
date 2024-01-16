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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import ReProvisioningInfo.Status.Running
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.util.jsonld.TinyTypeJsonLDCodec._
import io.renku.jsonld._
import io.renku.microservices.MicroserviceBaseUrl
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

private final case class ReProvisioningInfo(status:        ReProvisioningInfo.Status.Running,
                                            controllerUrl: MicroserviceBaseUrl
)

private object ReProvisioningInfo {

  sealed trait Status extends StringTinyType with Product with Serializable
  implicit object Status extends TinyTypeFactory[Status](StatusInstantiator) with TinyTypeJsonLDOps[Status] {
    type Running = Running.type
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

  import io.renku.jsonld.syntax._

  implicit def jsonLDEncoder(implicit renkuUrl: RenkuUrl): JsonLDEncoder[ReProvisioningInfo] = JsonLDEncoder.instance {
    entity =>
      JsonLD.entity(
        EntityId.of((renkuUrl / "re-provisioning").toString),
        EntityTypes of renku / "ReProvisioning",
        renku / "status"        -> entity.status.asInstanceOf[Status].asJsonLD,
        renku / "controllerUrl" -> entity.controllerUrl.asJsonLD
      )
  }
}
