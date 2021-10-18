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

package io.renku.graph.model.testentities

import cats.syntax.all._
import io.renku.graph.model.{RenkuBaseUrl, associations, entities}
import io.renku.jsonld._

final case class Association(activity: Activity, agent: Agent, plan: Plan)

object Association {

  def factory(agent: Agent, plan: Plan): Activity => Association = Association(_, agent, plan)

  import io.renku.jsonld.syntax._

  implicit def toEntitiesAssociation(implicit renkuBaseUrl: RenkuBaseUrl): Association => entities.Association =
    association =>
      entities.Association(associations.ResourceId(association.asEntityId.show),
                           association.agent.to[entities.Agent],
                           association.plan.to[entities.Plan]
      )

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl): JsonLDEncoder[Association] =
    JsonLDEncoder.instance(association => association.to[entities.Association].asJsonLD)

  implicit def entityIdEncoder(implicit renkuBaseUrl: RenkuBaseUrl): EntityIdEncoder[Association] =
    EntityIdEncoder.instance(entity => entity.activity.asEntityId.asUrlEntityId / "association")
}
