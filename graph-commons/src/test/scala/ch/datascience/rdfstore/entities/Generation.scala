/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.entities

import ch.datascience.rdfstore.entities.ProcessRun.ChildProcessRun

final case class Generation(location:           Location,
                            activity:           Activity,
                            maybeReverseEntity: Option[Entity with Artifact] = None)

object Generation {

  import ch.datascience.graph.config.RenkuBaseUrl
  import ch.datascience.rdfstore.FusekiBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def factory(entityFactory: Activity => Entity with Artifact)(activity: Activity): Generation = {
    val entity = entityFactory(activity)
    Generation(entity.location, activity, Some(entity))
  }

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[Generation] =
    JsonLDEncoder.instance { entity =>
      val maybeStep = entity.activity match {
        case a: ChildProcessRun => Some(a.processRunStep)
        case _ => None
      }
      val reverseEntity = entity.maybeReverseEntity
        .flatMap { entity =>
          Reverse.of(prov / "qualifiedGeneration" -> entity.asJsonLD).toOption
        }
        .getOrElse(Reverse.empty)

      JsonLD.entity(
        EntityId of fusekiBaseUrl / "activities" / "commit" / entity.activity.commitId / maybeStep / "tree" / entity.location,
        EntityTypes of prov / "Generation",
        reverseEntity,
        prov / "activity" -> entity.activity.asEntityId.asJsonLD
      )
    }
}
