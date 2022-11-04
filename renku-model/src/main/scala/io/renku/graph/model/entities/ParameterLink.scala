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

package io.renku.graph.model.entities

import cats.data.NonEmptyList
import io.renku.graph.model.Schemas.{renku, schema}
import io.renku.graph.model.commandParameters.{ResourceId => ParamResourceId}
import io.renku.graph.model.entities.CommandParameterBase.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.parameterLinks.ResourceId
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDEncoder}
import io.renku.jsonld.ontology.{Class, ObjectProperty, Type}
import io.renku.jsonld.syntax._

/** A parameter link has a source reference to a CommandOutputSchema and 1-n references to CommandInput- or
 *  CommandParameterSchema. 
 */
final case class ParameterLink(resourceId: ResourceId, source: ParamResourceId, sinks: NonEmptyList[ParamResourceId])

object ParameterLink {
  val entityTypes: EntityTypes = EntityTypes.of(renku / "ParameterLink")

  val ontology: Type = Type.Def(
    Class(renku / "ParameterLink"),
    ObjectProperty(renku / "linkSource", CommandOutput.ontology),
    ObjectProperty(renku / "linkSink", CommandInput.ontology, CommandParameter.ontology)
  )

  implicit def encoder: JsonLDEncoder[ParameterLink] =
    JsonLDEncoder.instance { link =>
      JsonLD.entity(
        link.resourceId.asEntityId,
        entityTypes,
        schema / "linkSource" -> link.source.asJsonLD,
        schema / "linkSink"   -> link.sinks.toList.asJsonLD
      )
    }
}
