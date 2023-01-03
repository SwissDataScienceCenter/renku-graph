/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.commandParameters.{ResourceId => ParamResourceId}
import io.renku.graph.model.entities.StepPlanCommandParameter.{CommandInput, CommandOutput, CommandParameter}
import io.renku.graph.model.parameterLinks.ResourceId
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}
import io.renku.jsonld.ontology.{Class, ObjectProperty, Type}
import io.renku.jsonld.syntax._

/** A parameter link has a source reference to a CommandOutputSchema and 1-n references to CommandInput- or
 *  CommandParameterSchema. 
 */
final case class ParameterLink(resourceId: ResourceId, source: ParamResourceId, sinks: NonEmptyList[ParamResourceId])

object ParameterLink {

  // noinspection TypeAnnotation
  object Ontology {
    val parameterLinkClass = Class(renku / "ParameterLink")
    val entityTypes: EntityTypes = EntityTypes.of(renku / "ParameterLink")

    val linkSource = renku / "linkSource"
    val linkSink   = renku / "linkSink"

    val typeDef: Type = Type.Def(
      parameterLinkClass,
      ObjectProperty(linkSource, CommandOutput.ontology),
      ObjectProperty(linkSink, CommandInput.ontology, CommandParameter.ontology)
    )
  }

  implicit def encoder: JsonLDEncoder[ParameterLink] =
    JsonLDEncoder.instance { link =>
      JsonLD.entity(
        link.resourceId.asEntityId,
        Ontology.entityTypes,
        Ontology.linkSource -> link.source.asJsonLD,
        Ontology.linkSink   -> link.sinks.toList.asJsonLD
      )
    }

  implicit def decoder: JsonLDDecoder[ParameterLink] =
    JsonLDDecoder.entity(Ontology.entityTypes) { cursor =>
      for {
        id     <- cursor.downEntityId.as[ResourceId]
        source <- cursor.downField(Ontology.linkSource).as[ParamResourceId]
        sinks  <- cursor.downField(Ontology.linkSink).as[NonEmptyList[ParamResourceId]]
      } yield ParameterLink(id, source, sinks)
    }
}
