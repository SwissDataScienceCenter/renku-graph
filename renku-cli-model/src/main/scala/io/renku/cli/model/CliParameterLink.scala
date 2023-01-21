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

package io.renku.cli.model

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliParameterLink.Sink
import io.renku.cli.model.Ontologies.Renku
import io.renku.graph.model.commandParameters
import io.renku.graph.model.parameterLinks._
import io.renku.jsonld.syntax._
import io.renku.jsonld._

final case class CliParameterLink(
    id:     ResourceId,
    source: CliCommandOutput,
    sinks:  NonEmptyList[Sink]
) extends CliModel

object CliParameterLink {

  sealed trait Sink {
    def resourceId: commandParameters.ResourceId
    def fold[A](fa: CliCommandInput => A, fb: CliCommandParameter => A): A
  }

  object Sink {
    final case class Input(value: CliCommandInput) extends Sink {
      val resourceId: commandParameters.ResourceId = value.resourceId
      def fold[A](fa: CliCommandInput => A, fb: CliCommandParameter => A): A = fa(value)
    }
    final case class Param(value: CliCommandParameter) extends Sink {
      val resourceId: commandParameters.ResourceId = value.resourceId
      def fold[A](fa: CliCommandInput => A, fb: CliCommandParameter => A): A = fb(value)
    }

    def apply(value: CliCommandInput):     Sink = Input(value)
    def apply(value: CliCommandParameter): Sink = Param(value)

    private val entityTypes: EntityTypes = EntityTypes.of(Renku.CommandParameterBase)

    implicit val jsonLDDecoder: JsonLDDecoder[Sink] = {
      val in    = CliCommandInput.jsonLDDecoder.emap(input => Sink(input).asRight)
      val param = CliCommandParameter.jsonLDDecoder.emap(param => Sink(param).asRight)

      JsonLDDecoder.entity(
        entityTypes,
        _.getEntityTypes.map(ets =>
          CliCommandInput.matchingEntityTypes(ets) || CliCommandParameter.matchingEntityTypes(ets)
        )
      ) { cursor =>
        val currentEntityTypes = cursor.getEntityTypes
        (currentEntityTypes.map(CliCommandInput.matchingEntityTypes),
         currentEntityTypes.map(CliCommandParameter.matchingEntityTypes)
        ).flatMapN {
          case (true, _) => in(cursor)
          case (_, true) => param(cursor)
          case _ =>
            DecodingFailure(s"Invalid entity for decoding parameter link sink: $currentEntityTypes", Nil).asLeft
        }
      }
    }

    implicit val jsonLDEncoder: JsonLDEncoder[Sink] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Renku.ParameterLink)

  implicit val jsonLDDecoder: JsonLDDecoder[CliParameterLink] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        id     <- cursor.downEntityId.as[ResourceId]
        source <- cursor.downField(Renku.linkSource).as[CliCommandOutput]
        sinks  <- cursor.downField(Renku.linkSink).as[NonEmptyList[Sink]]
      } yield CliParameterLink(id, source, sinks)
    }

  implicit val jsonLDEncoder: JsonLDEncoder[CliParameterLink] =
    JsonLDEncoder.instance { link =>
      JsonLD.entity(
        link.id.asEntityId,
        entityTypes,
        Renku.linkSource -> link.source.asJsonLD,
        Renku.linkSink   -> link.sinks.asJsonLD
      )
    }
}
