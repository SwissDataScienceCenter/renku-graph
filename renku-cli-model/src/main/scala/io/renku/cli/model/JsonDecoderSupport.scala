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

import io.renku.jsonld.{JsonLD, Property, Reverse}

private[model] trait JsonDecoderSupport {

  def spliceArrays(jsonld: JsonLD): JsonLD =
    jsonld match {
      case a: JsonLD.JsonLDArray  => spliceArrays(a)
      case a: JsonLD.JsonLDEntity => spliceArrays(a)
      case a => a
    }

  def spliceArrays(entity: JsonLD.JsonLDEntity): JsonLD.JsonLDEntity = {
    def spliceProperties(in: Map[Property, JsonLD]): Map[Property, JsonLD] =
      in.map { case (key, value) =>
        (key, spliceArrays(value))
      }

    JsonLD.JsonLDEntity(
      entity.id,
      entity.types,
      spliceProperties(entity.properties),
      Reverse(spliceProperties(entity.reverse.properties))
    )
  }

  def spliceArrays(array: JsonLD.JsonLDArray): JsonLD.JsonLDArray =
    JsonLD.JsonLDArray(array.jsons.flatMap {
      case JsonLD.JsonLDArray(children) => children.map(spliceArrays)
      case el                           => List(spliceArrays(el))
    })
}

private[model] object JsonDecoderSupport extends JsonDecoderSupport
