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

package io.renku.entities.search

import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.model.images.ImageUri
import io.renku.tinytypes._

object DecodingTools {

  def toListOf[TT <: StringTinyType, TTF <: TinyTypeFactory[TT]](separator: Char = ',')(implicit
      ttFactory: TTF
  ): Option[String] => Decoder.Result[List[TT]] =
    _.map(_.split(separator).toList.distinct.map(v => ttFactory.from(v)).sequence.map(_.sortBy(_.value))).sequence
      .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
      .map(_.getOrElse(List.empty))

  def toListOfImageUris(separator: Char = ','): Option[String] => Decoder.Result[List[ImageUri]] =
    _.map(ImageUri.fromSplitString(separator))
      .map(_.leftMap(ex => DecodingFailure(ex.getMessage, Nil)))
      .getOrElse(Nil.asRight)
}
