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

package io.renku.entities.search

import cats.syntax.all._

private object DecodingTools {

  import io.circe.{Decoder, DecodingFailure}
  import io.renku.tinytypes._

  def toListOf[TT <: StringTinyType, TTF <: TinyTypeFactory[TT]](implicit
      ttFactory: TTF
  ): Option[String] => Decoder.Result[List[TT]] =
    _.map(_.split(',').toList.map(v => ttFactory.from(v)).sequence.map(_.sortBy(_.value))).sequence
      .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
      .map(_.getOrElse(List.empty))

  def toListOfImageUris[TT <: TinyType { type V = String }, TTF <: From[TT]](implicit
      ttFactory: TTF
  ): Option[String] => Decoder.Result[List[TT]] =
    _.map(
      _.split(",")
        .map(_.trim)
        .map { case s"$position:$url" => ttFactory.from(url).map(tt => position.toIntOption.getOrElse(0) -> tt) }
        .toList
        .sequence
        .map(_.distinct.sortBy(_._1).map(_._2))
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
    ).getOrElse(Nil.asRight)
}
