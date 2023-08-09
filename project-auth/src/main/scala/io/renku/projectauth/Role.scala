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

package io.renku.projectauth

import cats.Order
import cats.data.NonEmptyList
import io.circe.{Decoder, Encoder}

sealed trait Role extends Ordered[Role] {
  def asString: String
}

object Role {
  case object Owner extends Role {
    val asString = "owner"

    override def compare(that: Role): Int =
      if (that == this) 0 else 1
  }
  case object Maintainer extends Role {
    val asString = "maintainer"

    override def compare(that: Role): Int =
      if (that == this) 0
      else if (that == Owner) -1
      else 1
  }
  case object Reader extends Role {
    val asString = "reader"

    override def compare(that: Role): Int =
      if (that == this) 0
      else -1
  }

  val all: NonEmptyList[Role] =
    NonEmptyList.of(Owner, Maintainer, Reader)

  def fromString(str: String): Either[String, Role] =
    all.find(_.asString.equalsIgnoreCase(str)).toRight(s"Invalid role name: $str")

  def unsafeFromString(str: String): Role =
    fromString(str).fold(sys.error, identity)

  /** Translated from here: https://docs.gitlab.com/ee/api/members.html#roles */
  def fromGitLabAccessLevel(accessLevel: Int) =
    accessLevel match {
      case n if n >= 50 => Owner
      case n if n >= 40 => Maintainer
      case _            => Reader
    }

  def toGitLabAccessLevel(role: Role): Int =
    role match {
      case Role.Owner      => 50
      case Role.Maintainer => 40
      case Role.Reader     => 20
    }

  implicit val ordering: Ordering[Role] =
    Ordering.by(r => -all.toList.indexOf(r))

  implicit val order: Order[Role] =
    Order.fromOrdering

  implicit val jsonDecoder: Decoder[Role] =
    Decoder.decodeString.emap(fromString)

  implicit val jsonEncoder: Encoder[Role] =
    Encoder.encodeString.contramap(_.asString)
}
