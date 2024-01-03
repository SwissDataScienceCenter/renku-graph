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

package io.renku.triplesgenerator.api.events

import cats.Show
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure, Encoder}
import io.circe.literal._
import io.renku.graph.model.persons
import io.renku.triplesstore.client.model.TripleObjectEncoder
import io.renku.triplesstore.client.syntax._
import io.renku.graph.model.views.TinyTypeToObject._

sealed trait UserId extends Product {
  lazy val widen: UserId = this
  def fold[A](glidF: persons.GitLabId => A, emailF: persons.Email => A): A
}

object UserId {

  def apply(value: persons.GitLabId): UserId.GLId = UserId.GLId(value)

  def apply(value: persons.Email): UserId.Email = UserId.Email(value)

  final case class GLId(value: persons.GitLabId) extends UserId {
    override def fold[A](glidF: persons.GitLabId => A, emailF: persons.Email => A): A = glidF(value)
  }

  final case class Email(value: persons.Email) extends UserId {
    override def fold[A](glidF: persons.GitLabId => A, emailF: persons.Email => A): A = emailF(value)
  }

  implicit def encoder[U <: UserId]: Encoder[U] = Encoder.instance {
    case UserId.GLId(id)     => json"""{"id": $id}"""
    case UserId.Email(email) => json"""{"email": $email}"""
  }

  implicit val decoder: Decoder[UserId] = cursor => {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    cursor.downField("id").as[Option[persons.GitLabId]] >>= {
      case Some(id) => UserId.GLId(id).widen.asRight
      case None =>
        cursor.downField("email").as[Option[persons.Email]] >>= {
          case Some(email) => UserId.Email(email).widen.asRight
          case None =>
            DecodingFailure(DecodingFailure.Reason.CustomReason("Neither id nor email on the user"), cursor).asLeft
        }
    }
  }

  implicit val tripleObjectEncoder: TripleObjectEncoder[UserId] = TripleObjectEncoder.instance {
    case UserId.GLId(id)     => id.asObject
    case UserId.Email(email) => email.asObject
  }

  implicit val show: Show[UserId] = Show.show {
    case UserId.GLId(id)     => id.show
    case UserId.Email(email) => email.show
  }
}
