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

package io.renku.http.server.security

import cats.Applicative
import cats.syntax.all._
import io.renku.graph.model.persons
import io.renku.http.ErrorMessage._
import io.renku.http.InfoMessage.messageJsonEntityEncoder
import io.renku.http.client.UserAccessToken
import io.renku.http.server.security.EndpointSecurityException.AuthenticationFailure
import io.renku.http.{ErrorMessage, InfoMessage}
import org.http4s.{Response, Status}

import java.util.Objects

object model {
  final case class AuthUser(id: persons.GitLabId, accessToken: UserAccessToken)

  final class MaybeAuthUser(private val user: Option[AuthUser]) {
    val option: Option[AuthUser] = user
    val required: Either[EndpointSecurityException, AuthUser] =
      user.toRight(AuthenticationFailure: EndpointSecurityException)

    def withAuthenticatedUser[F[_]: Applicative](code: AuthUser => F[Response[F]]): F[Response[F]] =
      required.fold(_.toHttpResponse[F].pure[F], code)

    def withUserOrNotFound[F[_]: Applicative](code: AuthUser => F[Response[F]]): F[Response[F]] =
      required.fold(_ => Response.notFound[F].withEntity(InfoMessage("Resource not found")).pure[F], code)

    override def equals(obj: Any): Boolean = obj match {
      case o: MaybeAuthUser => o.user == user
      case _ => false
    }

    override def hashCode(): Int = Objects.hashCode(user, "MaybeUser")
  }

  object MaybeAuthUser {
    val noUser: MaybeAuthUser = new MaybeAuthUser(None)
    def apply(user: Option[AuthUser]): MaybeAuthUser = new MaybeAuthUser(user)
    def apply(user: AuthUser):         MaybeAuthUser = apply(Some(user))
  }
}

sealed trait EndpointSecurityException extends Exception with Product with Serializable {
  def toHttpResponse[F[_]]: Response[F]
}

object EndpointSecurityException {

  final case object AuthenticationFailure extends EndpointSecurityException {

    override lazy val getMessage: String = "User authentication failure"

    override def toHttpResponse[F[_]]: Response[F] =
      Response[F](Status.Unauthorized).withEntity(ErrorMessage(getMessage))
  }
  type AuthenticationFailure = AuthenticationFailure.type

  final case object AuthorizationFailure extends EndpointSecurityException {

    override lazy val getMessage: String = "Resource not found"

    override def toHttpResponse[F[_]]: Response[F] =
      Response[F](Status.NotFound).withEntity(ErrorMessage(getMessage))
  }
  type AuthorizationFailure = AuthorizationFailure.type
}
