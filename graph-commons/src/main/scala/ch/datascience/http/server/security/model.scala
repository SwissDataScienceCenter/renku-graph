/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.http.server.security

import cats.effect.Effect
import ch.datascience.graph.model.users
import ch.datascience.http.client.AccessToken
import org.http4s.Response

object model {
  final case class AuthUser(id: users.GitLabId, accessToken: AccessToken)
}

sealed trait EndpointSecurityException extends Exception with Product with Serializable {
  def toHttpResponse[Interpretation[_]: Effect]: Response[Interpretation]
}

object EndpointSecurityException {

  import ch.datascience.http.ErrorMessage._
  import ch.datascience.http.ErrorMessage
  import org.http4s.{Response, Status}

  final case object AuthenticationFailure extends EndpointSecurityException {

    override lazy val getMessage: String = "User authentication failure"

    override def toHttpResponse[Interpretation[_]: Effect]: Response[Interpretation] =
      Response[Interpretation](Status.Unauthorized).withEntity(ErrorMessage(getMessage))
  }
  type AuthenticationFailure = AuthenticationFailure.type

  final case object AuthorizationFailure extends EndpointSecurityException {

    override lazy val getMessage: String = "User not authorized failure"

    override def toHttpResponse[Interpretation[_]: Effect]: Response[Interpretation] =
      Response[Interpretation](Status.Forbidden).withEntity(ErrorMessage(getMessage))
  }
  type AuthorizationFailure = AuthorizationFailure.type
}
