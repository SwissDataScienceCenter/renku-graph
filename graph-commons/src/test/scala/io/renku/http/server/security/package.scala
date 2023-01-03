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

package io.renku.http.server

import io.renku.http.client.AccessToken
import io.renku.http.client.AccessToken._
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.Header
import org.http4s.Header.ToRaw._
import org.http4s.headers.Authorization
import org.http4s.headers.Authorization._
import org.typelevel.ci._

package object security {

  implicit class AccessTokenOps(accessToken: AccessToken) {

    lazy val toHeader: Header.Raw = accessToken match {
      case UserOAuthAccessToken(token) => modelledHeadersToRaw(Authorization(Token(Bearer, token))).values.head
      case PersonalAccessToken(token)  => Header.Raw(ci"PRIVATE-TOKEN", token)
      case ProjectAccessToken(token)   => modelledHeadersToRaw(Authorization(Token(Bearer, token))).values.head
    }
  }
}
