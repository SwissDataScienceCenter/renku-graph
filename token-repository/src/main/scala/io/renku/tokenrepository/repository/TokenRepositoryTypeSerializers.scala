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

package io.renku.tokenrepository.repository

import AccessTokenCrypto.EncryptedAccessToken
import cats.syntax.all._
import io.renku.graph.model.projects
import skunk.codec.all.{int4, varchar}
import skunk.{Decoder, Encoder}

private trait TokenRepositoryTypeSerializers {
  val projectIdDecoder: Decoder[projects.Id] = int4.map(projects.Id.apply)
  val projectIdEncoder: Encoder[projects.Id] = int4.values.contramap(_.value)

  val projectPathDecoder: Decoder[projects.Path] = varchar.map(projects.Path.apply)
  val projectPathEncoder: Encoder[projects.Path] =
    varchar.values.contramap((b: projects.Path) => b.value)

  val encryptedAccessTokenDecoder: Decoder[EncryptedAccessToken] =
    varchar.emap(s => EncryptedAccessToken.from(s).leftMap(_.getMessage))
  val encryptedAccessTokenEncoder: Encoder[EncryptedAccessToken] =
    varchar.values.contramap((b: EncryptedAccessToken) => b.value)
}
