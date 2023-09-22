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

package io.renku.triplesgenerator.events.consumers.projectinfo

import io.circe.Decoder
import io.renku.graph.model.gitlab.{GitLabMember, GitLabUser}
import io.renku.graph.model.persons._
import io.renku.tinytypes.json.TinyTypeDecoders._

trait GitlabJsonDecoder {

  implicit val memberDecoder: Decoder[GitLabMember] = cursor =>
    for {
      gitLabId    <- cursor.downField("id").as[GitLabId]
      name        <- cursor.downField("name").as[Name]
      username    <- cursor.downField("username").as[Username]
      accessLevel <- cursor.downField("access_level").as[Int]
      email       <- cursor.downField("email").as[Option[Email]]
    } yield GitLabMember(name, username, gitLabId, email, accessLevel)

  implicit val userDecoder: Decoder[GitLabUser] = cursor =>
    for {
      gitLabId <- cursor.downField("id").as[GitLabId]
      name     <- cursor.downField("name").as[Name]
      username <- cursor.downField("username").as[Username]
      email1   <- cursor.downField("email").as[Option[Email]]
      email2   <- cursor.downField("public_email").as[Option[Email]]
    } yield GitLabUser(name, username, gitLabId, email1.orElse(email2))

}