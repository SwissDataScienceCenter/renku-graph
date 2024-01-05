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

package io.renku.http.client

import com.github.tomakehurst.wiremock.client.MappingBuilder
import com.github.tomakehurst.wiremock.client.WireMock.equalTo
import io.renku.http.client.AccessToken._

trait GitLabClientMappings {
  implicit class MappingBuilderOps(mappingBuilder: MappingBuilder) {
    lazy val withAccessToken: Option[AccessToken] => MappingBuilder = {
      case Some(ProjectAccessToken(token))   => mappingBuilder.withHeader("Authorization", equalTo(s"Bearer $token"))
      case Some(UserOAuthAccessToken(token)) => mappingBuilder.withHeader("Authorization", equalTo(s"Bearer $token"))
      case Some(PersonalAccessToken(token))  => mappingBuilder.withHeader("PRIVATE-TOKEN", equalTo(token))
      case None                              => mappingBuilder
    }
  }
}
