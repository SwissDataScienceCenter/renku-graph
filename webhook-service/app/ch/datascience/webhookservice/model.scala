/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice

import ch.datascience.graph.events.ProjectId
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}

object model {

  sealed trait AccessToken extends Any with TinyType[String]

  object AccessToken {

    final class PersonalAccessToken private (val value: String) extends AnyVal with AccessToken
    object PersonalAccessToken
        extends TinyTypeFactory[String, PersonalAccessToken](new PersonalAccessToken(_))
        with NonBlank

    final class OAuthAccessToken private (val value: String) extends AnyVal with AccessToken
    object OAuthAccessToken extends TinyTypeFactory[String, OAuthAccessToken](new OAuthAccessToken(_)) with NonBlank
  }

  final class ProjectAccessToken private (val value: String) extends AnyVal with AccessToken
  object ProjectAccessToken extends TinyTypeFactory[String, ProjectAccessToken](new ProjectAccessToken(_)) with NonBlank

  final case class HookToken(
      projectId:          ProjectId,
      projectAccessToken: ProjectAccessToken
  )
}
