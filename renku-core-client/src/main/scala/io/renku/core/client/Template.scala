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

package io.renku.core.client

import io.renku.tinytypes.constraints.{NonBlank, Url}
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory, UrlTinyType}

final case class Template(repositoryUrl: templates.RepositoryUrl, identifier: templates.Identifier)

object templates {

  final class RepositoryUrl private (val value: String) extends AnyVal with UrlTinyType
  implicit object RepositoryUrl extends TinyTypeFactory[RepositoryUrl](new RepositoryUrl(_)) with Url[RepositoryUrl]

  final class Identifier private (val value: String) extends AnyVal with StringTinyType
  implicit object Identifier extends TinyTypeFactory[Identifier](new Identifier(_)) with NonBlank[Identifier]
}
