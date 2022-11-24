/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.Show

// This prefix is the default prefix for all Project and Personal Access Tokens GitLab generates.
// The reason it cannot be used in production code either for validation or classification
// is that it's configurable and may be different on various GL instances.
case object ProjectAccessTokenDefaultPrefix {

  val value: String = "glpat-"

  override val toString: String = value

  val show: Show[ProjectAccessTokenDefaultPrefix.type] = Show.show(_.value)

  def exists(v: String): Boolean = v startsWith value
}
