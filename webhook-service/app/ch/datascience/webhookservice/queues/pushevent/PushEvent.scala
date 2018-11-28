/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.queues.pushevent

import ch.datascience.tinytypes.constraints.{ GitSha, NonBlank }
import ch.datascience.tinytypes.{ TinyType, TinyTypeFactory }

case class PushEvent(
    checkoutSha:      CheckoutSha,
    gitRepositoryUrl: GitRepositoryUrl,
    projectName:      ProjectName
)

class CheckoutSha private ( val value: String ) extends AnyVal with TinyType[String]
object CheckoutSha
  extends TinyTypeFactory[String, CheckoutSha]( new CheckoutSha( _ ) )
  with GitSha

class GitRepositoryUrl private ( val value: String ) extends AnyVal with TinyType[String]
object GitRepositoryUrl
  extends TinyTypeFactory[String, GitRepositoryUrl]( new GitRepositoryUrl( _ ) )
  with NonBlank

class ProjectName private ( val value: String ) extends AnyVal with TinyType[String]
object ProjectName
  extends TinyTypeFactory[String, ProjectName]( new ProjectName( _ ) )
  with NonBlank
