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

package ch.datascience.webhookservice.config

import cats.MonadError
import ch.datascience.config.ConfigLoader
import ch.datascience.webhookservice.config.GitLabConfigProvider._
import com.typesafe.config.{Config, ConfigFactory}
import eu.timepit.refined.api.Refined

import scala.language.higherKinds

class GitLabConfigProvider[Interpretation[_]](
    configuration: Config = ConfigFactory.load()
)(implicit ME:     MonadError[Interpretation, Throwable])
    extends ConfigLoader[Interpretation] {
  import eu.timepit.refined.pureconfig._

  def get: Interpretation[HostUrl] = find[HostUrl]("services.gitlab.url", configuration)
}

object GitLabConfigProvider {

  import eu.timepit.refined.string.Url

  type HostUrl = String Refined Url
}
