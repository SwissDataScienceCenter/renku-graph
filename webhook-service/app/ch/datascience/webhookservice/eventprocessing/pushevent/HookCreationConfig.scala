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

package ch.datascience.webhookservice.eventprocessing.pushevent

import cats.effect.{IO, Sync}
import ch.datascience.webhookservice.eventprocessing.pushevent.GitLabConfig._
import eu.timepit.refined.api.Refined
import javax.inject.{Inject, Singleton}
import play.api.Configuration
import pureconfig.module.catseffect._

import scala.language.higherKinds

private class GitLabConfig[Interpretation[_]](configuration: Configuration) {
  import eu.timepit.refined.pureconfig._

  def get()(implicit F: Sync[Interpretation]): Interpretation[HostUrl] =
    loadConfigF[Interpretation, HostUrl](configuration.underlying, "services.gitlab.url")
}

@Singleton
private class IOGitLabConfigProvider @Inject()(configuration: Configuration) extends GitLabConfig[IO](configuration)

private object GitLabConfig {

  import eu.timepit.refined.string.Url

  type HostUrl = String Refined Url
}
