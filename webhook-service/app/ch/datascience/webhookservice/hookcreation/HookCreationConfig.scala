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

package ch.datascience.webhookservice.hookcreation

import cats.effect.{ IO, Sync }
import cats.implicits._
import eu.timepit.refined.api.Refined
import play.api.Configuration
import pureconfig.module.catseffect._

import scala.language.higherKinds
import HookCreationConfig._
import javax.inject.{ Inject, Singleton }

private class HookCreationConfigProvider[Interpretation[_]]( configuration: Configuration ) {
  import eu.timepit.refined.pureconfig._

  def get()( implicit F: Sync[Interpretation] ): Interpretation[HookCreationConfig] = (
    loadConfigF[Interpretation, HostUrl]( configuration.underlying, "services.self.url" ),
    loadConfigF[Interpretation, HostUrl]( configuration.underlying, "services.gitlab.url" )
  ) mapN {
      case ( selfUrl, gitLabUrl ) =>
        HookCreationConfig( gitLabUrl, selfUrl )
    }
}

@Singleton
private class IOHookCreationConfigProvider @Inject() ( configuration: Configuration )
  extends HookCreationConfigProvider[IO]( configuration )

private case class HookCreationConfig(
    gitLabUrl: HostUrl,
    selfUrl:   HostUrl
)
private object HookCreationConfig {

  import eu.timepit.refined.string.Url

  type HostUrl = String Refined Url
}
