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

package io.renku.webhookservice

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader.{find, urlTinyTypeReader}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Id
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory, UrlTinyType}
import pureconfig.ConfigReader

object model {
  final case class HookToken(projectId: Id)

  final case class CommitSyncRequest(project: Project)

  final case class Project(id: projects.Id, path: projects.Path)

  final class ProjectHookUrl private (val value: String) extends AnyVal with StringTinyType

  object ProjectHookUrl {

    def fromConfig[Interpretation[_]: MonadThrow](
        config: Config = ConfigFactory.load
    ): Interpretation[ProjectHookUrl] =
      SelfUrl[Interpretation](config).map(from)

    def from(selfUrl: SelfUrl): ProjectHookUrl = new ProjectHookUrl((selfUrl / "webhooks" / "events").value)
  }

  final class SelfUrl private (val value: String) extends AnyVal with UrlTinyType

  object SelfUrl extends TinyTypeFactory[SelfUrl](new SelfUrl(_)) with Url with UrlOps[SelfUrl] {

    private implicit val configReader: ConfigReader[SelfUrl] = urlTinyTypeReader(SelfUrl)

    def apply[Interpretation[_]: MonadThrow](
        config: Config = ConfigFactory.load
    ): Interpretation[SelfUrl] =
      find[Interpretation, SelfUrl]("services.self.url", config)
  }
}
