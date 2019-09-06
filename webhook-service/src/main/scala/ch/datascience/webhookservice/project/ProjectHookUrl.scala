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

package ch.datascience.webhookservice.project

import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader.{find, stringTinyTypeReader}
import ch.datascience.tinytypes.constraints.{Url, UrlOps}
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader

import scala.language.higherKinds

final class ProjectHookUrl private (val value: String) extends AnyVal with StringTinyType
object ProjectHookUrl {

  def fromConfig[Interpretation[_]](
      config:    Config = ConfigFactory.load
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[ProjectHookUrl] =
    SelfUrl[Interpretation](config)
      .map(from)

  def from(selfUrl: SelfUrl): ProjectHookUrl = new ProjectHookUrl((selfUrl / "webhooks" / "events").value)
}

final class SelfUrl private (val value: String) extends AnyVal with StringTinyType
object SelfUrl extends TinyTypeFactory[SelfUrl](new SelfUrl(_)) with Url with UrlOps[SelfUrl] {

  private implicit val configReader: ConfigReader[SelfUrl] = stringTinyTypeReader(SelfUrl)

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[SelfUrl] =
    find[Interpretation, SelfUrl]("services.self.url", config)
}
