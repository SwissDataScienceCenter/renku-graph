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

package ch.datascience.graph.config

import cats.MonadThrow
import ch.datascience.config.ConfigLoader.{find, stringTinyTypeReader}
import ch.datascience.tinytypes.constraints.{Url, UrlOps}
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader

final class EventLogUrl private (val value: String) extends AnyVal with StringTinyType
object EventLogUrl extends TinyTypeFactory[EventLogUrl](new EventLogUrl(_)) with Url with UrlOps[EventLogUrl] {

  private implicit val urlReader: ConfigReader[EventLogUrl] = stringTinyTypeReader(EventLogUrl)

  def apply[Interpretation[_]: MonadThrow](
      config: Config = ConfigFactory.load
  ): Interpretation[EventLogUrl] =
    find[Interpretation, EventLogUrl]("services.event-log.url", config)
}
