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

package ch.datascience.config

import java.net.URL

import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import com.typesafe.config.Config
import play.api.{ConfigLoader => PlayConfigLoader}

import scala.language.implicitConversions

class ServiceUrl private (val value: URL) extends AnyVal with TinyType[URL]

object ServiceUrl extends TinyTypeFactory[URL, ServiceUrl](new ServiceUrl(_)) {

  def apply(url: String): ServiceUrl = ServiceUrl(new URL(url))

  implicit object ServiceUrlFinder extends PlayConfigLoader[ServiceUrl] {
    override def load(config: Config, path: String): ServiceUrl = ServiceUrl(config.getString(path))
  }

  implicit class ServiceUrlOps(serviceUrl: ServiceUrl) {
    def /(value: Any): ServiceUrl = ServiceUrl(
      new URL(s"$serviceUrl/$value")
    )
  }

  implicit def asString(serviceUrl: ServiceUrl): String = serviceUrl.toString
}
