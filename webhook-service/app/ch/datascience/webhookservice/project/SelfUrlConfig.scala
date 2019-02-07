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

import SelfUrlConfig.SelfUrl
import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.config.ConfigLoader
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import eu.timepit.refined.api.{RefType, Refined}
import javax.inject.{Inject, Singleton}
import play.api.Configuration
import pureconfig._
import pureconfig.error.CannotConvert

import scala.language.higherKinds

private class SelfUrlConfig[Interpretation[_]](
    configuration: Configuration
)(implicit ME:     MonadError[Interpretation, Throwable])
    extends ConfigLoader[Interpretation] {

  private implicit val selfUrlReader: ConfigReader[SelfUrl] =
    ConfigReader.fromString[SelfUrl] { value =>
      SelfUrl.from(value).leftMap { exception =>
        CannotConvert(value, SelfUrl.getClass.toString, exception.toString)
      }
    }

  def get(): Interpretation[SelfUrl] = find[SelfUrl]("services.self.url", configuration.underlying)
}

@Singleton
private class IOSelfUrlConfigProvider @Inject()(configuration: Configuration) extends SelfUrlConfig[IO](configuration)

object SelfUrlConfig {

  import eu.timepit.refined.string.Url

  class SelfUrl private (val value: String) extends AnyVal with TinyType[String]
  object SelfUrl extends TinyTypeFactory[String, SelfUrl](new SelfUrl(_)) {
    private type SelfUrlValue = String Refined Url

    addConstraint(
      check = RefType
        .applyRef[SelfUrlValue](_)
        .isRight,
      message = (value: String) => s"'$value' is not a valid $typeName"
    )
  }
}
