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

import cats.MonadError
import com.typesafe.config.Config
import pureconfig._
import pureconfig.error.ConfigReaderFailures

import scala.language.higherKinds

abstract class ConfigLoader[Interpretation[_]](implicit ME: MonadError[Interpretation, Throwable]) {

  protected def find[T](key: String, config: Config)(implicit reader: ConfigReader[T]): Interpretation[T] =
    ConfigLoader.find(key, config)
}

object ConfigLoader {

  final class ConfigLoadingException(failures: ConfigReaderFailures) extends Exception {
    override def getMessage: String = failures.toList.map(_.description).mkString("; ")
  }

  def find[Interpretation[_], T](
      key:           String,
      config:        Config
  )(implicit reader: ConfigReader[T], ME: MonadError[Interpretation, Throwable]): Interpretation[T] =
    fromEither {
      loadConfig[T](config, key)
    }

  private def fromEither[Interpretation[_], T](
      loadedConfig: ConfigReaderFailures Either T
  )(implicit ME:    MonadError[Interpretation, Throwable]): Interpretation[T] =
    ME.fromEither[T] {
      import cats.implicits._
      loadedConfig leftMap (new ConfigLoadingException(_))
    }
}
