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

package ch.datascience.config

import cats.MonadError
import cats.syntax.all._
import ch.datascience.tinytypes.{IntTinyType, StringTinyType, TinyTypeFactory}
import com.typesafe.config.Config
import pureconfig._
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

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
      ConfigSource.fromConfig(config).at(key).load[T]
    }

  private def fromEither[Interpretation[_], T](
      loadedConfig: ConfigReaderFailures Either T
  )(implicit ME:    MonadError[Interpretation, Throwable]): Interpretation[T] =
    ME.fromEither[T] {

      loadedConfig leftMap (new ConfigLoadingException(_))
    }

  implicit def stringTinyTypeReader[TT <: StringTinyType](implicit ttApply: TinyTypeFactory[TT]): ConfigReader[TT] =
    ConfigReader
      .fromString[TT] { value =>
        ttApply
          .from(value)
          .leftMap(exception => CannotConvert(value, ttApply.getClass.toString, exception.getMessage))
      }

  implicit def intTinyTypeReader[TT <: IntTinyType](implicit ttApply: TinyTypeFactory[TT]): ConfigReader[TT] =
    ConfigReader
      .fromString[TT] { stringValue =>
        stringValue.toIntOption
          .map { intValue =>
            ttApply
              .from(intValue)
              .leftMap(exception => CannotConvert(stringValue, ttApply.getClass.toString, exception.getMessage))
          }
          .getOrElse(Left(CannotConvert(stringValue, ttApply.getClass.toString, "Not an int value")))

      }
}
