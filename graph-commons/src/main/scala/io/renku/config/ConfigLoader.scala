/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.config

import cats.MonadThrow
import cats.syntax.all._
import com.typesafe.config.Config
import io.renku.tinytypes._
import pureconfig._
import pureconfig.error.{CannotConvert, ConfigReaderFailures}

abstract class ConfigLoader[F[_]: MonadThrow] {

  protected def find[T](key: String, config: Config)(implicit reader: ConfigReader[T]): F[T] =
    ConfigLoader.find(key, config)
}

object ConfigLoader {

  final class ConfigLoadingException(failures: ConfigReaderFailures) extends Exception {
    override def getMessage: String = failures.toList.map(_.description).mkString("; ")
  }

  def find[F[_]: MonadThrow, T](
      key:           String,
      config:        Config
  )(implicit reader: ConfigReader[T]): F[T] = fromEither {
    ConfigSource.fromConfig(config).at(key).load[T]
  }

  private def fromEither[F[_]: MonadThrow, T](
      loadedConfig: ConfigReaderFailures Either T
  ): F[T] =
    MonadThrow[F].fromEither[T] {
      loadedConfig leftMap (new ConfigLoadingException(_))
    }

  implicit def stringTinyTypeReader[TT <: StringTinyType](implicit ttApply: TinyTypeFactory[TT]): ConfigReader[TT] =
    ConfigReader
      .fromString[TT] { value =>
        ttApply
          .from(value)
          .leftMap(exception => CannotConvert(value, ttApply.getClass.toString, exception.getMessage))
      }

  implicit def urlTinyTypeReader[TT <: UrlTinyType](implicit ttApply: TinyTypeFactory[TT]): ConfigReader[TT] =
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
