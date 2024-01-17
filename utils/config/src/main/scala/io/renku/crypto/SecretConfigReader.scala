/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.crypto

import cats.syntax.all._
import io.renku.config.ConfigLoader.{asciiByteVectorReader, base64ByteVectorReader}
import pureconfig.ConfigReader
import pureconfig.error.FailureReason

trait SecretConfigReader {
  implicit def secretReader: ConfigReader[Secret] =
    base64SecretReader orElse asciiSecretReader

  private lazy val base64SecretReader =
    base64ByteVectorReader.emap { bv =>
      Secret(bv.takeWhile(_ != 10))
        .leftMap(err =>
          new FailureReason {
            override lazy val description: String = s"Cannot read AES secret: $err"
          }
        )
    }

  private lazy val asciiSecretReader =
    asciiByteVectorReader.emap { bv =>
      Secret(bv).leftMap(err =>
        new FailureReason {
          override lazy val description: String = s"Cannot read AES secret: $err"
        }
      )
    }
}

object SecretConfigReader extends SecretConfigReader
