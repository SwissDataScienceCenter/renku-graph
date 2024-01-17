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

package io.renku.config

import io.renku.config.certificates.Certificate
import io.renku.crypto.Secret
import io.renku.generators.Generators
import io.renku.generators.Generators.Implicits._
import org.scalacheck.Gen
import scodec.bits.ByteVector

trait ConfigGenerators {

  implicit val aesCryptoSecrets: Gen[Secret] =
    Gen
      .oneOf(16, 24, 32)
      .flatMap { length =>
        Gen
          .listOfN(length, Gen.asciiPrintableChar)
          .map(_.mkString)
          .map(_.getBytes("US-ASCII"))
          .map(ByteVector(_))
          .map(Secret.unsafe)
      }

  implicit lazy val serviceNames: Gen[ServiceName] = Generators.nonEmptyStrings().toGeneratorOf(ServiceName)

  implicit lazy val serviceVersions: Gen[ServiceVersion] = for {
    version       <- Generators.semanticVersions
    commitsNumber <- Generators.positiveInts(999)
    commitPart <-
      Generators.shas.toGeneratorOfOptions.map(_.map(_.take(8)).map(sha => s"-$commitsNumber-g$sha").getOrElse(""))
  } yield ServiceVersion(s"$version$commitPart")

  implicit lazy val certificates: Gen[Certificate] =
    Generators
      .nonBlankStrings()
      .toGeneratorOfNonEmptyList(min = 2)
      .map { lines =>
        Certificate {
          lines.toList.mkString("-----BEGIN CERTIFICATE-----\n", "\n", "\n-----END CERTIFICATE-----")
        }
      }

}

object ConfigGenerators extends ConfigGenerators
