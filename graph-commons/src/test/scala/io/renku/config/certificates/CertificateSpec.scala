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

package io.renku.config.certificates

import cats.syntax.all._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.Try

class CertificateSpec extends AnyWordSpec with should.Matchers {

  "fromConfig" should {

    "return some certificate if it can be found in conf file under the 'client-certificate' property" in {
      val certificateBody = certificateBodies.generateOne

      val config = ConfigFactory.parseMap(
        Map("client-certificate" -> certificateBody).asJava
      )

      Certificate.fromConfig[Try](config) shouldBe Some(Certificate(certificateBody)).pure[Try]
    }

    "return None if the 'client-certificate' property in the conf file has blank value" in {
      val config = ConfigFactory.parseMap(
        Map("client-certificate" -> blankStrings().generateOne).asJava
      )

      Certificate.fromConfig[Try](config) shouldBe None.pure[Try]
    }
  }

  private lazy val certificateBodies = for {
    lines <- nonBlankStrings(minLength = 64, maxLength = 64)
               .map(_.value)
               .toGeneratorOfNonEmptyList(minElements = 20, maxElements = 25)
    lastLine <- nonBlankStrings(maxLength = 64).map(_.value)
  } yield ("-----BEGIN CERTIFICATE-----" +: lines.toList :+ lastLine :+ "-----END CERTIFICATE-----").mkString("\n")
}
