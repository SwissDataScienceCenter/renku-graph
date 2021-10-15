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

package io.renku.triplesgenerator.config.certificates

import ch.datascience.generators.CommonGraphGenerators.certificates
import ch.datascience.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Files
import scala.io.Source
import scala.util.{Success, Try}

class CertificateSaverSpec extends AnyWordSpec with should.Matchers {

  "save" should {

    "save the given certificate into a file" in new TestCase {
      val certificate = certificates.generateOne

      val Success(path) = certSaver.save(certificate)

      try {
        val source = Source.fromFile(path.toAbsolutePath.toString)
        source.getLines().toList shouldBe certificate.toString.split("\n").toList
        source.close()
      } finally {
        Files.deleteIfExists(path) && Files.deleteIfExists(path.getParent)
        ()
      }
    }
  }

  private trait TestCase {
    val certSaver = new CertificateSaverImpl[Try]()
  }
}
