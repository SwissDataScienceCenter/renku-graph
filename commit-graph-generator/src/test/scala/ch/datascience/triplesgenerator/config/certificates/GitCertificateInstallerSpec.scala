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

package ch.datascience.triplesgenerator.config.certificates

import cats.Applicative
import cats.syntax.all._
import ch.datascience.config.certificates.Certificate
import ch.datascience.generators.CommonGraphGenerators.certificates
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Path, Paths}
import scala.util.Try

class GitCertificateInstallerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "run" should {

    "save the certificate into a file and configure git to trust it" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(Some(certificate).pure[Try])

      val path = Paths.get(relativePaths().generateOne)
      (certificateSaver
        .save(_: Certificate))
        .expects(certificate)
        .returning(path.pure[Try])

      (gitConfigModifier
        .makeGitTrust(_: Path))
        .expects(path)
        .returning(Applicative[Try].unit)

      certInstaller.run() shouldBe Applicative[Try].unit

      logger.loggedOnly(Info("Certificate installed for Git"))
    }

    "do nothing if there's no certificate found" in new TestCase {

      findCertificate.expects().returning(None.pure[Try])

      certInstaller.run() shouldBe Applicative[Try].unit
    }

    "fail if finding the certificate fails" in new TestCase {

      val exception = exceptions.generateOne
      findCertificate.expects().returning(exception.raiseError[Try, Option[Certificate]])

      certInstaller.run() shouldBe exception.raiseError[Try, Unit]

      logger.loggedOnly(Error("Certificate installation for Git failed", exception))
    }

    "fail if saving the certificate fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(Some(certificate).pure[Try])

      val exception = exceptions.generateOne
      (certificateSaver
        .save(_: Certificate))
        .expects(certificate)
        .returning(exception.raiseError[Try, Path])

      certInstaller.run() shouldBe exception.raiseError[Try, Unit]

      logger.loggedOnly(Error("Certificate installation for Git failed", exception))
    }

    "fail if changing Git config fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(Some(certificate).pure[Try])

      val path = Paths.get(relativePaths().generateOne)
      (certificateSaver
        .save(_: Certificate))
        .expects(certificate)
        .returning(path.pure[Try])

      val exception = exceptions.generateOne
      (gitConfigModifier
        .makeGitTrust(_: Path))
        .expects(path)
        .returning(exception.raiseError[Try, Unit])

      certInstaller.run() shouldBe exception.raiseError[Try, Unit]

      logger.loggedOnly(Error("Certificate installation for Git failed", exception))
    }
  }

  private trait TestCase {

    val findCertificate   = mockFunction[Try[Option[Certificate]]]
    val certificateSaver  = mock[CertificateSaver[Try]]
    val gitConfigModifier = mock[GitConfigModifier[Try]]
    val logger            = TestLogger[Try]()

    val certInstaller =
      new GitCertificateInstallerImpl[Try](findCertificate, certificateSaver, gitConfigModifier, logger)
  }
}
