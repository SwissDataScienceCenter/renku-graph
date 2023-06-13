/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.effect.IO
import cats.syntax.all._
import io.renku.config.certificates.Certificate
import io.renku.generators.CommonGraphGenerators.certificates
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Path, Paths}

class GitCertificateInstallerSpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "run" should {

    "save the certificate into a file and configure git to trust it" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(Some(certificate).pure[IO])

      val path = Paths.get(relativePaths().generateOne)
      (certificateSaver
        .save(_: Certificate))
        .expects(certificate)
        .returning(path.pure[IO])

      (gitConfigModifier
        .makeGitTrust(_: Path))
        .expects(path)
        .returning(IO.unit)

      certInstaller.run.unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("Certificate installed for Git"))
    }

    "do nothing if there's no certificate found" in new TestCase {

      findCertificate.expects().returning(None.pure[IO])

      certInstaller.run.unsafeRunSync() shouldBe ()
    }

    "fail if finding the certificate fails" in new TestCase {

      val exception = exceptions.generateOne
      findCertificate.expects().returning(exception.raiseError[IO, Option[Certificate]])

      intercept[Exception](certInstaller.run.unsafeRunSync()) shouldBe exception

      logger.loggedOnly(Error("Certificate installation for Git failed", exception))
    }

    "fail if saving the certificate fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(Some(certificate).pure[IO])

      val exception = exceptions.generateOne
      (certificateSaver
        .save(_: Certificate))
        .expects(certificate)
        .returning(exception.raiseError[IO, Path])

      intercept[Exception](certInstaller.run.unsafeRunSync()) shouldBe exception

      logger.loggedOnly(Error("Certificate installation for Git failed", exception))
    }

    "fail if changing Git config fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(Some(certificate).pure[IO])

      val path = Paths.get(relativePaths().generateOne)
      (certificateSaver
        .save(_: Certificate))
        .expects(certificate)
        .returning(path.pure[IO])

      val exception = exceptions.generateOne
      (gitConfigModifier
        .makeGitTrust(_: Path))
        .expects(path)
        .returning(exception.raiseError[IO, Unit])

      intercept[Exception](certInstaller.run.unsafeRunSync()) shouldBe exception

      logger.loggedOnly(Error("Certificate installation for Git failed", exception))
    }
  }

  private trait TestCase {

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val findCertificate   = mockFunction[IO[Option[Certificate]]]
    val certificateSaver  = mock[CertificateSaver[IO]]
    val gitConfigModifier = mock[GitConfigModifier[IO]]
    val certInstaller     = new GitCertificateInstallerImpl[IO](findCertificate, certificateSaver, gitConfigModifier)
  }
}
