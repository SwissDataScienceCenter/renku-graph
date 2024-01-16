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

package io.renku.config.certificates

import cats.MonadThrow
import cats.effect._
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.certificates
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.{Error, Info}
import io.renku.testtools.IOSpec
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import javax.net.ssl.SSLContext
import scala.util.{Failure, Try}

class CertificateLoaderSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "run" should {

    "find the certificate, " +
      "load the certificate to the keystore, " +
      "create SslContext, " +
      "make it default in the JVM and " +
      "log some info messagge" in new TestCase {

        val certificate = certificates.generateOne
        findCertificate.expects().returning(certificate.some.pure[IO])

        (keystore.load(_: Certificate)).expects(certificate).returning(().pure[IO])

        val sslContext = sslContexts.generateOne
        createSslContext.expects(keystore).returning(sslContext.pure[IO])

        makeSslContextDefault.expects(sslContext, *).returning(().pure[IO])

        certificateLoader.run.unsafeRunSync() shouldBe ()

        logger.loggedOnly(Info("Client certificate added"))
      }

    "log a relevant info when no client certificate is found" in new TestCase {

      findCertificate.expects().returning(None.pure[IO])

      certificateLoader.run.unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("No client certificate found"))
    }

    "fail and log an error if finding a certificate fails" in new TestCase {

      val exception = exceptions.generateOne
      findCertificate.expects().returning(exception.raiseError[IO, Option[Certificate]])

      Try(certificateLoader.run.unsafeRunSync()) shouldBe Failure(exception)

      logger.loggedOnly(Error("Loading client certificate failed", exception))
    }

    "fail and log an error if loading a certificate to the keystore fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(certificate.some.pure[IO])

      val exception = exceptions.generateOne
      (keystore.load(_: Certificate)).expects(certificate).returning(exception.raiseError[IO, Unit])

      Try(certificateLoader.run.unsafeRunSync()) shouldBe Failure(exception)

      logger.loggedOnly(Error("Loading client certificate failed", exception))
    }

    "fail and log an error if creating an SslContext fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(certificate.some.pure[IO])

      (keystore.load(_: Certificate)).expects(certificate).returning(().pure[IO])

      val exception = exceptions.generateOne
      createSslContext.expects(keystore).returning(exception.raiseError[IO, SslContext])

      Try(certificateLoader.run.unsafeRunSync()) shouldBe Failure(exception)

      logger.loggedOnly(Error("Loading client certificate failed", exception))
    }

    "fail and log an error if making the created SslContext default fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(certificate.some.pure[IO])

      (keystore.load(_: Certificate)).expects(certificate).returning(().pure[IO])

      val sslContext = sslContexts.generateOne
      createSslContext.expects(keystore).returning(sslContext.pure[IO])

      val exception = exceptions.generateOne
      makeSslContextDefault.expects(sslContext, *).returning(exception.raiseError[IO, Unit])

      Try(certificateLoader.run.unsafeRunSync()) shouldBe Failure(exception)

      logger.loggedOnly(Error("Loading client certificate failed", exception))
    }
  }

  private trait TestCase {
    val keystore              = mock[Keystore[IO]]
    val findCertificate       = mockFunction[IO[Option[Certificate]]]
    val createSslContext      = mockFunction[Keystore[IO], IO[SslContext]]
    val makeSslContextDefault = mockFunction[SslContext, MonadThrow[IO], IO[Unit]]
    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val certificateLoader =
      new CertificateLoaderImpl[IO](keystore, findCertificate, createSslContext, makeSslContextDefault)
  }

  private lazy val sslContexts: Gen[SslContext] = Gen.uuid.map(_ => new SslContext(SSLContext.getInstance("TLS")))
}
