/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.config.certificates

import cats.MonadError
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.certificates
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import javax.net.ssl.SSLContext
import scala.util.{Failure, Try}

class CertificateLoaderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    "find the certificate, " +
      "load the certificate to the keystore, " +
      "create SslContext, " +
      "make it default in the JVM and " +
      "log some info messagge" in new TestCase {

        val certificate = certificates.generateOne
        findCertificate.expects().returning(certificate.some.pure[Try])

        (keystore.load(_: Certificate)).expects(certificate).returning(().pure[Try])

        val sslContext = sslContexts.generateOne
        createSslContext.expects(keystore).returning(sslContext.pure[Try])

        makeSslContextDefault.expects(sslContext, *).returning(().pure[Try])

        certificateLoader.run() shouldBe ().pure[Try]

        logger.loggedOnly(Info("Client certificate added"))
      }

    "log a relevant info when no client certificate is found" in new TestCase {

      findCertificate.expects().returning(None.pure[Try])

      certificateLoader.run() shouldBe ().pure[Try]

      logger.loggedOnly(Info("No client certificate found"))
    }

    "fail and log an error if finding a certificate fails" in new TestCase {

      val exception = exceptions.generateOne
      findCertificate.expects().returning(exception.raiseError[Try, Option[Certificate]])

      certificateLoader.run() shouldBe Failure(exception)

      logger.loggedOnly(Error("Loading client certificate failed", exception))
    }

    "fail and log an error if loading a certificate to the keystore fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(certificate.some.pure[Try])

      val exception = exceptions.generateOne
      (keystore.load(_: Certificate)).expects(certificate).returning(exception.raiseError[Try, Unit])

      certificateLoader.run() shouldBe Failure(exception)

      logger.loggedOnly(Error("Loading client certificate failed", exception))
    }

    "fail and log an error if creating an SslContext fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(certificate.some.pure[Try])

      (keystore.load(_: Certificate)).expects(certificate).returning(().pure[Try])

      val exception = exceptions.generateOne
      createSslContext.expects(keystore).returning(exception.raiseError[Try, SslContext])

      certificateLoader.run() shouldBe Failure(exception)

      logger.loggedOnly(Error("Loading client certificate failed", exception))
    }

    "fail and log an error if making the created SslContext default fails" in new TestCase {

      val certificate = certificates.generateOne
      findCertificate.expects().returning(certificate.some.pure[Try])

      (keystore.load(_: Certificate)).expects(certificate).returning(().pure[Try])

      val sslContext = sslContexts.generateOne
      createSslContext.expects(keystore).returning(sslContext.pure[Try])

      val exception = exceptions.generateOne
      makeSslContextDefault.expects(sslContext, *).returning(exception.raiseError[Try, Unit])

      certificateLoader.run() shouldBe Failure(exception)

      logger.loggedOnly(Error("Loading client certificate failed", exception))
    }
  }

  private trait TestCase {
    val keystore              = mock[Keystore[Try]]
    val findCertificate       = mockFunction[Try[Option[Certificate]]]
    val createSslContext      = mockFunction[Keystore[Try], Try[SslContext]]
    val makeSslContextDefault = mockFunction[SslContext, MonadError[Try, Throwable], Try[Unit]]
    val logger                = TestLogger[Try]()
    val certificateLoader = new CertificateLoaderImpl[Try](
      keystore,
      findCertificate,
      createSslContext,
      makeSslContextDefault,
      logger
    )
  }

  private lazy val sslContexts: Gen[SslContext] = Gen.uuid.map(_ => new SslContext(SSLContext.getInstance("TLS")))
}
