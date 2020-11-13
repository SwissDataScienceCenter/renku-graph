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

import java.security.cert.CertificateException

import cats.syntax.all._
import ch.datascience.generators.Generators.nonBlankStrings
import ch.datascience.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

class KeystoreSpec extends AnyWordSpec with should.Matchers {

  "load" should {

    "generate X.509 certificate from the given Certificate, " +
      "clean the keystore and " +
      "add an entry with the Certificate associated with 'RenkuClientCertificate' alias" in {

        keystore.load(certificateExample) shouldBe ().pure[Try]

        keystore.toJavaKeyStore.aliases().asIterator().asScala.toList shouldBe List("renkuclientcertificate")

        Option(keystore.toJavaKeyStore.getCertificate("renkuclientcertificate")) shouldBe a[Some[_]]
      }

    "fail for an invalid certificate" in {
      val Failure(exception) = keystore.load(Certificate(nonBlankStrings().generateOne.value))
      exception shouldBe a[CertificateException]
    }
  }

  private lazy val keystore = new KeystoreImpl[Try]()

  private lazy val certificateExample = Certificate {
    """|-----BEGIN CERTIFICATE-----
       |MIICRDCCAa2gAwIBAgIBADANBgkqhkiG9w0BAQ0FADA/MQswCQYDVQQGEwJjaDER
       |MA8GA1UECAwITGF1c2FubmUxDTALBgNVBAoMBFNEU0MxDjAMBgNVBAMMBVJlbmt1
       |MB4XDTIwMTExMjE0MzY0MVoXDTIxMTExMjE0MzY0MVowPzELMAkGA1UEBhMCY2gx
       |ETAPBgNVBAgMCExhdXNhbm5lMQ0wCwYDVQQKDARTRFNDMQ4wDAYDVQQDDAVSZW5r
       |dTCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA08Ek059GLSosIgc8+YiL2WwB
       |W74jDg/vw14+CYItidouUdxAtl2oMB+6aAH3pX8qad7MgdsuPvgZnd61tWlS30g6
       |KsaGtgiQ2vglxdGpOm6dYEZ3yI9GI6bD5jWOVRaZAk6LHm8CGWEgCDb1ttvtL8iw
       |kxGGU+qnNrYYdUE/uGECAwEAAaNQME4wHQYDVR0OBBYEFJ6Go88n9DDApZ27v2bU
       |AAbpHcwXMB8GA1UdIwQYMBaAFJ6Go88n9DDApZ27v2bUAAbpHcwXMAwGA1UdEwQF
       |MAMBAf8wDQYJKoZIhvcNAQENBQADgYEAmqAxp/MYH1XdcbJkxre0sPAl2Sz4NPqv
       |YS+uZ9jTJ3JIZSOC38hoq+DRoCU3eR8lM4j1QS/Qe503XL/NgcKjNoh2lSyJ3VVG
       |W/T8OiVn0VlF7+dJwwfzFB2acXbk6EEDrZFftXol7D2G6b9kOLQHYTFOGUIq17zY
       |ZNtRDuH4XfQ=
       |-----END CERTIFICATE-----
       |""".stripMargin
  }
}
