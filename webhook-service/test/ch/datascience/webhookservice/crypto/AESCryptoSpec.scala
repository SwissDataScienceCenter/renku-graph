/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.crypto

import java.util.Base64

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.webhookservice.crypto.AESCrypto.Secret
import eu.timepit.refined.api.RefType
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.{ Failure, Success, Try }

class AESCryptoSpec extends WordSpec {

  "encrypt/decrypt" should {

    "encrypt and decrypt a given message" in new TestCase {
      val message: String = nonEmptyStrings().generateOne

      val Success( crypted ) = aesCrypto.encrypt( message )
      crypted.value should not be message

      val Success( decrypted ) = aesCrypto.decrypt( crypted.value )
      decrypted.value shouldBe message
    }

    "fail for blank messages" in new TestCase {
      val Failure( encryptException ) = aesCrypto.encrypt( " " )
      encryptException shouldBe an[IllegalArgumentException]
      encryptException.getMessage shouldBe "Message for encryption/decryption cannot be blank"

      val Failure( decryptException ) = aesCrypto.decrypt( " " )
      decryptException shouldBe an[IllegalArgumentException]
      decryptException.getMessage shouldBe "Message for encryption/decryption cannot be blank"
    }

    "fail if cannot be decrypted" in new TestCase {
      val Failure( decryptException ) = aesCrypto.decrypt( "sdfsf" )
      decryptException shouldBe an[IllegalArgumentException]
      decryptException.getMessage should not be "Message for encryption/decryption cannot be blank"
    }
  }

  private trait TestCase {

    import cats.implicits._

    private val secret = new String( Base64.getEncoder.encode( "1234567890123456".getBytes( "utf-8" ) ), "utf-8" )
    val aesCrypto = new AESCrypto[Try](
      RefType.applyRef[Secret]( secret )
        .getOrElse( throw new IllegalArgumentException( "Wrong secret" ) )
    )
  }
}
