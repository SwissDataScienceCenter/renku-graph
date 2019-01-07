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

import eu.timepit.refined.pureconfig._
import java.util.Base64

import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.webhookservice.crypto.AESCrypto.{ Message, Secret }
import eu.timepit.refined.W
import eu.timepit.refined.api.{ RefType, Refined }
import eu.timepit.refined.collection.MinSize
import eu.timepit.refined.string.MatchesRegex
import javax.crypto.Cipher
import javax.crypto.spec.{ IvParameterSpec, SecretKeySpec }
import javax.inject.{ Inject, Singleton }
import play.api.Configuration
import pureconfig._
import pureconfig.error.ConfigReaderException

import scala.language.{ higherKinds, implicitConversions }

class AESCrypto[Interpretation[_]]( secret: Secret )( implicit ME: MonadError[Interpretation, Throwable] ) {

  private lazy val base64Decoder = Base64.getDecoder
  private lazy val base64Encoder = Base64.getEncoder
  private lazy val algorithm = "AES/CBC/PKCS5Padding"
  private lazy val key = new SecretKeySpec( base64Decoder.decode( secret.value ), "AES" )
  private lazy val ivSpec = new IvParameterSpec( new Array[Byte]( 16 ) )
  private lazy val charset = "utf-8"

  def encrypt( message: String ): Interpretation[Message] = for {
    validatedMessage <- validate( message )
    cipher <- pure {
      val c = Cipher.getInstance( algorithm )
      c.init( Cipher.ENCRYPT_MODE, key, ivSpec )
      c
    }
    encoded <- pure {
      new String( base64Encoder.encode( cipher.doFinal( validatedMessage.value.getBytes( charset ) ) ), charset )
    }
    validatedDecoded <- validate( encoded )
  } yield validatedDecoded

  def decrypt( message: String ): Interpretation[Message] = for {
    validatedMessage <- validate( message )
    cipher <- pure {
      val c = Cipher.getInstance( algorithm )
      c.init( Cipher.DECRYPT_MODE, key, ivSpec )
      c
    }
    decoded <- pure {
      new String( cipher.doFinal( base64Decoder.decode( validatedMessage.value.getBytes( charset ) ) ), charset )
    }
    validatedDecoded <- validate( decoded )
  } yield validatedDecoded

  private def validate( string: String ): Interpretation[Message] =
    ME.fromEither[Message] {
      RefType
        .applyRef[Message]( string )
        .leftMap( _ => new IllegalArgumentException( "Message for encryption/decryption cannot be blank" ) )
    }

  private implicit def pure[T]( value: T ): Interpretation[T] =
    ME.pure( value )
}

object AESCrypto {
  type Secret = String Refined MinSize[W.`16`.T]
  type Message = String Refined MatchesRegex[W.`"""^(?!\\s*$).+"""`.T]
}

@Singleton
class IOAESCrypto( secret: Secret ) extends AESCrypto[IO]( secret ) {

  @Inject def this( configuration: Configuration ) = this(
    loadConfig[Secret]( configuration.underlying, "services.gitlab.secret-token-secret" ).fold(
      failures => throw new ConfigReaderException( failures ),
      identity
    )
  )
}
