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

package ch.datascience.config.certificates

import java.security.KeyStore

import cats.MonadError

private trait Keystore[Interpretation[_]] {
  def load(certificate: Certificate): Interpretation[Unit]
  def toJavaKeyStore: KeyStore
}

private object Keystore {

  val CertificateAlias: String = "RenkuClientCertificate"

  def apply[Interpretation[_]]()(implicit
      ME: MonadError[Interpretation, Throwable]
  ): Interpretation[Keystore[Interpretation]] =
    ME.catchNonFatal(new KeystoreImpl[Interpretation]())
}

private class KeystoreImpl[Interpretation[_]](implicit ME: MonadError[Interpretation, Throwable])
    extends Keystore[Interpretation] {

  import java.io.ByteArrayInputStream
  import java.security.KeyStore
  import java.security.cert.CertificateFactory

  import Keystore.CertificateAlias

  private val keystore = KeyStore.getInstance("JKS")

  override def load(certificate: Certificate): Interpretation[Unit] = ME.catchNonFatal {
    val certificateFactory = CertificateFactory.getInstance("X.509");
    val certAsInputStream  = new ByteArrayInputStream(certificate.value.getBytes())
    val x509Certificate    = certificateFactory.generateCertificate(certAsInputStream);
    keystore.load(null, null);
    keystore.setCertificateEntry(CertificateAlias, x509Certificate);
  }

  override def toJavaKeyStore: KeyStore = keystore
}
