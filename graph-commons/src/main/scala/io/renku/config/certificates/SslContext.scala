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

package io.renku.config.certificates

import cats.MonadThrow

import javax.net.ssl.SSLContext

private class SslContext private[certificates] (sslContext: SSLContext) {
  lazy val toJavaSSLContext: SSLContext = sslContext
}

private object SslContext {
  import javax.net.ssl.TrustManagerFactory

  def from[Interpretation[_]: MonadThrow](
      keystore: Keystore[Interpretation]
  ): Interpretation[SslContext] = MonadThrow[Interpretation].catchNonFatal {
    val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(keystore.toJavaKeyStore)

    val context = SSLContext.getInstance("TLS")
    context.init(null, trustManagerFactory.getTrustManagers, null)

    new SslContext(context)
  }

  def makeDefault[Interpretation[_]: MonadThrow](
      sslContext: SslContext
  ): Interpretation[Unit] = MonadThrow[Interpretation].catchNonFatal {
    SSLContext setDefault sslContext.toJavaSSLContext
  }
}
