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
import org.typelevel.log4cats.Logger

trait CertificateLoader[F[_]] {
  def run(): F[Unit]
}

object CertificateLoader {

  import cats.syntax.all._

  def apply[F[_]: MonadThrow: Logger]: F[CertificateLoader[F]] = for {
    keystore <- Keystore[F]()
  } yield new CertificateLoaderImpl[F](
    keystore,
    findCertificate = () => Certificate.fromConfig[F](),
    createSslContext = (keystore: Keystore[F]) => SslContext.from(keystore)
  )
}

class CertificateLoaderImpl[F[_]: MonadThrow: Logger] private[certificates] (
    keystore:         Keystore[F],
    findCertificate:  () => F[Option[Certificate]],
    createSslContext: Keystore[F] => F[SslContext],
    makeSslContextDefault: (SslContext, MonadThrow[F]) => F[Unit] = (context: SslContext, MT: MonadThrow[F]) =>
      SslContext.makeDefault[F](context)(MT)
) extends CertificateLoader[F] {

  import cats.syntax.all._

  import scala.util.control.NonFatal

  override def run(): F[Unit] = {
    for {
      maybeCertificate <- findCertificate()
      _                <- maybeCertificate map addCertificate getOrElse Logger[F].info("No client certificate found")
    } yield ()
  } recoverWith loggingError

  private def addCertificate(certificate: Certificate): F[Unit] = for {
    _          <- keystore load certificate
    sslContext <- createSslContext(keystore)
    _          <- makeSslContextDefault(sslContext, MonadThrow[F])
    _          <- Logger[F].info("Client certificate added")
  } yield ()

  private lazy val loggingError: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)("Loading client certificate failed")
    exception.raiseError[F, Unit]
  }
}
