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

trait CertificateLoader[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

object CertificateLoader {

  import cats.syntax.all._

  def apply[Interpretation[_]: MonadThrow: Logger]: Interpretation[CertificateLoader[Interpretation]] = for {
    keystore <- Keystore[Interpretation]()
  } yield new CertificateLoaderImpl[Interpretation](
    keystore,
    findCertificate = () => Certificate.fromConfig[Interpretation](),
    createSslContext = (keystore: Keystore[Interpretation]) => SslContext.from(keystore)
  )
}

class CertificateLoaderImpl[Interpretation[_]: MonadThrow: Logger] private[certificates] (
    keystore:         Keystore[Interpretation],
    findCertificate:  () => Interpretation[Option[Certificate]],
    createSslContext: Keystore[Interpretation] => Interpretation[SslContext],
    makeSslContextDefault: (SslContext, MonadThrow[Interpretation]) => Interpretation[Unit] =
      (context: SslContext, MT: MonadThrow[Interpretation]) => SslContext.makeDefault[Interpretation](context)(MT)
) extends CertificateLoader[Interpretation] {

  import cats.syntax.all._

  import scala.util.control.NonFatal

  override def run(): Interpretation[Unit] = {
    for {
      maybeCertificate <- findCertificate()
      _ <- maybeCertificate map addCertificate getOrElse Logger[Interpretation].info("No client certificate found")
    } yield ()
  } recoverWith loggingError

  private def addCertificate(certificate: Certificate): Interpretation[Unit] = for {
    _          <- keystore load certificate
    sslContext <- createSslContext(keystore)
    _          <- makeSslContextDefault(sslContext, MonadThrow[Interpretation])
    _          <- Logger[Interpretation].info("Client certificate added")
  } yield ()

  private lazy val loggingError: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    Logger[Interpretation].error(exception)("Loading client certificate failed")
    exception.raiseError[Interpretation, Unit]
  }
}
