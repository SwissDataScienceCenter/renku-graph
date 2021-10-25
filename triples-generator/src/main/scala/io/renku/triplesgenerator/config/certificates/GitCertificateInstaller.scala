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

package io.renku.triplesgenerator.config.certificates

import cats.MonadThrow
import io.renku.config.certificates.Certificate
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

trait GitCertificateInstaller[F[_]] {
  def run(): F[Unit]
}

object GitCertificateInstaller {
  def apply[F[_]: MonadThrow: Logger]: F[GitCertificateInstaller[F]] =
    MonadThrow[F].catchNonFatal {
      new GitCertificateInstallerImpl[F](
        () => Certificate.fromConfig[F](),
        CertificateSaver(),
        GitConfigModifier()
      )
    }
}

class GitCertificateInstallerImpl[F[_]: MonadThrow: Logger](
    findCertificate:   () => F[Option[Certificate]],
    certificateSaver:  CertificateSaver[F],
    gitConfigModifier: GitConfigModifier[F]
) extends GitCertificateInstaller[F] {

  import cats.syntax.all._

  def run(): F[Unit] = findCertificate() flatMap {
    case None => ().pure[F]
    case Some(certificate) =>
      for {
        certPath <- certificateSaver.save(certificate)
        _        <- gitConfigModifier.makeGitTrust(certPath)
        _        <- Logger[F] info "Certificate installed for Git"
      } yield ()
  } recoverWith logMessage

  private lazy val logMessage: PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F]
      .error(exception)("Certificate installation for Git failed")
      .flatMap(_ => exception.raiseError[F, Unit])
  }
}
