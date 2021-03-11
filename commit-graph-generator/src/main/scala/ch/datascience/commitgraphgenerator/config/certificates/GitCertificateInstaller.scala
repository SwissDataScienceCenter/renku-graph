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

package ch.datascience.commitgraphgenerator.config.certificates

import cats.MonadError
import ch.datascience.config.certificates.Certificate
import io.chrisdavenport.log4cats.Logger

import scala.util.control.NonFatal

trait GitCertificateInstaller[Interpretation[_]] {
  def run(): Interpretation[Unit]
}

object GitCertificateInstaller {
  def apply[Interpretation[_]](
      logger:    Logger[Interpretation]
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[GitCertificateInstaller[Interpretation]] =
    ME.catchNonFatal {
      new GitCertificateInstallerImpl[Interpretation](
        () => Certificate.fromConfig[Interpretation](),
        CertificateSaver(),
        GitConfigModifier(),
        logger
      )
    }
}

class GitCertificateInstallerImpl[Interpretation[_]](
    findCertificate:   () => Interpretation[Option[Certificate]],
    certificateSaver:  CertificateSaver[Interpretation],
    gitConfigModifier: GitConfigModifier[Interpretation],
    logger:            Logger[Interpretation]
)(implicit ME:         MonadError[Interpretation, Throwable])
    extends GitCertificateInstaller[Interpretation] {

  import cats.syntax.all._

  def run(): Interpretation[Unit] = findCertificate() flatMap {
    case None => ().pure[Interpretation]
    case Some(certificate) =>
      for {
        certPath <- certificateSaver.save(certificate)
        _        <- gitConfigModifier.makeGitTrust(certPath)
        _        <- logger info "Certificate installed for Git"
      } yield ()
  } recoverWith logMessage

  private lazy val logMessage: PartialFunction[Throwable, Interpretation[Unit]] = { case NonFatal(exception) =>
    logger
      .error(exception)("Certificate installation for Git failed")
      .flatMap(_ => exception.raiseError[Interpretation, Unit])
  }
}
