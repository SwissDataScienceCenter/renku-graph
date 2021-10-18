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

import cats.MonadError
import io.renku.config.certificates.Certificate

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path, Paths}

private trait CertificateSaver[Interpretation[_]] {
  def save(certificate: Certificate): Interpretation[Path]
}

private object CertificateSaver {
  def apply[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable]): CertificateSaver[Interpretation] =
    new CertificateSaverImpl[Interpretation]()
}

private class CertificateSaverImpl[Interpretation[_]](implicit ME: MonadError[Interpretation, Throwable])
    extends CertificateSaver[Interpretation] {

  private val certPathDirectory: Path = Paths.get(s"${System.getProperty("user.home")}${File.separator}/git-certs")
  private val certPath:          Path = certPathDirectory.resolve("cert.pem")

  override def save(certificate: Certificate): Interpretation[Path] = ME.catchNonFatal {

    Files.createDirectory(certPathDirectory)

    val certFile = Files.createFile(certPath).toFile

    val pw = new PrintWriter(certFile)

    try pw.write(certificate.toString)
    finally pw.close()

    certPath
  }
}
