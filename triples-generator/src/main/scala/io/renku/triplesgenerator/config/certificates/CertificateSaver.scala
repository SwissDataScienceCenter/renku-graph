/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.{MonadError, MonadThrow}
import io.renku.config.certificates.Certificate

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path, Paths}

private trait CertificateSaver[F[_]] {
  def save(certificate: Certificate): F[Path]
}

private object CertificateSaver {
  def apply[F[_]: MonadThrow]: CertificateSaver[F] = new CertificateSaverImpl[F]()
}

private class CertificateSaverImpl[F[_]](implicit ME: MonadError[F, Throwable]) extends CertificateSaver[F] {

  private val certPathDirectory: Path = Paths.get(s"${System.getProperty("user.home")}${File.separator}/git-certs")
  private val certPath:          Path = certPathDirectory.resolve("cert.pem")

  override def save(certificate: Certificate): F[Path] = ME.catchNonFatal {

    Files.createDirectory(certPathDirectory)

    val certFile = Files.createFile(certPath).toFile

    val pw = new PrintWriter(certFile)

    try pw.write(certificate.toString)
    finally pw.close()

    certPath
  }
}
