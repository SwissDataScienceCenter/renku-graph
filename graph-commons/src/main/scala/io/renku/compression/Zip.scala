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

package io.renku.compression

import cats.MonadThrow
import cats.effect.{Resource, Sync}
import cats.syntax.all._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.zip._
import scala.io.{Codec, Source}
import scala.util.control.NonFatal

trait Zip {
  def zip[F[_]:   Sync](content: String):      F[Array[Byte]]
  def unzip[F[_]: Sync](bytes:   Array[Byte]): F[String]
}

object Zip extends Zip {

  def zip[F[_]: Sync](content: String): F[Array[Byte]] = {
    val newStreams = MonadThrow[F].catchNonFatal {
      val arrOutputStream = new ByteArrayOutputStream(content.length)
      (arrOutputStream, new GZIPOutputStream(arrOutputStream))
    }

    val closeStreams: ((ByteArrayOutputStream, GZIPOutputStream)) => F[Unit] = { case (arrayOutputStream, _) =>
      Sync[F].delay(arrayOutputStream.close())
    }

    def zipContent(arrayOutputStream: ByteArrayOutputStream,
                   zipOutputStream:   GZIPOutputStream,
                   content:           String
    ): Array[Byte] = {
      zipOutputStream.write(content.getBytes(StandardCharsets.UTF_8))
      zipOutputStream.close()
      arrayOutputStream.toByteArray
    }

    Resource
      .make[F, (ByteArrayOutputStream, GZIPOutputStream)](newStreams)(closeStreams)
      .use { case (arrayOutputStream, zipOutputStream) =>
        MonadThrow[F].catchNonFatal(zipContent(arrayOutputStream, zipOutputStream, content))
      } recoverWith { case NonFatal(error) =>
      new Exception("Zipping content failed", error).raiseError[F, Array[Byte]]
    }
  }

  def unzip[F[_]: Sync](bytes: Array[Byte]): F[String] =
    Resource
      .make[F, GZIPInputStream] {
        MonadThrow[F].catchNonFatal(new GZIPInputStream(new ByteArrayInputStream(bytes)))
      }(stream => Sync[F].delay(stream.close()))
      .use { inputStream =>
        MonadThrow[F].catchNonFatal(Source.fromInputStream(inputStream)(Codec.UTF8).mkString)
      } recoverWith { case NonFatal(error) =>
      new Exception("Unzipping content failed", error).raiseError[F, String]
    }
}
