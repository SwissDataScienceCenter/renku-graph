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

package ch.datascience.compression

import cats.MonadThrow
import cats.effect.{BracketThrow, Resource, Sync}
import cats.syntax.all._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.zip._
import scala.io.{Codec, Source}
import scala.util.control.NonFatal

trait Zip {
  def zip[Interpretation[_]:   BracketThrow: Sync](content: String):      Interpretation[Array[Byte]]
  def unzip[Interpretation[_]: BracketThrow: Sync](bytes:   Array[Byte]): Interpretation[String]
}

object Zip extends Zip {

  def zip[Interpretation[_]: BracketThrow: Sync](content: String): Interpretation[Array[Byte]] = {
    val newStreams = MonadThrow[Interpretation].catchNonFatal {
      val arrOutputStream = new ByteArrayOutputStream(content.length)
      (arrOutputStream, new GZIPOutputStream(arrOutputStream))
    }

    val closeStreams: ((ByteArrayOutputStream, GZIPOutputStream)) => Interpretation[Unit] = {
      case (arrayOutputStream, _) =>
        Sync[Interpretation].delay(arrayOutputStream.close())
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
      .make[Interpretation, (ByteArrayOutputStream, GZIPOutputStream)](newStreams)(closeStreams)
      .use[Interpretation, Array[Byte]] { case (arrayOutputStream, zipOutputStream) =>
        MonadThrow[Interpretation].catchNonFatal(zipContent(arrayOutputStream, zipOutputStream, content))
      } recoverWith { case NonFatal(error) =>
      new Exception("Zipping content failed", error).raiseError[Interpretation, Array[Byte]]
    }
  }

  def unzip[Interpretation[_]: BracketThrow: Sync](bytes: Array[Byte]): Interpretation[String] =
    Resource
      .make[Interpretation, GZIPInputStream] {
        MonadThrow[Interpretation].catchNonFatal(new GZIPInputStream(new ByteArrayInputStream(bytes)))
      }(stream => Sync[Interpretation].delay(stream.close()))
      .use { inputStream =>
        MonadThrow[Interpretation].catchNonFatal(Source.fromInputStream(inputStream)(Codec.UTF8).mkString)
      } recoverWith { case NonFatal(error) =>
      new Exception("Unzipping content failed", error).raiseError[Interpretation, String]
    }
}
