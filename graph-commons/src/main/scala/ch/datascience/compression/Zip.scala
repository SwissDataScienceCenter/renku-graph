package ch.datascience.compression

import cats.MonadThrow
import cats.effect.{BracketThrow, Resource, Sync}
import cats.syntax.all._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.zip._
import scala.io.{Codec, Source}

sealed trait Zip {
  def zip[Interpretation[_]: BracketThrow: Sync](content: String): Interpretation[Array[Byte]]

  def unzip[Interpretation[_]: BracketThrow: Sync](bytes: Array[Byte]): Interpretation[String]
}

object Zip extends Zip {

  def zip[Interpretation[_]: BracketThrow: Sync](content: String): Interpretation[Array[Byte]] =
    Resource
      .make[Interpretation, (ByteArrayOutputStream, GZIPOutputStream)] {
        val arrOutputStream = new ByteArrayOutputStream(content.length)
        (arrOutputStream, new GZIPOutputStream(arrOutputStream)).pure[Interpretation]
      } { case (arrayOutputStream, _) =>
        Sync[Interpretation].delay(arrayOutputStream.close())
      }
      .use[Interpretation, Array[Byte]] { case (arrayOutputStream, zipOutputStream) =>
        MonadThrow[Interpretation].catchNonFatal {
          zipOutputStream.write(content.getBytes(StandardCharsets.UTF_8))
          zipOutputStream.close()
          arrayOutputStream.toByteArray
        }
      }

  def unzip[Interpretation[_]: BracketThrow: Sync](bytes: Array[Byte]): Interpretation[String] =
    Resource
      .make[Interpretation, GZIPInputStream] {
        new GZIPInputStream(new ByteArrayInputStream(bytes)).pure[Interpretation]
      }(stream => Sync[Interpretation].delay(stream.close()))
      .use { inputStream =>
        MonadThrow[Interpretation].catchNonFatal {
          Source.fromInputStream(inputStream)(Codec.UTF8).mkString
        }
      }
}
