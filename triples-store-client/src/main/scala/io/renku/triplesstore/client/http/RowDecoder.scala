package io.renku.triplesstore.client.http

import cats.syntax.all._
import io.circe.{Decoder, HCursor}

trait RowDecoder[A] extends Decoder[A]

object RowDecoder {

  def apply[A](implicit d: RowDecoder[A]): RowDecoder[A] = d

  def fromDecoder[A](d: Decoder[A]): RowDecoder[A] =
    (c: HCursor) => d.apply(c)

  private def prop[A: Decoder](name: String): Decoder[A] =
    Decoder.instance { cursor =>
      cursor.downField(name).downField("value").as[A]
    }

  def forProduct1[T, A0](name: String)(f: A0 => T)(implicit d: RowDecoder[A0]): RowDecoder[T] =
    fromDecoder(prop(name).map(f))

  def forProduct2[T, A0: Decoder, A1: Decoder](name0: String, name1: String)(f: (A0, A1) => T): RowDecoder[T] =
    fromDecoder((prop[A0](name0), prop[A1](name1)).mapN(f))

  def forProduct3[T, A0: Decoder, A1: Decoder, A2: Decoder](name0: String, name1: String, name2: String)(
      f: (A0, A1, A2) => T
  ): RowDecoder[T] =
    fromDecoder((prop[A0](name0), prop[A1](name1), prop[A2](name2)).mapN(f))
}
