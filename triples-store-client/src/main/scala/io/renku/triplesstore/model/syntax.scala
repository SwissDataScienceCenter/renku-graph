package io.renku.triplesstore.model

object syntax extends TripleObjectEncoder.implicits {

  implicit class Syntax[T](obj: T) {

    def asQuads(implicit enc: QuadsEncoder[T]): List[Quad] = enc(obj)

    def asTripleObject(implicit convert: TripleObjectEncoder[T]): TripleObject = convert(obj)
  }
}
