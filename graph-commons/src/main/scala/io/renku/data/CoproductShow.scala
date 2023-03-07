package io.renku.data

import cats.Show
import shapeless._

trait CoproductShow {

  implicit val cnilShow: Show[CNil] = Show.show(_ => "")

  implicit def coproductShow[H, T <: Coproduct](implicit
      hs: Lazy[Show[H]],
      ts: Show[T]
  ): Show[H :+: T] =
    Show.show {
      case Inl(h) => hs.value.show(h)
      case Inr(t) => ts.show(t)
    }

  implicit def toGeneric[A, Repr](implicit gen: Generic.Aux[A, Repr], rshow: Show[Repr]): Show[A] =
    Show.show[A] { a =>
      rshow.show(gen.to(a))
    }

}

object CoproductShow extends CoproductShow
