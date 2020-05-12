package ch.datascience.tinytypes.ordering

import java.time.Instant

import ch.datascience.tinytypes.TinyType

object TinyTypeOrderings {

  implicit class TinyTypeOps[V](tinyType: TinyType { type V })(implicit val valueOrdering: Ordering[V]) {
    def compareTo(other: TinyType { type V }): Int =
      valueOrdering.compare(tinyType.value.asInstanceOf[V], other.value.asInstanceOf[V])
  }

  implicit val instantOrdering: Ordering[Instant] = (x: Instant, y: Instant) => x compareTo y
}
