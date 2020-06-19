package ch.datascience.rdfstore.entities

import ch.datascience.tinytypes.constraints.{NonNegativeInt, PathSegment}
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}

final class Step private (val value: Int) extends AnyVal with IntTinyType
object Step extends TinyTypeFactory[Step](new Step(_)) with NonNegativeInt {

  implicit val toPathSegments: Step => List[PathSegment] = step =>
    List(
      PathSegment("steps"),
      PathSegment(s"step_$step")
    )
}
