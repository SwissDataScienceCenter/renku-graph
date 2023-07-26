package io.renku.projectauth

import cats.Order
import cats.data.NonEmptyList

sealed trait Role extends Ordered[Role] {
  def asString: String
}

object Role {

  case object Owner extends Role {
    val asString = "owner"

    override def compare(that: Role): Int =
      if (that == this) 0 else 1
  }
  case object Maintainer extends Role {
    val asString = "maintainer"

    override def compare(that: Role): Int =
      if (that == this) 0
      else if (that == Owner) -1
      else 1
  }
  case object Reader extends Role {
    val asString = "reader"

    override def compare(that: Role): Int =
      if (that == this) 0
      else -1
  }

  val all: NonEmptyList[Role] =
    NonEmptyList.of(Owner, Maintainer, Reader)

  def fromString(str: String): Either[String, Role] =
    all.find(_.asString.equalsIgnoreCase(str)).toRight(s"Invalid role name: $str")

  def unsafeFromString(str: String): Role =
    fromString(str).fold(sys.error, identity)

  implicit val ordering: Ordering[Role] =
    Ordering.by(r => -all.toList.indexOf(r))

  implicit val order: Order[Role] =
    Order.fromOrdering
}
