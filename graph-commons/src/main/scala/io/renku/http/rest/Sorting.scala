package io.renku.http.rest

import cats.Semigroup
import cats.data.NonEmptyList
import io.renku.triplesstore.client.model.OrderBy

/** Combines multiple [[SortBy.By]]s. */
final case class Sorting[S <: SortBy](sortBy: NonEmptyList[S#By]) {

  def toOrderBy(pm: S#PropertyType => OrderBy.Property): OrderBy =
    OrderBy(sortBy.map(e => OrderBy.Sort(pm(e.property), e.direction.toOrderByDirection)))

  lazy val asOrderBy: OrderBy = toOrderBy(p => OrderBy.Property(p.name))

  def :+(next: S#By): Sorting[S] =
    new Sorting(sortBy.append(next))

  def ++(next: Sorting[S]): Sorting[S] =
    new Sorting(this.sortBy.concatNel(next.sortBy))
}

object Sorting {
  def apply[E <: SortBy](e: E#By, more: E#By*): Sorting[E] =
    apply[E](NonEmptyList(e, more.toList))

  def apply[E <: SortBy](sorts: NonEmptyList[E#By]): Sorting[E] =
    new Sorting[E](sorts)

  def fromList[E <: SortBy](list: List[E#By]): Option[Sorting[E]] =
    NonEmptyList.fromList(list).map(Sorting(_))

  def fromListOrDefault[E <: SortBy](list: List[E#By], default: => Sorting[E]): Sorting[E] =
    fromList(list).getOrElse(default)

  implicit def sortingSemigroup[A <: SortBy]: Semigroup[Sorting[A]] =
    Semigroup.instance(_ ++ _)

}
