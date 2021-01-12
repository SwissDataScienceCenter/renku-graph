package ch.datascience.triplesgenerator.events

import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.triplesgenerator.events.EventHandler.CategoryName
import org.http4s.Request

private trait EventHandler[Interpretation[_]] {
  def handle(request: Request[Interpretation]): Interpretation[EventSchedulingResult]

  def name: CategoryName
}

private object EventHandler {

  final class CategoryName private (val value: String) extends AnyVal with StringTinyType
  implicit object CategoryName extends TinyTypeFactory[CategoryName](new CategoryName(_)) with NonBlank
}
