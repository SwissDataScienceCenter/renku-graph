package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import ch.datascience.graph.model.entities.Person

private trait KGPersonFinder[Interpretation[_]] {
  def find(person: Person): Interpretation[Option[Person]]
}
