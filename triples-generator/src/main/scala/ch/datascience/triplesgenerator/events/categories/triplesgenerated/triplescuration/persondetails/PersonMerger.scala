package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import ch.datascience.graph.model.entities.Person

private trait PersonMerger {
  def merge(modelPerson: Person, kgPerson: Person): Person
}
