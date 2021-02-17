package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.effect.IO
import ch.datascience.graph.model.events.{CommitId, EventId}
import ch.datascience.rdfstore.JsonLDTriples

private trait PersonTrimmer[Interpretation[_]] {
  def getTriplesAndTrimmedPersons(triples: JsonLDTriples,
                                  eventId: EventId
  ): Interpretation[(JsonLDTriples, Set[Person])]

}

private class PersonTrimmerImpl[Interpretation[_]](personExtractor:       PersonExtractor,
                                                   commitCommitterFinder: CommitCommitterFinder[Interpretation]
) extends PersonTrimmer[Interpretation] {
  override def getTriplesAndTrimmedPersons(triples: JsonLDTriples,
                                           eventId: EventId
  ): Interpretation[(JsonLDTriples, Set[Person])] = ???

  // maybeEmail <- verify emails. if multiple, fail interpretation
  // verifyNames. if multiple, call gitlab to get single name for commit
  //                                commitInfoFinder (see other examples) will return a committer and author, both with name and email
  //                                try to match based on that response. it could be either but it should be committer
  //                                 if we can match, then we use it for the name in the person, else fail. failures defined in FailureOps
  //                               if there is no email but there are multiple names, we can't do anything....RAISE ERROR
  //

  //    for {
//      triplesAndPersons <- extractPersons(triples)
//      (updatedTriples, personDatas) = triplesAndPersons
//      commitId       <- commitIdExtractor.extractCommitId(updatedTriples)
//      trimmedPersons <- trimPersons(personDatas, commitId)
//    } yield (updatedTriples, trimmedPersons)

//  private def trimPersons(personDatas: Set[PersonData], commitId: CommitId): Interpretation[Set[Person]] =
//    personDatas
//      .map {
//        case (id, name :: Nil, emails) => Some(Person(id, name, emails.headOption))
//        case (_, name :: names, _)     => tryToGetPersonFromGitLab(commitId)
//        case _                         => ???
//      }
//      .flatten
//      .pure[Interpretation]
}

private object IOPersonTrimmer {
  def apply(): PersonTrimmer[IO] =
    new PersonTrimmerImpl[IO](new PersonExtractorImpl(), IOCommitCommitterFinder())
}
