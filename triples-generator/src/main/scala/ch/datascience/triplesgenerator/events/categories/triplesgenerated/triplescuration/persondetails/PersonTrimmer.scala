package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.MonadError
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.graph.model.events.{CommitId, EventId}
import ch.datascience.graph.model.users.{Email, Name, ResourceId}
import ch.datascience.graph.model.{events, projects}
import ch.datascience.rdfstore.JsonLDTriples

private trait PersonTrimmer[Interpretation[_]] {
  def getTriplesAndTrimmedPersons(triples:     JsonLDTriples,
                                  projectPath: projects.Path,
                                  eventId:     EventId
  ): Interpretation[(JsonLDTriples, Set[Person])]

}

private class PersonTrimmerImpl[Interpretation[_]: MonadError[*[_], Throwable]](
    personExtractor:       PersonExtractor,
    commitCommitterFinder: CommitCommitterFinder[Interpretation]
) extends PersonTrimmer[Interpretation] {

  override def getTriplesAndTrimmedPersons(triples:     JsonLDTriples,
                                           projectPath: projects.Path,
                                           eventId:     EventId
  ): Interpretation[(JsonLDTriples, Set[Person])] = for {
    updatedAndPersonData <- personExtractor.extractPersons(triples).pure[Interpretation]
    (updatedTriples, personsRawData) = updatedAndPersonData
    personsWithMaybeEmail <- checkForMultipleEmails(personsRawData)
    persons               <- trimPersons(personsWithMaybeEmail, projectPath, CommitId(eventId.value))
  } yield (updatedTriples, persons)

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

  private def checkForMultipleEmails(personsRawData: Set[PersonRawData]): Interpretation[Set[RawDataMaybeEmail]] =
    personsRawData
      .find(_.emails.size > 1)
      .map { person =>
        new Exception(s"Multiple emails for person with '${person.id}' id found in generated JSON-LD")
          .raiseError[Interpretation, Set[RawDataMaybeEmail]]
      }
      .getOrElse(personsRawData.toMaybeEmail.pure[Interpretation])

  private def trimPersons(personsRawData: Set[RawDataMaybeEmail],
                          projectPath:    projects.Path,
                          commitId:       events.CommitId
  ): Interpretation[Set[Person]] = {
    val (personsDataWithNoEmail, personsDataWithSingleEmail)     = partitionPersonsWithSingleEmail(personsRawData)
    val (personsFromSingleEmail, withSingleEmailToCheckInGitlab) = personsDataWithSingleEmail.toPersonOrToGitlab
    val personsWithNoEmail                                       = personsDataWithNoEmail.toPersonsOrThrow
    if (withSingleEmailToCheckInGitlab.nonEmpty) {
      for {
        commitPersonInfo <- commitCommitterFinder.findCommitPeople(projectPath, commitId)
        persons          <- mergeRawDataWithGilabData(withSingleEmailToCheckInGitlab, commitPersonInfo)
        noEmails         <- personsWithNoEmail
      } yield persons ++ noEmails ++ personsFromSingleEmail
    } else {
      personsWithNoEmail.map(_ ++ personsFromSingleEmail)
    }
  }

  private def mergeRawDataWithGilabData(personsRawData:   Set[RawDataDisregardNameSingleEmail],
                                        commitPersonInfo: CommitPersonInfo
  ): Interpretation[Set[Person]] =
    personsRawData
      .map { case disregardNameSingleEmail =>
        commitPersonInfo.committers
          .find(_.email == disregardNameSingleEmail.email)
          .map(committer =>
            Person(disregardNameSingleEmail.id, None, committer.name, committer.email.some).pure[Interpretation]
          )
          .getOrElse(
            new Exception(s"Could not find the email for person with id '${disregardNameSingleEmail.id}' in gitlab")
              .raiseError[Interpretation, Person]
          )
      }
      .toList
      .sequence
      .map(_.toSet)

  private def partitionPersonsWithSingleEmail(
      personsRawData: Set[RawDataMaybeEmail]
  ): (Set[RawDataNoEmail], Set[RawDataSingleEmail]) =
    personsRawData.foldLeft((Set.empty[RawDataNoEmail], Set.empty[RawDataSingleEmail])) {
      case ((rawDataNoEmail, rawDataWithSingleEmail), RawDataMaybeEmail(id, names, Some(email))) =>
        rawDataNoEmail -> (rawDataWithSingleEmail + RawDataSingleEmail(id, names, email))
      case ((rawDataNoEmail, rawDataWithSingleEmail), RawDataMaybeEmail(id, names, None)) =>
        (rawDataNoEmail + RawDataNoEmail(id, names)) -> rawDataWithSingleEmail
    }

  private implicit class PersonsRawDataOps(persons: Set[PersonRawData]) {
    lazy val toMaybeEmail =
      persons.map(rawPerson => RawDataMaybeEmail(rawPerson.id, rawPerson.names, rawPerson.emails.headOption))
  }

  private implicit class PersonsRawDataSingleEmailOps(persons: Set[RawDataSingleEmail]) {

    lazy val toPersonOrToGitlab = persons.foldLeft((Set.empty[Person], Set.empty[RawDataDisregardNameSingleEmail])) {
      case ((persons, personsWithMultipleNames), RawDataSingleEmail(id, name :: Nil, email)) =>
        (persons + Person(id, None, name, email.some)) -> personsWithMultipleNames
      case ((persons, personsWithMultipleNames), RawDataSingleEmail(id, _, email)) =>
        persons -> (personsWithMultipleNames + RawDataDisregardNameSingleEmail(id, email))
    }
  }

  private implicit class PersonsRawDataWithNoEmailOps(persons: Set[RawDataNoEmail]) {
    lazy val toPersonsOrThrow = persons
      .map {
        case RawDataNoEmail(id, Nil) =>
          new Exception(s"No email and no name for person with id '$id' found in generated JSON-LD")
            .raiseError[Interpretation, Person]
        case RawDataNoEmail(id, name :: Nil) => Person(id, None, name, None).pure[Interpretation]
        case RawDataNoEmail(id, _) =>
          new Exception(s"No email for person with id '$id' and multiple names found in generated JSON-LD")
            .raiseError[Interpretation, Person]
      }
      .toList
      .sequence
      .map(_.toSet)
  }

}

private final case class RawDataMaybeEmail(id: ResourceId, names: List[Name], maybeEmail: Option[Email])
private final case class RawDataDisregardNameSingleEmail(id: ResourceId, email: Email)
private final case class RawDataSingleEmail(id: ResourceId, names: List[Name], email: Email)
private final case class RawDataNoEmail(id: ResourceId, names: List[Name])

private object IOPersonTrimmer {
  def apply(): PersonTrimmer[IO] =
    new PersonTrimmerImpl[IO](new PersonExtractorImpl(), IOCommitCommitterFinder())
}
