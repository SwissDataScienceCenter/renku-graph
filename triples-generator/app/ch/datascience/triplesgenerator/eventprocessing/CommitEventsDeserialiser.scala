package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.implicits._
import ch.datascience.graph.events.{CommitId, ProjectPath}
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import io.circe.parser._
import io.circe.{Decoder, DecodingFailure, Error, HCursor, ParsingFailure}

import scala.language.higherKinds

private class CommitEventsDeserialiser[Interpretation[_]](
    implicit ME: MonadError[Interpretation, Throwable]
) {

  def deserialiseToCommitEvents(jsonString: String): Interpretation[List[Commit]] = ME.fromEither {
    parse(jsonString).flatMap(_.as[List[Commit]]).leftMap(toMeaningfulError(jsonString))
  }

  private implicit val commitsDecoder: Decoder[List[Commit]] = (cursor: HCursor) =>
    for {
      commitId      <- cursor.downField("id").as[CommitId]
      projectPath   <- cursor.downField("project").downField("path").as[ProjectPath]
      parentCommits <- cursor.downField("parents").as[List[CommitId]]
    } yield
      parentCommits match {
        case Nil       => List(CommitWithoutParent(commitId, projectPath))
        case parentIds => parentIds map (CommitWithParent(commitId, _, projectPath))
    }

  private def toMeaningfulError(json: String): Error => Error = {
    case failure: DecodingFailure => failure.withMessage(s"CommitEvent cannot be deserialised: '$json'")
    case failure: ParsingFailure => {
      println(failure)
      ParsingFailure(s"CommitEvent cannot be deserialised: '$json'", failure)
    }
  }

}
