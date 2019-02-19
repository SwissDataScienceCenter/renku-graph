/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.triplesgenerator.queues.logevent

import akka.stream.Materializer
import akka.stream.OverflowStrategy.backpressure
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import ch.datascience.graph.events.{CommitId, ProjectPath}
import ch.datascience.triplesgenerator.eventprocessing.RDFTriples
import javax.inject.{Inject, Singleton}
import play.api.libs.json.{JsError, JsSuccess, JsValue}
import play.api.{Logger, LoggerLike}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

@Singleton
class LogEventQueue(
    queueConfig:             QueueConfig,
    logEventsSource:         Source[Try[JsValue], Future[Done]],
    triplesFinder:           TriplesFinder,
    fusekiConnector:         FusekiConnector,
    logger:                  LoggerLike
)(implicit executionContext: ExecutionContext, materializer: Materializer) {

  @Inject() def this(
      queueConfig:             QueueConfig,
      logEventsSourceProvider: EventLogSourceProvider,
      triplesFinder:           TriplesFinder,
      fusekiConnector:         FusekiConnector
  )(implicit executionContext: ExecutionContext, materializer: Materializer) = this(
    queueConfig,
    logEventsSourceProvider.get,
    triplesFinder,
    fusekiConnector,
    Logger
  )

  import LogEventQueue._
  import Commit._
  import queueConfig._

  logEventsSource
    .mapAsync(bufferSize.value) {
      case Failure(throwable) =>
        logger.error("Received broken notification from Event Log", throwable)
        Future.successful(Done)
      case Success(eventAsJson) =>
        logger.info(s"Received event from Event Log: $eventAsJson")
        queue.offer(eventAsJson)
    }
    .toMat(Sink.ignore)(Keep.none)
    .run()

  private lazy val queue = Source
    .queue[JsValue](bufferSize.value, overflowStrategy = backpressure)
    .map(toCommits)
    .flatMapConcat(logAndSkipJsonErrors)
    .mapAsync(triplesFinderThreads.value)(commitToRdfTriples)
    .flatMapConcat(logAndSkipTriplesErrors)
    .toMat(fusekiSink)(Keep.left)
    .run()

  private def toCommits(jsValue: JsValue): Either[Throwable, List[Commit]] =
    jsValue.validate[List[Commit]] match {
      case JsSuccess(commit, _) => Right(commit)
      case JsError(errors) =>
        Left(
          new RuntimeException(
            errors.foldLeft("Json deserialization error(s):") {
              case (message, (path, pathErrors)) =>
                s"$message\n $path -> ${pathErrors.map(_.message).mkString("; ")}"
            }
          )
        )
    }

  private lazy val logAndSkipJsonErrors: Either[Throwable, List[Commit]] => Source[Commit, NotUsed] = {
    case Left(exception) =>
      logger.error(s"Invalid event data received from Event Log: ${exception.getMessage}")
      Source.empty[Commit]
    case Right(commits) =>
      Source[Commit](commits)
  }

  private def commitToRdfTriples(commit: Commit): Future[(Commit, Either[Throwable, RDFTriples])] =
    for {
      maybeTriplesFile <- triplesFinder.generateTriples(commit)
    } yield commit -> maybeTriplesFile

  private lazy val logAndSkipTriplesErrors
    : ((Commit, Either[Throwable, RDFTriples])) => Source[(Commit, RDFTriples), NotUsed] = {
    case (commit, Left(exception)) =>
      logger.error(s"Generating triples for $commit failed", exception)
      Source.empty[(Commit, RDFTriples)]
    case (commit, Right(triples)) =>
      Source.single(commit -> triples)
  }

  private lazy val fusekiSink: Sink[(Commit, RDFTriples), Future[Done]] =
    Flow[(Commit, RDFTriples)]
      .mapAsync(fusekiUploadThreads.value) {
        case (commit, triplesFile) =>
          fusekiConnector
            .upload(triplesFile)
            .map(_ => logger.info(s"Triples for $commit uploaded to triples store"))
            .recover {
              case exception =>
                logger.error(s"Uploading triples for $commit failed", exception)
                Done
            }
      }
      .toMat(Sink.ignore)(Keep.right)
}

private object LogEventQueue {

  private[logevent] sealed trait Commit {
    val id:          CommitId
    val projectPath: ProjectPath
  }

  private[logevent] case class CommitWithParent(
      id:          CommitId,
      parentId:    CommitId,
      projectPath: ProjectPath
  ) extends Commit

  private[logevent] case class CommitWithoutParent(
      id:          CommitId,
      projectPath: ProjectPath
  ) extends Commit

  private[logevent] object Commit {

    import play.api.libs.functional.syntax._
    import play.api.libs.json.Reads._
    import play.api.libs.json._

    implicit val commitsReads: Reads[List[Commit]] = (
      (__ \ "id").read[CommitId] and
        (__ \ "project" \ "path").read[ProjectPath] and
        (__ \ "parents").read[Seq[CommitId]]
    )(toCommits _)

    private def toCommits(commitId: CommitId, projectPath: ProjectPath, parents: Seq[CommitId]) =
      parents match {
        case Nil =>
          List(CommitWithoutParent(commitId, projectPath))
        case someParents =>
          someParents
            .map(CommitWithParent(commitId, _, projectPath))
            .toList
      }
  }
}

trait EventLogSourceProvider {
  def get: Source[Try[JsValue], Future[Done]]
}
