package ch.datascience.commiteventservice.commitsync

import cats.MonadError
import cats.data.OptionT
import cats.effect.{ContextShift, IO}
import ch.datascience.generators.CommonGraphGenerators.accessTokens
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.graph.tokenrepository.{AccessTokenFinder, IOAccessTokenFinder}
import cats.effect.IO
import ch.datascience.commiteventservice.commits.{CommitInfo, LatestCommitFinder}
import ch.datascience.commiteventservice.commitsync.LatestEventsFetcher.LatestProjectCommit
import ch.datascience.commiteventservice.eventprocessing.{Project, StartCommit}
import ch.datascience.commiteventservice.eventprocessing.startcommit.CommitToEventLog
import ch.datascience.commiteventservice.project.ProjectInfoFinder
import org.scalamock.scalatest.MockFactory
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should
import ch.datascience.graph.tokenrepository.IOAccessTokenFinder
import eu.timepit.refined.auto._
import org.scalacheck.Gen
import ch.datascience.http.client.AccessToken
import ch.datascience.interpreters.TestLogger
import ch.datascience.interpreters.TestLogger.Level.{Error, Info, Warn}
import ch.datascience.logging.TestExecutionTimeRecorder

class CommitSyncEndpointSpec extends AnyWordSpec with MockFactory with should.Matchers {}
