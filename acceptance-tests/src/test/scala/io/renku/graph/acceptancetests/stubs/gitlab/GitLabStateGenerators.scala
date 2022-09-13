package io.renku.graph.acceptancetests.stubs.gitlab

import io.renku.generators.Generators
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.CommitData
import io.renku.graph.model.GraphModelGenerators
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.testentities.Person
import org.scalacheck.Gen

import java.time.Instant

trait GitLabStateGenerators {
  def commitData(commitId: CommitId): Gen[CommitData] =
    for {
      authorName     <- GraphModelGenerators.personNames
      authorEmail    <- GraphModelGenerators.personEmails
      committerName  <- GraphModelGenerators.personNames
      committerEmail <- GraphModelGenerators.personEmails
      message        <- Generators.nonEmptyStrings()
    } yield CommitData(
      commitId,
      Person(authorName, authorEmail),
      Person(committerName, committerEmail),
      Instant.now(),
      message,
      Nil
    )
}

object GitLabStateGenerators extends GitLabStateGenerators
