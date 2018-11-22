/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.queue

import ammonite.ops.root
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.webhookservice.generators.ServiceTypesGenerators._
import ch.datascience.webhookservice.{ CheckoutSha, GitRepositoryUrl }
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.concurrent.{ IntegrationPatience, ScalaFutures }
import os.Path

import scala.concurrent.ExecutionContext.Implicits.global

class TriplesFinderSpec extends WordSpec with MockFactory with ScalaFutures with IntegrationPatience {

  "generateTriples" should {

    "create a temp directory, " +
      "clone the repo into it, " +
      "check out the commit, " +
      "call 'renku log' and saves the result in a file and " +
      "removes the temp directory" in new TestCase {
        ( file.mkdir( _: Path ) )
          .expects( repositoryDirectory )

        ( git.cloneRepo( _: GitRepositoryUrl, _: Path, _: Path ) )
          .expects( gitRepositoryUrl, repositoryDirectory, workDirectory )

        ( git.checkout( _: CheckoutSha, _: Path ) )
          .expects( checkoutSha, repositoryDirectory )

        ( renku.logToFile( _: TriplesFile, _: Path ) )
          .expects( triplesFile, repositoryDirectory )

        ( file.removeSilently( _: Path ) )
          .expects( repositoryDirectory )

        triplesFinder.generateTriples( gitRepositoryUrl, checkoutSha ).futureValue shouldBe Right( triplesFile )
      }

    "return an error if create a temp directory fails" in new TestCase {
      val exception = new Exception( "message" )
      ( file.mkdir( _: Path ) )
        .expects( repositoryDirectory )
        .throwing( exception )

      ( file.removeSilently( _: Path ) )
        .expects( repositoryDirectory )

      triplesFinder.generateTriples( gitRepositoryUrl, checkoutSha ).futureValue shouldBe Left( exception )
    }

    "return an error if cloning the repo fails" in new TestCase {
      ( file.mkdir( _: Path ) )
        .expects( repositoryDirectory )

      val exception = new Exception( "message" )
      ( git.cloneRepo( _: GitRepositoryUrl, _: Path, _: Path ) )
        .expects( gitRepositoryUrl, repositoryDirectory, workDirectory )
        .throwing( exception )

      ( file.removeSilently( _: Path ) )
        .expects( repositoryDirectory )

      triplesFinder.generateTriples( gitRepositoryUrl, checkoutSha ).futureValue shouldBe Left( exception )
    }

    "return an error if checking out the sha fails" in new TestCase {
      ( file.mkdir( _: Path ) )
        .expects( repositoryDirectory )

      ( git.cloneRepo( _: GitRepositoryUrl, _: Path, _: Path ) )
        .expects( gitRepositoryUrl, repositoryDirectory, workDirectory )

      val exception = new Exception( "message" )
      ( git.checkout( _: CheckoutSha, _: Path ) )
        .expects( checkoutSha, repositoryDirectory )
        .throwing( exception )

      ( file.removeSilently( _: Path ) )
        .expects( repositoryDirectory )

      triplesFinder.generateTriples( gitRepositoryUrl, checkoutSha ).futureValue shouldBe Left( exception )
    }

    "return an error if calling 'renku log' fails" in new TestCase {
      ( file.mkdir( _: Path ) )
        .expects( repositoryDirectory )

      ( git.cloneRepo( _: GitRepositoryUrl, _: Path, _: Path ) )
        .expects( gitRepositoryUrl, repositoryDirectory, workDirectory )

      ( git.checkout( _: CheckoutSha, _: Path ) )
        .expects( checkoutSha, repositoryDirectory )

      val exception = new Exception( "message" )
      ( renku.logToFile( _: TriplesFile, _: Path ) )
        .expects( triplesFile, repositoryDirectory )
        .throwing( exception )

      ( file.removeSilently( _: Path ) )
        .expects( repositoryDirectory )

      triplesFinder.generateTriples( gitRepositoryUrl, checkoutSha ).futureValue shouldBe Left( exception )
    }

    "return an error if removing the temp folder fails" in new TestCase {
      ( file.mkdir( _: Path ) )
        .expects( repositoryDirectory )

      ( git.cloneRepo( _: GitRepositoryUrl, _: Path, _: Path ) )
        .expects( gitRepositoryUrl, repositoryDirectory, workDirectory )

      ( git.checkout( _: CheckoutSha, _: Path ) )
        .expects( checkoutSha, repositoryDirectory )

      ( renku.logToFile( _: TriplesFile, _: Path ) )
        .expects( triplesFile, repositoryDirectory )

      val exception = new Exception( "message" )
      ( file.removeSilently( _: Path ) )
        .expects( repositoryDirectory )
        .throwing( exception )

      ( file.removeSilently( _: Path ) )
        .expects( repositoryDirectory )

      triplesFinder.generateTriples( gitRepositoryUrl, checkoutSha ).futureValue shouldBe Left( exception )
    }
  }

  private trait TestCase {
    val repositoryName = nonEmptyStrings().generateOne
    val gitRepositoryUrl = GitRepositoryUrl( s"http://host/$repositoryName.git" )
    val checkoutSha = checkoutShas.generateOne
    val pathDifferentiator = Gen.choose( 1, 100 ).generateOne

    val workDirectory = root / "tmp"
    val repositoryDirectory = workDirectory / s"$repositoryName-$pathDifferentiator"
    val triplesFile = TriplesFile( ( workDirectory / s"$repositoryName-$pathDifferentiator.rdf" ).toNIO )

    val file = mock[Commands.File]
    val git = mock[Commands.Git]
    val renku = mock[Commands.Renku]
    val randomLong = mockFunction[Long]
    randomLong.expects().returning( pathDifferentiator )
    val triplesFinder = new TriplesFinder( file, git, renku, randomLong )
  }
}
