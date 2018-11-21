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

package ch.datascience.webhookservice.generators

import java.nio.file.Paths

import ch.datascience.generators.Generators._
import ch.datascience.webhookservice._
import ch.datascience.webhookservice.queue.TriplesFile
import org.scalacheck.Gen

object ServiceTypesGenerators {

  val shas: Gen[String] = for {
    length <- Gen.choose( 5, 40 )
    chars <- Gen.listOfN( length, Gen.oneOf( ( 0 to 9 ).map( _.toString ) ++ ( 'a' to 'f' ).map( _.toString ) ) )
  } yield chars.mkString( "" )

  implicit val filePaths: Gen[FilePath] = relativePaths map FilePath

  implicit val checkoutShas: Gen[CheckoutSha] = shas map CheckoutSha

  implicit val gitRepositoryUrls: Gen[GitRepositoryUrl] =
    nonEmptyStrings()
      .map { repoName =>
        GitRepositoryUrl( s"http://host/$repoName.git" )
      }

  implicit val projectNames: Gen[ProjectName] = nonEmptyStrings() map ProjectName

  implicit val pushEvents: Gen[PushEvent] = for {
    sha <- checkoutShas
    repositoryUrl <- gitRepositoryUrls
    projectName <- projectNames
  } yield PushEvent( sha, repositoryUrl, projectName )

  implicit val triplesFiles: Gen[TriplesFile] =
    relativePaths
      .map( stringPath => Paths.get( stringPath ) )
      .map( TriplesFile )
}
