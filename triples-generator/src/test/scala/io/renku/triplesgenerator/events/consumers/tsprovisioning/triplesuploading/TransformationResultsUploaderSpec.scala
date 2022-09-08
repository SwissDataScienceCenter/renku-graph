/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.triplesuploading

import cats.data.EitherT.rightT
import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.TSVersion
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TransformationResultsUploaderSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "apply" should {

    "return a facility allowing to find a TransformationResultsUploader relevant for the given TSVersion" in {
      implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]

      val uploader = TransformationResultsUploader[IO].unsafeRunSync()

      uploader[TSVersion.DefaultGraph].getClass shouldBe classOf[DefaultGraphResultsUploader[IO]]
    }
  }
}

class DefaultGraphResultsUploaderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "execute" should {

    "call the queryRunner's run with the given query" in new TestCase {
      val query = sparqlQueries.generateOne

      (queryRunner.run _).expects(query).returning(rightT(()))

      uploader.execute(query).value shouldBe ().asRight.pure[Try]
    }
  }

  "upload" should {

    "encode the given project to JsonLD, flatten it and pass to the projectUploader" in new TestCase {
      val project = anyProjectEntities.generateOne.to[entities.Project]

      val projectJsonLD = project.asJsonLD.flatten.fold(throw _, identity)

      (projectUploader.uploadProject _).expects(projectJsonLD).returning(rightT(()))

      uploader.upload(project).value shouldBe ().asRight.pure[Try]
    }
  }

  private trait TestCase {
    val projectUploader = mock[ProjectUploader[Try]]
    val queryRunner     = mock[UpdateQueryRunner[Try]]
    val uploader        = new DefaultGraphResultsUploader(projectUploader, queryRunner)
  }
}
