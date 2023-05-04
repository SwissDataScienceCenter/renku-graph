/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

import cats.data.EitherT
import cats.syntax.all._
import io.renku.entities.searchgraphs.datasets.DatasetsGraphProvisioner
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError.LogWorthyRecoverableError
import io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.Generators.recoverableClientErrors
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class SearchGraphsProvisionerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "provisionSearchGraphs" should {

    "perform Datasets graph provisioning" in new TestCase {

      givenDatasetsGraphProvisioning(project, returning = ().pure[Try])

      provisioner.provisionSearchGraphs(project) shouldBe EitherT.rightT[Try, ProcessingRecoverableError](())
    }

    "fail with RecoverableFailure if provisioning fails with a Recoverable Failure" in new TestCase {

      val exception = recoverableClientErrors.generateOne
      givenDatasetsGraphProvisioning(project, returning = exception.raiseError[Try, Unit])

      val result = provisioner.provisionSearchGraphs(project).value

      result                         shouldBe a[Success[_]]
      result.fold(throw _, identity) shouldBe a[Left[_, _]]
      val Left(error) = result.fold(throw _, identity)
      error          shouldBe a[LogWorthyRecoverableError]
      error.getMessage should startWith("Problem while provisioning Search Graphs")
    }

    "fail with NonRecoverableFailure if provisioning fails with an unknown exception" in new TestCase {

      val exception = exceptions.generateOne
      givenDatasetsGraphProvisioning(project, returning = exception.raiseError[Try, Unit])

      provisioner.provisionSearchGraphs(project).value shouldBe exception.raiseError[Try, Unit]
    }
  }

  private trait TestCase {

    val project = anyProjectEntities.generateOne.to[entities.Project]

    private val datasetsGraphProvisioner = mock[DatasetsGraphProvisioner[Try]]
    val provisioner                      = new SearchGraphsProvisionerImpl[Try](datasetsGraphProvisioner)

    def givenDatasetsGraphProvisioning(project: entities.Project, returning: Try[Unit]) =
      (datasetsGraphProvisioner.provisionDatasetsGraph _)
        .expects(project)
        .returning(returning)
  }
}
