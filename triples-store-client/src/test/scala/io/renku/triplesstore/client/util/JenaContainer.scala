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

package io.renku.triplesstore.client.util

import com.dimafeng.testcontainers.{FixedHostPortGenericContainer, GenericContainer, SingleContainer}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import org.http4s.Uri
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers
import org.testcontainers.utility.DockerImageName

object JenaContainer {
  val version   = "0.0.21"
  val imageName = s"renku/renku-jena:$version"
  val image     = DockerImageName.parse(imageName)

  def create(mode: JenaRunMode): SingleContainer[_] = mode match {
    case JenaRunMode.GenericContainer =>
      GenericContainer(
        dockerImage = imageName,
        exposedPorts = Seq(3030),
        waitStrategy = Wait forHttp "/$/ping"
      )
    case JenaRunMode.FixedPortContainer(fixedPort) =>
      FixedHostPortGenericContainer(
        imageName = imageName,
        exposedPorts = Seq(3030),
        exposedHostPort = fixedPort,
        exposedContainerPort = fixedPort,
        waitStrategy = Wait forHttp "/$/ping"
      )
    case JenaRunMode.Local(_) =>
      new GenericContainer(new containers.GenericContainer("") {
        override def start(): Unit = ()
        override def stop():  Unit = ()
      })
  }

  def serverPort(mode: JenaRunMode, cnt: SingleContainer[_]): Int Refined Positive = mode match {
    case JenaRunMode.GenericContainer         => Refined.unsafeApply(cnt.mappedPort(cnt.exposedPorts.head))
    case JenaRunMode.FixedPortContainer(port) => port
    case JenaRunMode.Local(port)              => port
  }

  def fusekiUrl(mode: JenaRunMode, cnt: SingleContainer[_]): String = s"http://localhost:${serverPort(mode, cnt)}"
  def fusekiUri(mode: JenaRunMode, cnt: SingleContainer[_]): Uri    = Uri.unsafeFromString(fusekiUrl(mode, cnt))
}
