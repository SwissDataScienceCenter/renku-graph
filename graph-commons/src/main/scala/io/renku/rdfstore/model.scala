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

package io.renku.rdfstore

import cats.Show
import io.renku.config.ConfigLoader.stringTinyTypeReader
import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}
import pureconfig.ConfigReader

import java.io.FileNotFoundException
import scala.io.{BufferedSource, Source}

class DatasetName private (val value: String) extends AnyVal with StringTinyType
object DatasetName extends TinyTypeFactory[DatasetName](new DatasetName(_)) with NonBlank[DatasetName] {
  implicit val configReader: ConfigReader[DatasetName] = stringTinyTypeReader(DatasetName)
}

trait DatasetConfigFile extends StringTinyType
object DatasetConfigFile {
  implicit lazy val show: Show[DatasetConfigFile] = Show.show(_.toString)
}
abstract class DatasetConfigFileFactory[TT <: DatasetConfigFile](instantiate: String => TT, ttlFileName: String)
    extends TinyTypeFactory[TT](instantiate) {
  import cats.syntax.all._

  def fromTtlFile(): Either[Exception, TT] = instance

  private lazy val instance: Either[Exception, TT] = readFromFile() >>= from

  private def readFromFile(): Either[Exception, String] =
    readTtlLines
      .leftMap(new Exception(s"Problems while reading $ttlFileName", _))
      .flatMap(validateLines)
      .map(_.mkString("\n"))

  private def readTtlLines =
    findFile.map {
      _.getLines().toList
    }

  private def findFile = {
    val locationsAndReaders: Seq[(String, String => BufferedSource)] = List(
      ttlFileName                                         -> ((f: String) => Source.fromResource(f)),
      ttlFileName                                         -> ((f: String) => Source.fromFile(f)),
      s"graph-commons/src/main/resources/$ttlFileName"    -> ((f: String) => Source.fromFile(f)),
      s"../graph-commons/src/main/resources/$ttlFileName" -> ((f: String) => Source.fromFile(f))
    )

    locationsAndReaders.foldLeft[Either[Throwable, BufferedSource]](
      new FileNotFoundException(
        s"$ttlFileName cannot be found in the locations: ${locationsAndReaders.map(_._1).toSet.mkString(", ")}"
      ).asLeft
    ) {
      case (source @ Right(_), _) => source
      case (_, (file, reader))    => Either.catchNonFatal(reader(file))
    }
  }

  private def validateLines: List[String] => Either[Exception, List[String]] = {
    case Nil   => new Exception(s"$ttlFileName is empty").asLeft
    case lines => lines.asRight
  }
}
