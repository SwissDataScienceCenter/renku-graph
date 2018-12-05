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

package ch.datascience.config

import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{ TinyType, TinyTypeFactory }
import com.typesafe.config.Config
import javax.inject.{ Inject, Singleton }
import play.api.{ ConfigLoader, Configuration }

import scala.language.implicitConversions

class DatasetName private ( val value: String ) extends AnyVal with TinyType[String]

object DatasetName
  extends TinyTypeFactory[String, DatasetName]( new DatasetName( _ ) )
  with NonBlank {

  implicit object DatasetNameFinder extends ConfigLoader[DatasetName] {
    override def load( config: Config, path: String ): DatasetName = DatasetName( config.getString( path ) )
  }
}

sealed trait DatasetType extends TinyType[String]

object DatasetType {

  case object Mem extends DatasetType {
    override val value: String = "mem"
  }

  case object TDB extends DatasetType {
    override val value: String = "tdb"
  }

  implicit object DatasetTypeFinder extends ConfigLoader[DatasetType] {
    override def load( config: Config, path: String ): DatasetType = config.getString( path ) match {
      case Mem.value => Mem
      case TDB.value => TDB
      case other     => throw new IllegalArgumentException( s"'$other' is not valid dataset type" )
    }
  }
}

class Username private ( val value: String ) extends AnyVal with TinyType[String]

object Username
  extends TinyTypeFactory[String, Username]( new Username( _ ) )
  with NonBlank {

  implicit object UsernameFinder extends ConfigLoader[Username] {
    override def load( config: Config, path: String ): Username = Username( config.getString( path ) )
  }
}

class Password private ( val value: String ) extends AnyVal with TinyType[String]

object Password
  extends TinyTypeFactory[String, Password]( new Password( _ ) )
  with NonBlank {

  implicit object PasswordFinder extends ConfigLoader[Password] {
    override def load( config: Config, path: String ): Password = Password( config.getString( path ) )
  }
}

@Singleton
case class FusekiConfig(
    fusekiBaseUrl: ServiceUrl,
    datasetName:   DatasetName,
    datasetType:   DatasetType,
    username:      Username,
    password:      Password
) {

  @Inject def this( configuration: Configuration ) = this(
    configuration.get[ServiceUrl]( "services.fuseki.url" ),
    configuration.get[DatasetName]( "services.fuseki.dataset-name" ),
    configuration.get[DatasetType]( "services.fuseki.dataset-type" ),
    configuration.get[Username]( "services.fuseki.username" ),
    configuration.get[Password]( "services.fuseki.password" )
  )
}
