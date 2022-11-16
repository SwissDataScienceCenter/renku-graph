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

package io.renku.graph.model.entities

import cats.syntax.all._
import io.circe.Json
import io.circe.literal._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestamps
import io.renku.graph.model.GraphModelGenerators.{graphClasses, projectCreatedDates}
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model._
import io.renku.graph.model.entities.Generators._
import io.renku.graph.model.testentities._
import io.renku.jsonld.{JsonLD, JsonLDEncoder}
import io.renku.jsonld.JsonLDEncoder.encodeEntityId
import io.renku.jsonld.parser._
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import com.softwaremill.diffx.scalatest.DiffShouldMatcher

class PlanSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with DiffShouldMatcher
    with DiffInstances {

  "test decode" in {
    val jsonString =
      """[
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/4d75b8788bef4ebdbf9f03f953354ef9",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CompositePlan",
        |      "http://www.w3.org/ns/prov#Plan",
        |      "http://schema.org/Action",
        |      "http://schema.org/CreativeWork"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "guifSGfZ ho YfOJeI czyEu GjENiDRQ hiDVNUh moxIlhVw WEzEqBSO"
        |    },
        |    "http://www.w3.org/ns/prov#wasDerivedFrom" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/plans/182a5d40754640de8d1d3f5a208f29a9"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#hasSubprocess" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972"
        |      }
        |    ],
        |    "http://schema.org/creator" : [
        |    ],
        |    "http://www.w3.org/ns/prov#invalidatedAtTime" : {
        |      "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
        |      "@value" : "2015-03-29T21:03:55.269Z"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#hasMappings" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/parameterMapping/714340ef48544ff1869e0232ec34ce86"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/parameterMapping/45f99a4b7e7d44cca828e80668877e6c"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/parameterMapping/0ccd400309424197b2957c1e78a11f0a"
        |      }
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#topmostDerivedFrom" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/plans/182a5d40754640de8d1d3f5a208f29a9"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "QnZqFXxVr"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#workflowLinks" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/parameterLink/636160d55a694e0ea32a5aa85a9d7b4f"
        |      }
        |    ],
        |    "http://schema.org/dateCreated" : {
        |      "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
        |      "@value" : "1984-06-18T12:58:44.677Z"
        |    },
        |    "http://schema.org/keywords" : [
        |    ]
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/parameterLink/636160d55a694e0ea32a5aa85a9d7b4f",
        |    "@type" : "https://swissdatasciencecenter.github.io/renku-ontology#ParameterLink",
        |    "https://swissdatasciencecenter.github.io/renku-ontology#linkSink" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/parameters/2"
        |      }
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#linkSource" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/CBBJsd"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/parameterMapping/714340ef48544ff1869e0232ec34ce86",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#ParameterMapping",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "EyGhdthH NeYNEW PnQaGv oJa bSUQbnsaQw IKTSw bGSKAAsiVK"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "CkVJyH"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mapsTo" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/parameters/1"
        |      }
        |    ],
        |    "http://schema.org/name" : {
        |      "@value" : "btsLxOVukl"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/parameterMapping/45f99a4b7e7d44cca828e80668877e6c",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#ParameterMapping",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "HlRRtOQF GwmFBl Evrg fc"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "faF"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mapsTo" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/Dmg"
        |      }
        |    ],
        |    "http://schema.org/name" : {
        |      "@value" : "pCTDFcSVmh"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/parameterMapping/0ccd400309424197b2957c1e78a11f0a",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#ParameterMapping",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "aSV NVDYSaZu BDxFwoB McydYEQLV TlpMivrVT ABqBBIHnSZ ZYyC UzUDn WbTc soW"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "fVIO"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mapsTo" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/parameters/2"
        |      }
        |    ],
        |    "http://schema.org/name" : {
        |      "@value" : "CDU"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#Plan",
        |      "http://www.w3.org/ns/prov#Plan",
        |      "http://schema.org/Action",
        |      "http://schema.org/CreativeWork"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#hasArguments" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/parameters/1"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/parameters/2"
        |      }
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#command" : {
        |      "@value" : "EyvrWKXgm"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#hasInputs" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/hVo"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/ZfN"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/dbPv"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/UFSIWvxlF"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/qPQl"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/NM"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/HLCgjFVRP"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/MbAUuMxPl"
        |      }
        |    ],
        |    "http://schema.org/creator" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/persons/19607823"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/persons/74074976"
        |      }
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#successCodes" : [
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#topmostDerivedFrom" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "edyUXs"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#hasOutputs" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/pAEhNcFf"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/Dmg"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/NA"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/CBBJsd"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/YsSBI"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/OuB"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/TQVYUtMSA"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/TGihNReMiE"
        |      }
        |    ],
        |    "http://schema.org/dateCreated" : {
        |      "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
        |      "@value" : "2005-11-09T21:50:45.812Z"
        |    },
        |    "http://schema.org/keywords" : [
        |    ]
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/pAEhNcFf",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/description" : {
        |      "@value" : "MVakdEtog"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "IvgwQz-u"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/nFt"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "cMocXwy"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "US-ASCII"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 11
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "pAEhNcFf"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/nFt",
        |    "@type" : "https://swissdatasciencecenter.github.io/renku-ontology#IOStream",
        |    "https://swissdatasciencecenter.github.io/renku-ontology#streamType" : {
        |      "@value" : "stderr"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/Dmg",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/description" : {
        |      "@value" : "YV"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "_bapO-"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/MxcxQLpRw"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "UTF-16"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 12
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "Dmg"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/MxcxQLpRw",
        |    "@type" : "https://swissdatasciencecenter.github.io/renku-ontology#IOStream",
        |    "https://swissdatasciencecenter.github.io/renku-ontology#streamType" : {
        |      "@value" : "stdout"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/NA",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/description" : {
        |      "@value" : "XTdZuAGLFN"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "EV-iCYnhdW/r_CD/gKCA/Sippb/kcdxVv-_d/lFVIqJHtn/tprIrIj/SRuou_s/R-odA/okXB"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/nFt"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "JMeAM"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "cnIgmBYFct"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 13
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "NA"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/CBBJsd",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : true
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "-TZiXo"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/MxcxQLpRw"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "HVi"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "ISO-8859-1"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 14
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "CBBJsd"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/YsSBI",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/description" : {
        |      "@value" : "jVGMNaUuhA"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "_cFgpM/iXXSbFH"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/nFt"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "cnIgmBYFct"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 15
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "YsSBI"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/OuB",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : true
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "LCkGXxEeP/LIDsweed/GtRIWQjHHU/APK-pYyMUW/EGdy-Ix/EgmYkj_/uiOnOn"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "cJaTUZFo"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "UTF-8"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "OuB"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/TQVYUtMSA",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "icV_/TpfQNinI/PDhkahS/gwaf"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "tObWZz"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "TQVYUtMSA"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/outputs/TGihNReMiE",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : true
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Gi_EXiS/-sjYLkB-s/HZaU-Oti/O_kPak/IDeAkqVAh/mv_J/dYmjPzmk-/mdhEo"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "LH"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "ISO-8859-1"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "TGihNReMiE"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/persons/19607823",
        |    "@type" : [
        |      "http://www.w3.org/ns/prov#Person",
        |      "http://schema.org/Person"
        |    ],
        |    "http://schema.org/name" : {
        |      "@value" : "VjEv-CQ lOWXED"
        |    },
        |    "http://schema.org/sameAs" : [
        |      {
        |        "@id" : "https://IifAgEfk:7296/dAZl-/QgvM/cEjKEOwv/ZOKmF/api/v4/users/19607823"
        |      },
        |      {
        |        "@id" : "https://orcid.org/4330-5043-9663-3085"
        |      }
        |    ],
        |    "http://schema.org/affiliation" : {
        |      "@value" : "lLLJy"
        |    },
        |    "http://schema.org/email" : {
        |      "@value" : "L1hHa~@Ddb"
        |    }
        |  },
        |  {
        |    "@id" : "https://IifAgEfk:7296/dAZl-/QgvM/cEjKEOwv/ZOKmF/api/v4/users/19607823",
        |    "@type" : "http://schema.org/URL",
        |    "http://schema.org/additionalType" : {
        |      "@value" : "GitLab"
        |    },
        |    "http://schema.org/identifier" : {
        |      "@value" : 19607823
        |    }
        |  },
        |  {
        |    "@id" : "https://orcid.org/4330-5043-9663-3085",
        |    "@type" : "http://schema.org/URL",
        |    "http://schema.org/additionalType" : {
        |      "@value" : "Orcid"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/persons/74074976",
        |    "@type" : [
        |      "http://www.w3.org/ns/prov#Person",
        |      "http://schema.org/Person"
        |    ],
        |    "http://schema.org/name" : {
        |      "@value" : "wLXEo flfzGOCu"
        |    },
        |    "http://schema.org/sameAs" : [
        |      {
        |        "@id" : "https://IifAgEfk:7296/dAZl-/QgvM/cEjKEOwv/ZOKmF/api/v4/users/74074976"
        |      }
        |    ],
        |    "http://schema.org/email" : {
        |      "@value" : "JRgmgT@Mccr"
        |    }
        |  },
        |  {
        |    "@id" : "https://IifAgEfk:7296/dAZl-/QgvM/cEjKEOwv/ZOKmF/api/v4/users/74074976",
        |    "@type" : "http://schema.org/URL",
        |    "http://schema.org/additionalType" : {
        |      "@value" : "GitLab"
        |    },
        |    "http://schema.org/identifier" : {
        |      "@value" : 74074976
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/hVo",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "TEVnprH"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Jn_cW-eZR/AM-uJQkfTp/LlPcLyARZ_/HLqTode"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 3
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "hVo"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/ZfN",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "uqrKGor"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "xnnJes/Px-RtSG"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 4
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "ZfN"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/dbPv",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "Aq"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "iOItHj/ta-NYHc"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 5
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "dbPv"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/UFSIWvxlF",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "flw"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "bu-OpLu_/wx-xu_hh_S/GTODir/zvkGjcG/v_WSmVaPTm/dhTjD_Yhu/fhswA/JsXOTnzvF/ayNkdw/-_xFTHV_G"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "dvvCXPjT"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "ISO-8859-1"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 6
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "UFSIWvxlF"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/qPQl",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "qXSJzFHrk"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "pqPIDIzz/h--_EF/zcKAKBCLiv/hOWm/RbgDyb/QfVzFMF-U/pQa_/LBGS"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/iostreams/stdin"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "OJ"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 7
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "qPQl"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/iostreams/stdin",
        |    "@type" : "https://swissdatasciencecenter.github.io/renku-ontology#IOStream",
        |    "https://swissdatasciencecenter.github.io/renku-ontology#streamType" : {
        |      "@value" : "stdin"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/NM",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "InnvHkPtYe"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Ph-S"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/iostreams/stdin"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "zvU"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "cnIgmBYFct"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 8
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "NM"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/HLCgjFVRP",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "RHfW"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Kb-wM/Ddwtdz/YVGsXWQYaI/lOUw/-q-JVY/Y-mcP-w-/NbTfQZx/HZmNpiVDFr/AQ-A"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/iostreams/stdin"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "sxw"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "US-ASCII"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 9
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "HLCgjFVRP"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/inputs/MbAUuMxPl",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "lju"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Pjv_Jg/Ox_YZZZF"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "US-ASCII"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "MbAUuMxPl"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/parameters/1",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameter",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "ZMzhaXUtNt"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "BXQQBqgbgP"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 1
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "jxjlnJgJE"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/9ece35f9877d42b9b5241a9bc8a61736/parameters/2",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameter",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "sUCn"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "myjb"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "FTIKTosGvj"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 2
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "fI"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#Plan",
        |      "http://www.w3.org/ns/prov#Plan",
        |      "http://schema.org/Action",
        |      "http://schema.org/CreativeWork"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#hasArguments" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/parameters/1"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/parameters/2"
        |      }
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#hasInputs" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/hVo"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/ZfN"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/dbPv"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/UFSIWvxlF"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/qPQl"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/NM"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/HLCgjFVRP"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/MbAUuMxPl"
        |      }
        |    ],
        |    "http://schema.org/creator" : [
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#successCodes" : [
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#topmostDerivedFrom" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "VtpFGt"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#hasOutputs" : [
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/pAEhNcFf"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/Dmg"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/NA"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/CBBJsd"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/YsSBI"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/OuB"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/TQVYUtMSA"
        |      },
        |      {
        |        "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/TGihNReMiE"
        |      }
        |    ],
        |    "http://schema.org/dateCreated" : {
        |      "@type" : "http://www.w3.org/2001/XMLSchema#dateTime",
        |      "@value" : "2006-04-12T19:33:36.265Z"
        |    },
        |    "http://schema.org/keywords" : [
        |    ]
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/pAEhNcFf",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/description" : {
        |      "@value" : "MVakdEtog"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "IvgwQz-u"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/nFt"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "cMocXwy"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "US-ASCII"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 11
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "pAEhNcFf"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/Dmg",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/description" : {
        |      "@value" : "YV"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "_bapO-"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/MxcxQLpRw"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "UTF-16"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 12
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "Dmg"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/NA",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/description" : {
        |      "@value" : "XTdZuAGLFN"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "EV-iCYnhdW/r_CD/gKCA/Sippb/kcdxVv-_d/lFVIqJHtn/tprIrIj/SRuou_s/R-odA/okXB"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/nFt"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "JMeAM"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "cnIgmBYFct"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 13
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "NA"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/CBBJsd",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : true
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "-TZiXo"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/MxcxQLpRw"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "HVi"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "ISO-8859-1"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 14
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "CBBJsd"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/YsSBI",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/description" : {
        |      "@value" : "jVGMNaUuhA"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "_cFgpM/iXXSbFH"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/nFt"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "cnIgmBYFct"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 15
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "YsSBI"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/OuB",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : true
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "LCkGXxEeP/LIDsweed/GtRIWQjHHU/APK-pYyMUW/EGdy-Ix/EgmYkj_/uiOnOn"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "cJaTUZFo"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "UTF-8"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "OuB"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/TQVYUtMSA",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : false
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "icV_/TpfQNinI/PDhkahS/gwaf"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "tObWZz"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "TQVYUtMSA"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/outputs/TGihNReMiE",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandOutput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#createFolder" : {
        |      "@value" : true
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Gi_EXiS/-sjYLkB-s/HZaU-Oti/O_kPak/IDeAkqVAh/mv_J/dYmjPzmk-/mdhEo"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "LH"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "ISO-8859-1"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "TGihNReMiE"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/hVo",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "TEVnprH"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Jn_cW-eZR/AM-uJQkfTp/LlPcLyARZ_/HLqTode"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 3
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "hVo"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/ZfN",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "uqrKGor"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "xnnJes/Px-RtSG"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 4
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "ZfN"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/dbPv",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "Aq"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "iOItHj/ta-NYHc"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 5
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "dbPv"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/UFSIWvxlF",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "flw"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "bu-OpLu_/wx-xu_hh_S/GTODir/zvkGjcG/v_WSmVaPTm/dhTjD_Yhu/fhswA/JsXOTnzvF/ayNkdw/-_xFTHV_G"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "dvvCXPjT"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "ISO-8859-1"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 6
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "UFSIWvxlF"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/qPQl",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "qXSJzFHrk"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "pqPIDIzz/h--_EF/zcKAKBCLiv/hOWm/RbgDyb/QfVzFMF-U/pQa_/LBGS"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/iostreams/stdin"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "OJ"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 7
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "qPQl"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/NM",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "InnvHkPtYe"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Ph-S"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/iostreams/stdin"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "zvU"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "cnIgmBYFct"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 8
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "NM"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/HLCgjFVRP",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "RHfW"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Kb-wM/Ddwtdz/YVGsXWQYaI/lOUw/-q-JVY/Y-mcP-w-/NbTfQZx/HZmNpiVDFr/AQ-A"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mappedTo" : {
        |      "@id" : "http://nlcuk:8759/-lAQsFm/iostreams/stdin"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "sxw"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "US-ASCII"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 9
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "HLCgjFVRP"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/inputs/MbAUuMxPl",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandInput",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "lju"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "Pjv_Jg/Ox_YZZZF"
        |    },
        |    "http://schema.org/encodingFormat" : {
        |      "@value" : "US-ASCII"
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "MbAUuMxPl"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/parameters/1",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameter",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "ZMzhaXUtNt"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "BXQQBqgbgP"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 1
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "jxjlnJgJE"
        |    }
        |  },
        |  {
        |    "@id" : "http://nlcuk:8759/-lAQsFm/plans/75c32f90b45f4ef1bdd0c43e462cb972/parameters/2",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameter",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "sUCn"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "myjb"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#prefix" : {
        |      "@value" : "FTIKTosGvj"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#position" : {
        |      "@value" : 2
        |    },
        |    "http://schema.org/name" : {
        |      "@value" : "fI"
        |    }
        |  }
        |]
        |""".stripMargin

    val jsonLD = io.renku.jsonld.parser.parse(jsonString).getOrElse(sys.error("parsing jsonld failed"))
    val plan   = jsonLD.cursor.as[List[entities.CompositePlan]]
    println(plan)
  }

  "decode param mapping" in {
    val mappingJsonString =
      """
        |[{
        |    "@id" : "http://kpgrb:6217/parameterMapping/92b76e0390a544c9aa9c89d30a90aaa3",
        |    "@type" : [
        |      "https://swissdatasciencecenter.github.io/renku-ontology#ParameterMapping",
        |      "https://swissdatasciencecenter.github.io/renku-ontology#CommandParameterBase"
        |    ],
        |    "http://schema.org/description" : {
        |      "@value" : "CLCxDSceT WyP HQ ffdJfIr SB kRnC htk XweZJr"
        |    },
        |    "http://schema.org/defaultValue" : {
        |      "@value" : "TBEOXlKuL"
        |    },
        |    "https://swissdatasciencecenter.github.io/renku-ontology#mapsTo" : [
        |      {
        |        "@id" : "http://kpgrb:6217/plans/9aa1960659ab4b12abddacb8e75b3f46/parameters/4"
        |      }
        |    ],
        |    "http://schema.org/name" : {
        |      "@value" : "wVmtNaf"
        |    }
        |  }]
        |""".stripMargin

    val jsonLD  = io.renku.jsonld.parser.parse(mappingJsonString).getOrElse(sys.error("parsing failed"))
    val decoded = jsonLD.cursor.as[List[entities.ParameterMapping]]
    println(decoded)
  }

  "decode (StepEntity)" should {
    implicit val graph: GraphClass = GraphClass.Default

    "turn JsonLD of a non-modified Plan entity into the StepPlan object" in {
      forAll(plans) { plan =>
        flattenedJsonLD(plan).cursor
          .as[List[entities.StepPlan]] shouldBe List(plan.to[entities.StepPlan]).asRight
      }
    }

    "turn JsonLD of a modified Plan entity into the StepPlan object" in {
      forAll(plans.map(_.createModification())) { plan =>
        flattenedJsonLD(plan).cursor
          .as[List[entities.StepPlan]] shouldBe List(plan.to[entities.StepPlan]).asRight
      }
    }

    "decode if invalidation after the creation date" in {

      val plan = plans
        .map(_.invalidate())
        .generateOne
        .to[entities.StepPlan]

      flattenedJsonLD(plan).cursor.as[List[entities.StepPlan]] shouldBe List(plan).asRight
    }

    "fail if invalidatedAtTime present on non-modified Plan" in {

      val plan = plans
        .map(_.invalidate())
        .generateOne
        .to[entities.StepPlan]

      val jsonLD = parse {
        plan.asJsonLD.toJson.hcursor
          .downField((prov / "wasDerivedFrom").show)
          .delete
          .top
          .getOrElse(fail("Invalid Json after removing property"))
      }.flatMap(_.flatten).fold(throw _, identity)

      val Left(message) = jsonLD.cursor.as[List[entities.StepPlan]].leftMap(_.message)
      message should include(show"Plan ${plan.resourceId} has no parent but invalidation time")
    }

    "fail if invalidation done before the creation date" in {

      val plan = plans
        .map(_.invalidate())
        .generateOne
        .to[entities.StepPlan]

      val invalidationTime = timestamps(max = plan.dateCreated.value.minusSeconds(1)).generateAs(InvalidationTime)
      val jsonLD = parse {
        plan.asJsonLD.toJson.hcursor
          .downField((prov / "invalidatedAtTime").show)
          .delete
          .top
          .getOrElse(fail("Invalid Json after removing property"))
          .deepMerge(
            Json.obj(
              (prov / "invalidatedAtTime").show -> json"""{"@value": ${invalidationTime.show}}"""
            )
          )
      }.flatMap(_.flatten).fold(throw _, identity)

      val Left(message) = jsonLD.cursor.as[List[entities.StepPlan]].leftMap(_.message)
      message should include {
        show"Invalidation time $invalidationTime on StepPlan ${plan.resourceId} is older than dateCreated ${plan.dateCreated}"
      }
    }
  }

  def compositePlanGen: Gen[CompositePlan] =
    for {
      created        <- projectCreatedDates()
      paramFactories <- commandParametersLists
      plan           <- compositePlanEntities(planEntitiesList(paramFactories: _*)).run(created)
    } yield plan

  "decode (CompositeEntity)" should {
    implicit val graphClass: GraphClass = GraphClass.Default

    "return json for a non-modified composite plan" in {
      forAll(compositePlanGen) { plan =>
        val expected = plan.to[entities.CompositePlan]
        flattenedJsonLD(plan).cursor
          .as[List[entities.CompositePlan]]
          .fold(fail(_), _.filter(_.resourceId == expected.resourceId))
          .shouldMatchTo(List(plan.to[entities.CompositePlan]))
      }
    }

    "return json for a modified composite plan" in {
      forAll(compositePlanGen.map(_.createModification())) { plan =>
        val expected = plan.to[entities.CompositePlan]
        flattenedJsonLD(plan).cursor
          .as[List[entities.CompositePlan]]
          .fold(fail(_), _.filter(_.resourceId == expected.resourceId))
          .shouldMatchTo(List(plan.to[entities.CompositePlan]))
      }
    }

    "decode if invalidation after the creation date (composite plan)" in {
      val plan = compositePlanGen
        .map(_.createModification().invalidate())
        .generateOne
      val expected = List(plan.to[entities.CompositePlan])
      val json     = flattenedJsonLD(plan)

      json.cursor
        .as[List[entities.CompositePlan]]
        .fold(fail(_), _.filter(_.resourceId == expected.head.resourceId))
        .shouldMatchTo(expected)
    }

    "pick up the correct decoder" in {
      val cp = compositePlanGen.generateOne
        .to[entities.CompositePlan]

      val sp = plans.generateOne.to[entities.StepPlan]

      val spDecoded = flattenedJsonLD(sp).cursor.as[List[entities.Plan]]
      val cpDecoded = flattenedJsonLD(cp).cursor.as[List[entities.Plan]]

      spDecoded shouldBe Right(List(sp))
      cpDecoded shouldBe Right(List(cp))
    }

    "fail decode if a parameter maps to itself" in {
//      val pm_ = parameterMappingEntities.generateOne
//      val pm  = pm_.copy(mappedParameter = NonEmptyList.one(pm_.id.value))
//
//      val cp = compositePlanEntities()(projectCreatedDates().generateOne).generateOne
//        .asInstanceOf[CompositePlan.NonModified]
//        .addParamMapping(pm)
//        .to[entities.CompositePlan]
//
//      val decoded = flattenedJsonLD(cp).cursor
//        .as[List[entities.CompositePlan]]
//        .swap
//        .getOrElse(sys.error("Expected decoding to fail, but it was successful"))
//
//      decoded.message should include(
//        show"Parameter ${pm.toEntitiesParameterMapping.resourceId} maps to itself"
//      )
    }

    "fail if invalidatedAtTime present on non-modified CompositePlan" in {
      val plan = compositePlanGen
        .map(_.createModification().invalidate())
        .generateOne
        .to[entities.CompositePlan]
      val jsonValue = plan.asJsonLD.toJson.hcursor
        .downField((prov / "wasDerivedFrom").show)
        .delete
        .top
        .getOrElse(fail("Invalid Json after removing property"))

      val jsonLD = parse(jsonValue).flatMap(_.flatten).fold(throw _, identity)

      val Left(message) = jsonLD.cursor.as[List[entities.CompositePlan]].leftMap(_.message)
      message should include(show"Plan ${plan.resourceId} has no parent but invalidation time")
    }

    "fail if invalidation done before the creation date on a CompositePlan" in {
      val plan = compositePlanGen
        .map(_.createModification().invalidate())
        .generateOne
        .to[entities.CompositePlan]

      val invalidationTime = timestamps(max = plan.dateCreated.value.minusSeconds(1)).generateAs(InvalidationTime)
      val jsonLD = parse {
        plan.asJsonLD.toJson.hcursor
          .downField((prov / "invalidatedAtTime").show)
          .delete
          .top
          .getOrElse(fail("Invalid Json after removing property"))
          .deepMerge(
            Json.obj(
              (prov / "invalidatedAtTime").show -> json"""{"@value": ${invalidationTime.show}}"""
            )
          )
      }.flatMap(_.flatten).fold(throw _, identity)

      val Left(message) = jsonLD.cursor.as[List[entities.CompositePlan]].leftMap(_.message)
      message should include {
        show"Invalidation time $invalidationTime on CompositePlan ${plan.resourceId} is older than dateCreated ${plan.dateCreated}"
      }
    }
  }

  "encode for the Default Graph (StepEntity)" should {
    implicit val graph: GraphClass = GraphClass.Default

    "produce JsonLD for a non-modified Plan with all the relevant properties" in {
      val plan = plans.generateOne.replaceCreators(personEntities.generateList(min = 1)).to[entities.StepPlan]

      plan.asJsonLD shouldBe JsonLD
        .entity(
          plan.resourceId.asEntityId,
          entities.StepPlan.entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.resourceId.asEntityId.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD
        )
    }

    "produce JsonLD for a modified Plan with all the relevant properties" in {
      val plan = plans.generateOne
        .invalidate()
        .replaceCreators(personEntities.generateList(min = 1))
        .to(StepPlan.Modified.toEntitiesStepPlan)

      plan.asJsonLD shouldBe JsonLD
        .entity(
          plan.resourceId.asEntityId,
          entities.StepPlan.entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD,
          prov / "wasDerivedFrom"        -> plan.derivation.derivedFrom.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.derivation.originalResourceId.asEntityId.asJsonLD,
          prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
        )
    }
  }

  "encode for the Named Graphs (StepEntity)" should {
    implicit val graph: GraphClass = GraphClass.Project

    "produce JsonLD for a non-modified Plan with all the relevant properties" in {
      val plan = plans.generateOne.replaceCreators(personEntities.generateList(min = 1)).to[entities.StepPlan]

      plan.asJsonLD shouldBe JsonLD
        .entity(
          plan.resourceId.asEntityId,
          entities.StepPlan.entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.map(_.resourceId.asEntityId).asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.resourceId.asEntityId.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD
        )
    }

    "produce JsonLD for a modified Plan with all the relevant properties" in {
      val plan = plans.generateOne
        .invalidate()
        .replaceCreators(personEntities.generateList(min = 1))
        .to(StepPlan.Modified.toEntitiesStepPlan)

      plan.asJsonLD shouldBe JsonLD
        .entity(
          plan.resourceId.asEntityId,
          entities.StepPlan.entityTypes,
          schema / "name"                -> plan.name.asJsonLD,
          schema / "description"         -> plan.maybeDescription.asJsonLD,
          renku / "command"              -> plan.maybeCommand.asJsonLD,
          schema / "creator"             -> plan.creators.map(_.resourceId.asEntityId).asJsonLD,
          schema / "dateCreated"         -> plan.dateCreated.asJsonLD,
          schema / "programmingLanguage" -> plan.maybeProgrammingLanguage.asJsonLD,
          schema / "keywords"            -> plan.keywords.asJsonLD,
          renku / "hasArguments"         -> plan.parameters.asJsonLD,
          renku / "hasInputs"            -> plan.inputs.asJsonLD,
          renku / "hasOutputs"           -> plan.outputs.asJsonLD,
          renku / "successCodes"         -> plan.successCodes.asJsonLD,
          prov / "wasDerivedFrom"        -> plan.derivation.derivedFrom.asJsonLD,
          renku / "topmostDerivedFrom"   -> plan.derivation.originalResourceId.asEntityId.asJsonLD,
          prov / "invalidatedAtTime"     -> plan.maybeInvalidationTime.asJsonLD
        )
    }
  }
  "encode for the Named Graphs (CompositePlan)" should {
    implicit val graph: GraphClass = GraphClass.Project

    "produce JsonLD for a non-modified composite plan" in {
      val plan: entities.CompositePlan =
        compositePlanGen.generateOne.to[entities.CompositePlan]

      plan.asJsonLD shouldBe JsonLD.entity(
        plan.resourceId.asEntityId,
        entities.CompositePlan.Ontology.entityTypes,
        schema / "name"              -> plan.name.asJsonLD,
        schema / "description"       -> plan.maybeDescription.asJsonLD,
        schema / "creator"           -> plan.creators.map(_.resourceId.asEntityId).asJsonLD,
        schema / "dateCreated"       -> plan.dateCreated.asJsonLD,
        schema / "keywords"          -> plan.keywords.asJsonLD,
        renku / "hasSubprocess"      -> plan.plans.toList.asJsonLD,
        renku / "workflowLinks"      -> plan.links.asJsonLD,
        renku / "hasMappings"        -> plan.mappings.asJsonLD,
        renku / "topmostDerivedFrom" -> plan.resourceId.asEntityId.asJsonLD
      )
    }

    "produce JsonLD for a modified composite plan" in {
      val plan =
        compositePlanGen.generateOne
          .createModification(identity)
          .invalidate()
          .to(CompositePlan.Modified.toEntitiesCompositePlan)

      (plan: entities.CompositePlan).asJsonLD shouldBe JsonLD.entity(
        plan.resourceId.asEntityId,
        entities.CompositePlan.Ontology.entityTypes,
        schema / "name"              -> plan.name.asJsonLD,
        schema / "description"       -> plan.maybeDescription.asJsonLD,
        schema / "creator"           -> plan.creators.map(_.resourceId.asEntityId).asJsonLD,
        schema / "dateCreated"       -> plan.dateCreated.asJsonLD,
        schema / "keywords"          -> plan.keywords.asJsonLD,
        renku / "hasSubprocess"      -> plan.plans.toList.asJsonLD,
        renku / "workflowLinks"      -> plan.links.asJsonLD,
        renku / "hasMappings"        -> plan.mappings.asJsonLD,
        renku / "topmostDerivedFrom" -> plan.resourceId.asEntityId.asJsonLD,
        prov / "invalidatedAtTime"   -> plan.maybeInvalidationTime.asJsonLD,
        prov / "wasDerivedFrom"      -> plan.derivation.derivedFrom.asJsonLD,
        renku / "topmostDerivedFrom" -> plan.derivation.originalResourceId.asEntityId.asJsonLD
      )
    }
  }

  "entityFunctions.findAllPersons" should {

    "return all creators" in {
      val plan = plans.generateOne
        .replaceCreators(personEntities.generateList(min = 1))
        .to[entities.StepPlan]

      EntityFunctions[entities.Plan].findAllPersons(plan) shouldBe plan.creators.toSet
    }
  }

  "entityFunctions.encoder" should {

    "return encoder that honors the given GraphClass" in {

      val plan = plans.generateOne.to[entities.Plan]

      implicit val graph: GraphClass = graphClasses.generateOne
      val functionsEncoder = EntityFunctions[entities.Plan].encoder(graph)

      plan.asJsonLD(functionsEncoder) shouldBe plan.asJsonLD
    }
  }

  private lazy val plans = stepPlanEntities(commandParametersLists.generateOne: _*)(planCommands)
    .map(_.replaceCreators(personEntities.generateList(max = 2)))
    .run(projectCreatedDates().generateOne)

  private def flattenedJsonLD[A: JsonLDEncoder](value: A): JsonLD =
    JsonLDEncoder[A].apply(value).flatten.fold(fail(_), identity)
}
