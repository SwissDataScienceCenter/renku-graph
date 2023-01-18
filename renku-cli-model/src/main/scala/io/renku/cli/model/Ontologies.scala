package io.renku.cli.model

import io.renku.graph.model.Schemas.{prov, renku, schema}

private[cli] object Ontologies {

  object Schema {
    val Action          = schema / "Action"
    val CreativeWork    = schema / "CreativeWork"
    val Dataset         = schema / "Dataset"
    val DigitalDocument = schema / "DigitalDocument"
    val URL             = schema / "URL"
    val PropertyValue   = schema / "PropertyValue"
    val Person          = schema / "Person"

    val affiliation    = schema / "affiliation"
    val email          = schema / "email"
    val valueReference = schema / "valueReference"
    val value          = schema / "value"
    val name           = schema / "name"
    val description    = schema / "description"
    val creator        = schema / "creator"
    val dateCreated    = schema / "dateCreated"
    val dateModified   = schema / "dateModified"
    val keywords       = schema / "keywords"
    val defaultValue   = schema / "defaultValue"
    val encodingFormat = schema / "encodingFormat"
    val identifier     = schema / "identifier"
    val datePublished  = schema / "datePublished"
    val sameAs         = schema / "sameAs"
    val image          = schema / "image"
    val license        = schema / "license"
    val version        = schema / "version"
    val hasPart        = schema / "hasPart"
    val url            = schema / "url"
  }

  object Prov {
    val Entity        = prov / "Entity"
    val Plan          = prov / "Plan"
    val SoftwareAgent = prov / "SoftwareAgent"
    val Collection    = prov / "Collection"
    val Person        = prov / "Person"

    val wasDerivedFrom      = prov / "wasDerivedFrom"
    val invalidatedAtTime   = prov / "invalidatedAtTime"
    val entity              = prov / "entity"
    val atLocation          = prov / "atLocation"
    val qualifiedGeneration = prov / "qualifiedGeneration"
  }

  object Renku {
    val Plan                      = renku / "Plan"
    val CompositePlan             = renku / "CompositePlan"
    val CommandOutput             = renku / "CommandOutput"
    val CommandParameterBase      = renku / "CommandParameterBase"
    val CommandInput              = renku / "CommandInput"
    val CommandParameter          = renku / "CommandParameter"
    val IOStream                  = renku / "IOStream"
    val ParameterLink             = renku / "ParameterLink"
    val ParameterMapping          = renku / "ParameterMapping"
    val ParameterValue            = renku / "ParameterValue"
    val WorkflowFilePlan          = renku / "WorkflowFilePlan"
    val WorkflowFileCompositePlan = renku / "WorkflowFileCompositePlan"

    val command            = renku / "command"
    val linkSource         = renku / "linkSource"
    val linkSink           = renku / "linkSink"
    val streamType         = renku / "streamType"
    val checksum           = renku / "checksum"
    val workflowLink       = renku / "workflowLinks"
    val hasMappings        = renku / "hasMappings"
    val hasSubprocess      = renku / "hasSubprocess"
    val hasArguments       = renku / "hasArguments"
    val hasInputs          = renku / "hasInputs"
    val hasOutputs         = renku / "hasOutputs"
    val successCodes       = renku / "successCodes"
    val position           = renku / "position"
    val prefix             = renku / "prefix"
    val mappedTo           = renku / "mappedTo"
    val mapsTo             = renku / "mapsTo"
    val createFolder       = renku / "createFolder"
    val slug               = renku / "slug"
    val originalIdentifier = renku / "originalIdentifier"
    val external           = renku / "external"
    val source             = renku / "source"
  }
}
