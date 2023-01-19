package io.renku.cli.model

import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

final class EntityPath private (val value: String) extends StringTinyType

object EntityPath extends TinyTypeFactory[EntityPath](new EntityPath(_)) with TinyTypeJsonLDOps[EntityPath]
