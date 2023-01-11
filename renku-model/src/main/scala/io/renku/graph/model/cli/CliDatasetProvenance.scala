package io.renku.graph.model.cli

import io.renku.graph.model.InvalidationTime
import io.renku.graph.model.datasets._

/** View on the dataset focusing on provenance properties.
 */
final case class CliDatasetProvenance(
    createdOrPublished: Date,
    modifiedAt:         Option[DateModified],
    sameAs:             Option[CliDatasetSameAs],
    derivedFrom:        Option[DerivedFrom],
    originalIdentifier: Option[OriginalIdentifier],
    invalidationTime:   Option[InvalidationTime]
)
