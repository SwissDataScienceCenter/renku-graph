# renku cli model

TODO: double check docs on jsonld library and only link there

This is a small library for interacting with the output of the
renku-cli tool. Renku cli produces JSONLD data in a flat structure. So
every relation is moved outside an entity and added to the root array.
The id remains in the entity.

In order to decode a flattened jsonld structure, a normal
`JsonLDDecoder[A]` can be used, but it must be decoded into a
`List[A]`, like in `someJsonLD.as[List[A]]`. The exception is when
decoding a related entity *B* inside another entity *A*. Then the id
of *B* is known to the decoder, because it is still present in *A*. It
will then look up the correct related entity from the outer array. In
that case, it must be decoded to a single element, as in
`cursor.downField(someProperty).as[B]`. This is the reason the
decoders are defined for a single `A`. But since the cli always
returns a flattened JSONLD structure, the encoders are explicitly
marked and return a flattened structure.
