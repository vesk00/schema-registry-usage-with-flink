AVRO and Schema Evolution

6 rules to ensure full compatibility
Make your primary key required.
Give defaults to all the fields that could be removed in the future.
Be very careful when using enums as they cannot evolve over time.
Don't rename fields. You can add aliases instead.
When evolving a schema, ALWAYS give default values.
When evolving schema, NEVER delete a required field.


Kinds of schema evoluton
Backward: a backward compatible change is when a new schema can be used to read old data
we can read old data with new schema, thanks to a default value. In case the field does not exists, Avro will use the default

Forward: a forward compatible change is when and old schema can be used to read new data
we can read new data with the old schema, Avro will just ignore new fields. 
deleting fields with no defaults is not forward compatible

Full: both backward and forward
only add fields with defaults
only remove fields with defaults
Breaking: none of those
Adding/Removing elements from Enum
changing the type of a filed (string → int for example)
renaming a required field (field without default)
DON'T DO THAT
How to change compatibility - scenarios
If you write forward compatible change (very common)

→ update your produced to V2, you won't brake your consumers

→ take time to update your consumers to V2



If you write backward compatible change (less common)

→ update all consumers to V2, you will still be able to read V1 producer data

→ when all are updated, update producer to V2
