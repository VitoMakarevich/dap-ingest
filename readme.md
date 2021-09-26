# About
Simple data generator to simulate CDC records. Creates 1000 UUIDs and then
ingests 5000 records and waits for 1 sec, works as an indefinite loop.
As the result, you have a potentially unlimited amount of records that are spread
between 1000 UUIDs.
# Format
Each record is in format (`id`: uuid, `order`: long, `value`: string, `timestamp`: long). Where:
- `id` is a primary key of a table.
- `order` is an ordering field that represents the order in which rows with this `id` was updated, bigger is later.
- `value` is the value of the record(database row).
- `timestamp` is the time when the row was changed.