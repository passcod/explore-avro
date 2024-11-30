# explore-avro

A CLI for [Apache Avro](https://avro.apache.org/) exploration.

![Screenshot](./assets/image.png)

## Installation

### Compile from Source

```
cargo install --git https://github.com/passcod/explore-avro explore-avro
```

## Usage

```shell
> # Retrieve all columns for a list of records
> ravro get test.avro

+-----------+--------------+-----+
| firstName | lastName     | age |
+-----------+--------------+-----+
| Marty     | McFly        | 24  |
+-----------+--------------+-----+
| Biff      | Tannen       | 72  |
+-----------+--------------+-----+
| Emmett    | Brown        | 65  |
+-----------+--------------+-----+
| Loraine   | Baines-McFly | 62  |
+-----------+--------------+-----+

> # Search (using regular expressions)
> ravro get test.avro --search McFly

+-----------+--------------+-----+
| firstName | lastName     | age |
+-----------+--------------+-----+
| Marty     | McFly        | 24  | # the second field will appear in bold green here
+-----------+--------------+-----+
| Loraine   | Baines-McFly | 62  | # the second field will appear in bold green here
+-----------+--------------+-----+

> # Select only some columns
> ravro get test.avro --fields firstName age

+-----------+-----+
| firstName | age |
+-----------+-----+
| Marty     | 24  |
+-----------+-----+
| Biff      | 72  |
+-----------+-----+
| Emmett    | 65  |
+-----------+-----+
| Loraine   | 62  |
+-----------+-----+

> # Select the first 2 columns
> ravro get test*.avro --fields firstName age --take 2

+-----------+-----+
| firstName | age |
+-----------+-----+
| Marty     | 24  |
+-----------+-----+
| Biff      | 72  |
+-----------+-----+

> # Output as CSV
> ravro get test*.avro --fields firstName age --take 2 --format csv

firstName,age
Marty,24
Biff,72

> # Output as JSON
> ravro get test*.avro --fields firstName age --take 2 --format csv

{"firstName":"Marty","age":24}
{"firstName":"Biff","age":72}
```

## Options

- `fields (f)` - The list (separated by spaces) of the fields you wish to retrieve
- `search (s)` - The regular expression to filter and display only rows with columns that contain matching values. The matching fields will be highlighed
- `take (t)` - The number of records you wish to retrieve
- `format (p)` - The format you wish to output the Avro - omit for a pretty print as a table, or specify "csv" for CSV
