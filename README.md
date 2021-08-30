# tap-gcs-csv

Singer tap to pull data from a Google Cloud Storage bucket.

Maintainer: FIXD Automotive
Forked from [Fishtown's `tap-s3-csv`](https://github.com/dbt-labs/tap-s3-csv)

[Singer](singer.io) tap that produces JSON-formatted data following
the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Given a configuration that specifies a bucket, a file pattern to match, a file format (`csv` or `excel`),
and a table name, this tap reads new files from Google Cloud Storage, parses them, infers a schema, and outputs the data
according to the Singer spec.

### Installation

To run locally, clone this repo, then run:

```bash
python setup.py install
```

Now you can run:

```
tap-gcs-csv --config config.json
```

to generate data.

### How it works

This tap:

 - Searches GCS for files matching the spec given.
 - Samples 1000 records out of the first five files found to infer datatypes.
 - Iterates through files from least recently modified to most recently modified, outputting data according
   to the generated schema & Singer spec.
 - After completing each file, it writes out state that can be used to enable incremental replication.

### Example

Given a source file: `gs://csv-bucket/csv-exports/today.csv`

```csv
id,First Name, Last Name
1,Michael,Bluth
2,Lindsay,Bluth Fünke
3,Tobias,Fünke
```

And a config file:

```json
{
    "credentials_path": "./client_secret.json",
    "start_date": "2017-05-01T00:00:00Z",
    "bucket": "csv-bucket",
    "tables": [
        {
            "name": "bluths",
            "pattern": "csv-exports/(.*)\\.csv$",
            "key_properties": ["id"],
            "format": "csv"
        }
    ]
}
```

An output record might look like:

```json
{
  "id": 3,
  "first_name": "Tobias",
  "last_name": "Funke",
  "_gcs_source_bucket": "csv-bucket",
  "_gcs_source_file": "csv-exports/today.csv",
  "_gcs_source_lineno": 4,
  "_gcs_extra": null
}
```

### Input File Gotchas

- Input files MUST have a header row.
- Input files MUST have cells fully populated. Missing cells will break the integration. Empty cells
  are handled by the tap.
- If you have the choice, use CSV, not Excel. This tap is able to stream CSV files from GCS row-by-row,
  but it cannot stream Excel files. CSV files are more efficient and more reliable.
- This tap can convert datetimes, but it does not infer date-time as an output datatype. If you want
  the tap to convert a field to datetime, you must specify `date-time` as the `_conversion_type` in
  `schema_overrides`. See "Configuration Format" below for more information.

### Configuration Format

See below for an exhaustive list of configuration fields:

```javascript
{
    // A path to a JSON credentials file for a service account that
    // has access to read from the bucket. 
    "credentials_path": "/path/to/client_secret.json",
    // the start date to use on the first run. the tap outputs an updated state on each
    // run which you can use going forward for incremental replication
    "start_date": "2017-05-01T00:00:00Z",

    // the bucket to use. make sure the AWS credentials provided have read access.
    "bucket": "csv-bucket",

    // table definitions. you can specify multiple tables to be pulled from a given
    // bucket.
    "tables": [
        // example csv table definition with schema overrides
        {
            // table name to output
            "name": "bluths_from_csv",

            // you can limit the paths searched in gcs if there are many files in your
            // bucket
            "search_prefix": "csv-exports",

            // pattern to match in the bucket
            "pattern": "csv-exports/(.*)\\.csv$",

            // primary key for this table. if append only, use:
            //   ["_gcs_source_file", "_gcs_source_lineno"]
            "key_properties": ["id"],

            // format, either "csv" or "excel"
            "format": "csv",

            // if the files don't have a header row, you can specify the field names
            "field_names": ["id", "first_name", "last_name"],

            // for any field in the table, you can hardcode the json schema datatype.
            // "_conversion_type" is the type that the tap will try to coerce the field
            // to -- one of "string", "integer", "number", or "date-time". this tap
            // also assumes that all fields are nullable to be more resilient to empty cells.
            "schema_overrides": {
                "id": {
                    "type": ["null", "integer"],
                    "_conversion_type": "integer"
                },

                // if you want the tap to enforce that a field is not nullable, you can do
                // it like so:
                "first_name": {
                    "type": "string",
                    "_conversion_type": "string"
                }
            }
        },

        // example excel definition
        {
            "name": "bluths_from_excel",
            "pattern": "excel-exports/(.*)\\.xlsx$",
            "key_properties": ["id"],
            "format": "excel",

            // the excel definition is identical to csv except that you must specify
            // the worksheet name to pull from in your xls(x) file.
            "worksheet_name": "Names"
        }
    ]
}
```

### Output Format

- Column names have whitespace removed and replaced with underscores.
- They are also downcased.
- A few extra fields are added for help with auditing:
  - `_gcs_source_bucket`: The bucket that this record came from
  - `_gcs_source_file`: The path to the file that this record came from
  - `_gcs_source_lineno`: The line number in the source file that this record was found on
  - `_gcs_extra`: If you specify field names in the config, and there are more records in a row than field names, the overflow will end up here.
