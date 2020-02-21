# Index CSV Generator

This Python script scans a source s3 bucket and creates an `index.csv` file which will contain a list of the csv files contained in the source bucket as well as the paths to all `manifest.json` files associated with each csv file.

Usage -
`python3 index_csv_generator.py <source s3 bucket> <source directory> <target s3 bucket> <target directory>`


The file created will be created as:
`<target s3 bucket>/<target directory>/index.csv`

Note that you will need to have access to the source/target buckets that you specify.
Your credentials should be automatically picked up from `~/.aws/credentials`. Go [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) to find out more about creating/modifying the AWS credentials file.