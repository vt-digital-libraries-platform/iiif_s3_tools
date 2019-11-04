# Batch Task JSON Generator

This Python script generates json files corresponding to each directory that needs to be processed by the IIIF tile generator.

Usage -
`python3 generator.py <Path_Prefix> <Collection_Name>`

The json files are generated in a directory called `json_files` with the collection name as a subdirectory. For example, running `python3 generator.py SpecScans/Women_of_Design/ Ms1994_016_Crawford` will create the json task files in `./json_files/Ms1994_016_Crawford`.

The file names contains the truncated directory name with random characters at the end in order to not cause a collision in the Batch job title.

Note that you need to have access to the `vtlib-store` bucket and the raw tiff files. Your credentials should be automatically picked up from `~/.aws/credentials`. Go [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) to find out more about creating/modifying the AWS credentials file.