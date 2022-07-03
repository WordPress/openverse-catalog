"""
This is really just a one time thing I used to generate the sample files, once I had
identified some photo IDs that I thought were worth using for testing. I'm not sure it
actually belongs in the on-going code base at all, and if it does, it needs a bunch of
clean-up and a better location in the file structure.
"""

import csv
import gzip
import os
from datetime import datetime as dt

import pandas as pd


def pull_sample_records(file_name, file_path, id_name, id_list):
    """
    Reads through a full gzip file and keeps just the selected ID records
    """
    # can we read the stuff? (should assert that id_name is a valid field name, but...)
    assert len(id_list) > 0
    working_input_file = "./" + file_path + "/" + file_name
    assert os.path.exists(working_input_file)
    # set up where we'll write results
    if file_path == ".":
        output_file_name = "sample_records_" + file_name
    else:
        output_file_name = file_name
    # read in the selected records
    sample_records = []
    print(
        "Starting to read",
        working_input_file,
        "at",
        dt.now().strftime("%d/%m/%Y %H:%M:%S"),
    )
    with gzip.open(working_input_file, "rt") as in_file:
        records = csv.DictReader(in_file, delimiter="\t")
        lines_read = 0
        for record in records:
            if record[id_name] in id_list:
                sample_records += [record]
            lines_read += 1
            if lines_read % 10**7 == 0:
                print(
                    "Read line number",
                    lines_read,
                    "at",
                    dt.now().strftime("%d/%m/%Y %H:%M:%S"),
                )
    # write them to the sample output file
    sample_df = pd.DataFrame(sample_records)
    with gzip.open(output_file_name, "wt") as out_file:
        sample_df.to_csv(out_file, sep="\t", header=True, index=False)
    print(len(sample_df), "sample records saved in", output_file_name)
    return output_file_name


def get_sample_id_list(sample_file, joined_on):
    """
    sample_file:    from a table that you have already drawn sample records
                    a file object so that you can handle if it's compressed or not, set
                    up to read text
    joined_on:      the string name of the field they have in common
    """
    records = csv.DictReader(sample_file, delimiter="\t")
    sample_values = set()
    for record in records:
        sample_values.add(record[joined_on])
    return list(sample_values)


if __name__ == "__main__":

    # PHOTOS
    photo_ids = [
        str(i)
        for i in [
            20314159,
            10314159,
            30314159,
            40314159,
            60314159,
            80314159,
            90314159,
            110314159,
            120314159,
            130314159,
            150314159,
            160314159,
            170314159,
            191019692,
            191018903,
            191018870,
            191016506,
            191015547,
            191015484,
            191012628,
            191001500,
            190995656,
            190995604,
            191028942,
            191028931,
            191024617,
        ]
    ]
    pull_sample_records("photos.csv.gz", "full_june_2022", "photo_id", photo_ids)

    # ASSOCIATED OBSERVATIONS
    with gzip.open("photos.csv.gz", "rt") as photo_output:
        sample_observations = get_sample_id_list(photo_output, "observation_uuid")
    pull_sample_records(
        "observations.csv.gz", "full_june_2022", "observation_uuid", sample_observations
    )

    # ASSOCIATED OBSERVERS
    with gzip.open("photos.csv.gz", "rt") as photo_output:
        sample_observers = get_sample_id_list(photo_output, "observer_id")
    pull_sample_records(
        "observers.csv.gz", "full_june_2022", "observer_id", sample_observers
    )

    # ASSOCIATED TAXA (including ancestry for photo tags)
    with gzip.open("observations.csv.gz", "rt") as observation_output:
        sample_taxa = get_sample_id_list(observation_output, "taxon_id")
    sample_taxa_with_ancestors = set(sample_taxa)
    with gzip.open("full_june_2022/taxa.csv.gz", "rt") as all_taxa:
        records = csv.DictReader(all_taxa, delimiter="\t")
        for t in records:
            if t["taxon_id"] in sample_taxa:
                sample_taxa_with_ancestors.update(t["ancestry"].split("/"))
    pull_sample_records(
        "taxa.csv.gz", "full_june_2022", "taxon_id", list(sample_taxa_with_ancestors)
    )

    # PRINT RESULTS
    for f in [
        "photos.csv.gz",
        "observations.csv.gz",
        "observers.csv.gz",
        "taxa.csv.gz",
    ]:
        print("=" * 80)
        print("====> Displaying", f)
        with gzip.open(f, "rt") as unzipped:
            for line in unzipped:
                print(line.strip())
