import json
import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd


working_dir = Path(__file__).parent
logger = logging.getLogger(__name__)

PROVIDERS = {
    "SIA": "smithsonian_institution_archives",
    "NZP": "smithsonian_zoo_and_conservation",
    "FBR": "smithsonian_field_book_project",
    "NAA": "smithsonian_anthropological_archives",
}

# taken from https://gist.github.com/gruber/8891611, and adapted for edu only
URL_REGEX = r"(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.]edu/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:edu)\b/?(?!@)))"  # noqa


def get_url_paths(tree, url_regex=URL_REGEX):
    if isinstance(tree, dict):
        return [
            f"{key}.{result}"
            for (key, value) in tree.items()
            for result in get_url_paths(value)
        ]
    elif isinstance(tree, list):
        return [
            f"{idx}.{result}"
            for (idx, value) in enumerate(tree)
            for result in get_url_paths(value)
        ]
    elif isinstance(tree, str):
        return [f"|{url}" for url in re.findall(url_regex, tree)]
    else:
        return []


# cp_sample_00 = "aws s3 cp s3://smithsonian-open-access/metadata/edan/{}/00.txt {}_00.json" # noqa
# cp_sample_ff = "aws s3 cp s3://smithsonian-open-access/metadata/edan/{}/ff.txt {}_ff.json" # noqa
# cp_index = "aws s3 cp s3://smithsonian-open-access/metadata/edan/{}/index.txt index_{}.txt" # noqa
# for (provider, provider_name) in PROVIDERS.items():
#     print( cp_sample_00.format(provider.lower(), provider) )
#     print( cp_sample_ff.format(provider.lower(), provider) )
#     # print( cp_index.format(provider.lower(), provider) )


all_urls = []
for f in working_dir.glob("*.txt"):
    provider = str(f).split("/")[-1][:-7]
    provider_name = PROVIDERS[provider]
    with open(f) as s3_file:
        for line in s3_file:
            json_row = json.loads(line.strip())
            urls = get_url_paths(json_row)
            all_urls += [
                {
                    "provider_name": provider_name,
                    "provider": provider,
                    "json_path": result.split("|")[0],
                    "url": result.split("|")[1],
                }
                for result in urls
            ]

url_df = pd.DataFrame.from_records(all_urls)
print(url_df.provider_name.value_counts())
print(url_df.head(10))

test_paths = (
    url_df.groupby(["provider", "json_path"], as_index=False)["url"]
    .agg([np.size, min, max])
    .rename(columns={"size": "num_urls", "min": "sample1", "max": "sample2"})
)
test_paths = test_paths.loc[test_paths.num_urls > 2]
test_paths.to_csv(working_dir / "test_path_results.csv")
