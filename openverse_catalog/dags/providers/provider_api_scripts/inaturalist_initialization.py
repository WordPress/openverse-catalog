"""
Content Provider:       Inaturalist

ETL Process:            Use inaturalist open data on S3 to access required metadata for a full initialization load.

Output:                 TSV file containing the media metadata.

Notes:                  The inaturalist API is not intended for data scraping.
                        https://api.inaturalist.org/v1/docs/
                        But there is a full dump intended for sharing on S3.
                        https://github.com/inaturalist/inaturalist-open-data/tree/documentation/Metadata

TO DO:                  Right now, one of the look-up tables of the photo table is stretching the limits of 
                        memory using pandas. Consider using postgresql instead with something like 
                            TSV -> postgres -> join -> TSV -> ImageStore (for normalization) -> normal TSV load
                        Get rid of the nrows clause when reading in the observations csv
                        Integrate ImageStore for standardizing fields
                        Consider using this to get filesize, create, and modified dates for photo files stored on 
                        S3, and to identify and mark deleted photos.
                        https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/s3/index.html#airflow.providers.amazon.aws.operators.s3.S3FileTransformOperator
                        Consider using this (GBIF Backbone Taxonomy dataset) to get additional names and tags for animals:
                        https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c
"""

from psutil import virtual_memory
print(virtual_memory())

import os
import boto3
import pandas as pd
import datetime as dt
## from common.storage.image import ImageStore

# ##########################################################################################
# print('\n ******** Download files from inaturalist-open-data on S3 ******** \n', virtual_memory())
# ##########################################################################################

# input file names, will use the same names locally
photo_file = 'photos.csv.gz'
observation_file = 'observations.csv.gz'
taxa_file = 'taxa.csv.gz'
creator_file = 'observers.csv.gz'
file_list = [photo_file, observation_file, taxa_file, creator_file]

# s3 connection and downloads
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY') )
for file_name in file_list:
    print('-->', file_name)
    with open(file_name, 'wb') as f:
        s3.download_fileobj('inaturalist-open-data', file_name, f)

# output file name for future use
output_file = 'inaturalist.csv.gz'

##########################################################################################
print('\n ******** Read in creator metadata ******** \n', virtual_memory())
##########################################################################################

creator_df = pd.read_csv(creator_file,
    header=0,
    sep='\t',
    usecols=['observer_id', 'login'],
    index_col=['observer_id']
    )
creator_df.rename(columns={'login': 'creator'}, inplace=True)
creator_df['creator_url'] = ['https://www.inaturalist.org/users/'+str(i) for i in creator_df.index]
print(creator_df.head())

##########################################################################################
print('\n ******** Read in taxa data for titles and tags ******** \n', virtual_memory())
##########################################################################################

## basic taxa table, with index and renaming
taxa_df = pd.read_csv(taxa_file, header=0, sep='\t',
    usecols=['taxon_id', 'name', 'ancestry'])
taxa_df.rename(columns={'name': 'title'}, inplace=True)
taxa_df.set_index('taxon_id', inplace=True)

## transform ancestry to support title lookup
taxa_df['ancestry'] = [ str(a).split('/')[1:5] for a in taxa_df['ancestry'] ]
exploded_df = taxa_df[['ancestry']].reset_index().explode('ancestry')
exploded_df['ancestry'] = [float(a) for a in exploded_df['ancestry'] ]

## do the lookup
exploded_df = taxa_df \
    .merge(
        exploded_df,
        how='inner',
        left_index=True,
        right_on='ancestry',
        suffixes=['', '_right']
        ) \
    .drop(columns=['ancestry', 'ancestry_right']) \
    .rename(columns={'title': 'tags'})

## aggregate tags into a string
exploded_df['tags'] = [t+'; ' for t in exploded_df['tags']]
exploded_df = exploded_df.groupby(by='taxon_id', as_index=True).sum()
taxa_df = taxa_df.join(exploded_df, how='left', on='taxon_id').drop(columns='ancestry')

print(taxa_df.head())

##########################################################################################
print('\n ******** Read in observation data ******** \n', virtual_memory())
##########################################################################################

## limiting rows for testing the rest of this script, but this has to go for longer term
observation_df = pd.read_csv(observation_file, 
    header=0, 
    sep='\t', 
    usecols=['observation_uuid', 'taxon_id'],
    index_col=['observation_uuid'],
    nrows=10**7
    )
print(observation_df.head())

##########################################################################################
print('\n ******** Set up the photorecord iterator ******** \n', virtual_memory())
##########################################################################################

photo_column_mapping = {
    'observer_id': 'observer_id',
    'photo_id': 'foreign_identifier',
    'extension': 'filetype',
    'license': 'license_info',
    'width': 'width',
    'height': 'height',
    'observation_uuid': 'observation_uuid'
}

photo_df = pd.read_csv(photo_file,
    header=0,
    sep='\t',
    usecols=photo_column_mapping.keys(),
    dtype={'extension': 'category', 'license': 'category'},
    chunksize=10000
    )

print(pd.read_csv(photo_file,
    header=0,
    sep='\t',
    usecols=photo_column_mapping.keys(),
    nrows=5
    ))

##########################################################################################
print('\n ******** Cycle through joining and writing ******** \n', virtual_memory())
##########################################################################################

# write the file header once
output_columns = ['foreign_identifier',
    'filetype', 'license_info', 'width', 'height',
    'foreign_landing_url', 'image_url',
    'creator', 'creator_url',
    'title', 'tags']
with open(output_file, 'w') as f:
    f.write('\t'.join(output_columns)+'\n')

for chunk in photo_df:
    print('Starting photo #', min(chunk.index),'at',dt.datetime.now())

    output_df = chunk.join(creator_df, on='observer_id', how='left')
    output_df = output_df.join(observation_df, on='observation_uuid', how='left')
    output_df = output_df.join(taxa_df, on='taxon_id', how='left')
    output_df.drop(columns=['observer_id', 'observation_uuid', 'taxon_id'], inplace=True)

    output_df['foreign_landing_url'] = \
        ['https://www.inaturalist.org/photos/'+str(p) for p in output_df['photo_id']]
    output_df['image_url'] = \
        ['https://inaturalist-open-data.s3.amazonaws.com/photos/'+str(p)+'/medium.'+e \
            for (p, e) in zip(output_df.photo_id, output_df.extension)]

    output_df.rename(columns=photo_column_mapping, inplace=True)

    output_df.to_csv(
        output_file,
        mode='a',
        columns=output_columns,
        header=False,
        sep='\t',
        index=False
    )

##########################################################################################
print('\n ******** All done! ******** \n', virtual_memory())
##########################################################################################
