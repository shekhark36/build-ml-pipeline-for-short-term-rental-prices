#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    logger.info('input_artifact_received')

    # reading input_artifact
    df = pd.read_csv(artifact_local_path)

    # filtering outliers for price
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # converting last_review to datetime type
    df['last_review'] = pd.to_datetime(df['last_review'])
    logger.info('basic_cleaning done')

    # adding to fix test_proper_boundaries failure
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    # saving cleaned data as clean_sample.csv file
    df.to_csv('clean_sample.csv', index=False)
    logger.info('Dataframe saved to csv')
    
    # uploading output_artifact to W&B
    artifact = wandb.Artifact(name= args.output_artifact,
    	 type=args.output_type,
   	 description=args.output_description)

    artifact.add_file("clean_sample.csv")

    run.log_artifact(artifact)
    logger.info('output_artifact uploaded to W&B')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='the input artifact',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='the name for the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='the type for the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help='a description for the output artifact',
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help='the minimum price to consider',
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help='the maximum price to consider',
        required=True
    )


    args = parser.parse_args()

    go(args)
