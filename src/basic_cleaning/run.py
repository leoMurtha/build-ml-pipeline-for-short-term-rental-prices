#!/usr/bin/env python
"""
input_artifact,output_artifact,output_type,output_description,min_price,max_price
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
    # artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    logger.info(f"Loaded raw dataframe with shape: {df.shape}")
    
    logger.info(f"Capping prices between [{args.min_price}, {args.max_price}]")
    
    # Drop outliers
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    
    # removing unwanted rows based on geolocation
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])
    
    df.to_csv("clean_sample.csv", index=False)
    
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    logger.info('Saved cleaned file: clean_sample.csv')

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning step")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='Name of the input raw file in wandb',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output cleaned file",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Output type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Output artifact description",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="min price of houses/rentals",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="max price of houses/rentals",
        required=True
    )


    args = parser.parse_args()

    go(args)
