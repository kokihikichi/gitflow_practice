# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv
from dsdtools import build_hiveserver2_session
import pickle


@click.command()
@click.argument('output_filepath', type=click.Path())
def main(output_filepath):
    """ Queries HIVE to download raw sequences for training.
    (saved in ../data/raw).
    """
    logger = logging.getLogger(__name__)
    logger.info('creating hive context')
    hive_context = build_hiveserver2_session()
    query = """
    select
        *
    from 
        prabhakarbha01.log_rat_agg_sess
    where
        cnt > 4
        and cnt <= 75
    """
    logger.info('querying')
    hive_context.execute(query)
    raw_data = hive_context.fetchall()
    logger.info('downloaded data')
    with open(output_filepath, "wb") as handle:
        pickle.dump(raw_data, handle)
    print("No of datapoints saved - {:d}".format(len(raw_data)))
    logger.info("done")
    


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
#     project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
