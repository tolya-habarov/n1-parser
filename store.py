from __future__ import annotations

import os
import csv
import logging
import datetime as dt
from pathlib import Path
from decimal import Decimal
from itertools import groupby
from typing import Any, Dict, List, Union

from peewee import * 
from peewee import PeeweeException

from config import config_logger


CSV_DIR = os.environ.get('CSV_DIR')
FILES = (Path(__file__).resolve().parent / CSV_DIR).glob('*.csv')

db = PostgresqlDatabase(
    os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
)
logger = logging.getLogger(__name__)


class StoreException(Exception):
    pass


class BaseModel(Model):
    @classmethod
    def insert_butch(cls, fields: List[str], rows: List[List[Any]]) -> None:
        """Model batch insert values by fields

        Args:
            fields (List[str]): Model fields
            rows (List[List[Any]]): List values
        """

        for butch in chunked(rows, 100):
            cls.insert_many(butch, fields).execute()

    @classmethod
    def fields(cls) -> List[str]:
        """Get model fields without 'id'

        Returns:
            List[str]: List fields
        """

        return list(cls._meta.fields.keys())[1:]

    class Meta:
        database = db


class OfferModel(BaseModel):
    offer_id = IntegerField(unique=True)
    url = CharField(max_length=150, unique=True)
    address = CharField(max_length=150)
    area = IntegerField()
    floor = IntegerField()
    release_year = IntegerField()
    house_material = CharField(max_length=50)
    lat = DecimalField(max_digits=17, decimal_places=15)
    lon = DecimalField(max_digits=17, decimal_places=15)

    class Meta:
        table_name = 'offers'


class PriceModel(BaseModel):
    address = ForeignKeyField(
        OfferModel,
        field='offer_id',
        related_name='prices',
        on_delete='CASCADE',
    )
    date = DateField()
    price = IntegerField()

    class Meta:
        table_name = 'prices'


class AvgPriceModel(BaseModel):
    address = CharField(max_length=150)
    date = DateField()
    avg_price = IntegerField()
    avg_price_change = IntegerField(null=True)

    @staticmethod
    def last_avg_price(address: str, end_date: dt.date) -> Union[AvgPriceModel, None]:
        """Return last average price by date for address

        Args:
            address (str): Address
            end_date (dt.date): End date

        Returns:
            Union[AvgPriceModel, None]: Last average price or None if not found.
        """

        try:
            return (
                AvgPriceModel.select()
                .where(
                    AvgPriceModel.address == address,
                    AvgPriceModel.date < end_date,
                )
                .order_by(AvgPriceModel.date)[-1]
            )
        except IndexError:
            return None
        except PeeweeException as e:
            raise StoreException(
                f'Fail get average prices for daress: {address}, date: {end_date}.'
            ) from e

    class Meta:
        table_name = 'avg_prices'


def get_exists_dates(cls: Union[PriceModel, AvgPriceModel]) -> List[dt.date]:
    """Get all dates from database

    Args:
        cls (Union[PriceModel, AvgPriceModel]): Data model

    Returns:
        List[dt.date]: List unique dates
    """

    try:
        prices = cls.select(cls.date).order_by(cls.date).distinct(cls.date)
        return [price.date for price in prices]
    except PeeweeException as e:
            raise StoreException('Error get price dates from db.') from e


def parse_raw_offer(raw_offer: Dict[str, Any]) -> Dict[str, Any]:
    """Cast string values to valid Offer fields

    Args:
        raw_offer (Dict[str, Any]): Raw value

    Returns:
        Dict[str, Any]: Offer as dict
    """

    try:
        raw_offer['offer_id'] = int(raw_offer['offer_id'])
        raw_offer['date'] = dt.datetime.strptime(raw_offer['date'], '%Y-%m-%d')
        raw_offer['area'] = int(int(raw_offer['area']) / 100)
        raw_offer['floor'] = int(raw_offer['floor'])
        raw_offer['release_date'] = int(raw_offer['release_date'])
        raw_offer['price'] = int(raw_offer['price'])
        raw_offer['lat'] = Decimal(raw_offer['lat'])
        raw_offer['lon'] = Decimal(raw_offer['lon'])
    except Exception as e:
        raise StoreException(f'Fail parse raw offer. value: {raw_offer}') from e

    return raw_offer


def read_offers(file_path: Path) -> List[Dict[str, Any]]:
    """Read offers from csv file

    Args:
        file_path (Path): File path

    Returns:
        List[Dict[str, Any]]: List Offers as dict
    """

    try:
        with open(str(file_path)) as f:
            reader = csv.DictReader(f, delimiter=';')
            return [parse_raw_offer(x) for x in reader]
    except OSError as e:
        raise StoreException(f'Fail read file {file_path}.') from e


def get_offers_rows(offers: List[Dict[str, Any]]) -> List[Any]:
    """Get new address rows

    Args:
        offers (List[Dict[str, Any]]): Offers list

    Returns:
        List[Any]: Address rows
    """
    
    offer_id = [offer['offer_id'] for offer in offers]

    try:
        exists_addresses = (
            OfferModel
                .select(OfferModel.offer_id)
                .where(OfferModel.offer_id.in_(offer_id))
        )
    except PeeweeException as e:
        raise StoreException('Error get addresses id from db.') from e

    rows = []
    fields = 'offer_id url address area floor release_date house_material lat lon'.split()
    exists_id = {address.offer_id for address in exists_addresses}

    for offer in offers:
        if offer['offer_id'] in exists_id:
            continue

        rows.append([offer.get(field) for field in fields])

    return rows


def get_price_rows(offers: List[Dict[str, Any]]) -> List[Any]:
    """Get new price rows

    Args:
        offers (List[Dict[str, Any]]): Offers list

    Returns:
        List[Any]: Price rows
    """

    rows = []
    exists_dates = set(get_exists_dates(PriceModel))
    for offer in offers:
        if offer['date'] in exists_dates:
            continue

        rows.append([offer['offer_id'], offer['date'], offer['price']])

    return rows


def get_avg_prices_rows(offers: List[Dict[str, Any]]) -> List[Any]:
    """Get new average price rows

    Args:
        offers (List[Dict[str, Any]]): Offers list

    Returns:
        List[Any]: Average price rows
    """
    avg_prices = []
    grouper = lambda x: x['address']
    for address, items in groupby(sorted(offers, key=grouper), key=grouper):
        items = list(items)
        date = items[0]['date']

        # avg price sq.m
        avg_price = sum([item['price'] / item['area'] for item in items]) / len(items)
        avg_price = round(avg_price, 2)

        # avg change
        avg_price_change = None
        if last_avg_price := AvgPriceModel.last_avg_price(address, date):
            avg_price_change = avg_price - last_avg_price.avg_price
        
        avg_prices.append([address, date, avg_price, avg_price_change])

    return avg_prices


def save_file(file_path: Path) -> None:
    """Save file to db

    Args:
        file_path (Path): File path
    """

    logger.info(f'Read file: {file_path}')
    offers = read_offers(file_path)
    logger.debug(f'Found {len(offers)} offers.')
    
    offer_rows = get_offers_rows(offers)
    logger.debug(f'Offers for storing count: {len(offer_rows)}')

    price_rows = get_price_rows(offers)
    logger.debug(f'Prices for storing count: {len(price_rows)}')

    avg_price_rows = get_avg_prices_rows(offers)
    logger.debug(f'Average prices for storing count: {len(avg_price_rows)}')

    # save data
    try:
        with db.atomic():
            OfferModel.insert_butch(OfferModel.fields(), offer_rows)
            PriceModel.insert_butch(PriceModel.fields(), price_rows)
            AvgPriceModel.insert_butch(AvgPriceModel.fields(), avg_price_rows)
        logger.info(f'Success saved file: {file_path}')
    except PeeweeException as e:
        raise StoreException('Error db on save data.') from e


def db_connect() -> None:
    """Connect to database"""

    try:
        db.connect()
        db.create_tables([OfferModel, PriceModel, AvgPriceModel])
    except PeeweeException as e:
        raise StoreException('Error on db connecting.') from e


def main():
    config_logger(logger)
    
    db_connect()
    logger.info('Start saving files...')

    to_date = lambda x: dt.datetime.strptime(x.stem, '%Y-%m-%d')
    for file_path in sorted(FILES, key=to_date):
        try:
            save_file(file_path)
        except KeyboardInterrupt:
            break
        except StoreException as e:
            logger.exception(e)
            continue
        except:
            logger.exception('Something went wrong.')


if __name__ == '__main__':
    main()
    # pass