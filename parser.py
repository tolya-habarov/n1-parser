import os
import csv
import time
import logging
import requests
import datetime as dt
from pathlib import Path
from dataclasses import dataclass, fields
from typing import Any, Dict, List, Tuple

from config import config_logger


CSV_DIR = os.environ.get('CSV_DIR')
ADDRESSES = [
    'Ядринцевская, 55',
    'Королева, 1б',
    'Гоголя, 205/1',
    'Кошурникова, 23',
    'Крылова, 64/1',
    'Державина, 50',
    'Ядринцевская, 71',
    'Закаменский микрорайон, 23',
    'Фрунзе, 252/1',
    'Гоголя, 209',
]

logger = logging.getLogger(__name__)

_headers = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'}
_city_id = '89026,89030,89027,2355612,89946,89966,89958,89978,89963,89996,\
        90006,89955,89967,89994,90027,90008,89999,89982,90004,89989,89028,90014,\
        89979,89973,90015,89975,89984,90019,89956,89912,89992,89961,89985,90007,\
        90013,89968,89969,90024,89948,89997,89976,89962,89551,89540,89951,89988,\
        89945,89974,89953,90011,90012,89892,89936,89548,89981,89957,89954,89980,\
        89585,89983,89516,89959,90020,90003,89584,89919,89572,89986,2355854,2355857,\
        89313,89995,89964,2356025,2355856,2355853,89977,2355858,90029,4848837,90032,\
        8686872,2355751,8791284,8790815,8790850,90000,8794084'
_search_params = {
    'offset': 0,
    'project_key': 'REALTY',
    'context[parent_type]': 'city',
    'context[parent_id]': _city_id,
    'limit': 25,
    'types[0]': 'street',
    'types[1]': 'house',
    'types[2]': 'district',
    'types[3]': 'microdistrict',
    'types[4]': 'metro_station',
    'types[5]': 'housing_estate',
    'fields': 'id,name_ru,name_seo,name_socr,type,abbr_raw_ru,street,params,city,area,region,location,line,params.id',
    'region_id': 1054,
}
_offers_params = {

    'limit': 25,
    'offset': 0,
    'sort': '-billing_weight,-order_date,-creation_date',
    'query[0][deal_type]': 'sell',
    'query[0][rubric]': 'flats',
    'filter[city_id]': _city_id,
    'filter[region_id]': 1054,
    'status': 'published',
}


@dataclass
class Offer:
    """Offer object"""

    offer_id: int
    date: dt.date
    url: str
    address: str
    area: int
    floor: int
    release_date: int
    price: int
    house_material: str
    lat: float
    lon: float

    @staticmethod
    def fields() -> List[str]:
        return [x.name for x in fields(Offer)]


class ParserException(Exception):
    pass


class NotFoundException(ParserException):
    pass


def search_address(query: str) -> Tuple[int, str]:
    """Search address by query

    Args:
        query (str): Query string

    Returns:
        Tuple[int, str]: Street id, house number
    """

    url = 'https://api.n1.ru/api/v1/geo/geocoder/with_cities/'
    params = _search_params.copy()
    params['q'] = query

    try:
        r = requests.get(url, params=params, headers=_headers)
        response =  r.json()

        if not 'result' in response or not response['result']:
            raise NotFoundException('Result not found or empty.')
        
        address = None
        house_number = query.split(',')[-1].strip()
        for x in response['result']:
            if x['name_ru'].lower() == house_number:
                address = x
                break
        
        if address is None:
            raise NotFoundException(f'Not found house number {house_number} in result: {response["result"]}')
        
        return address['street']['id'], address['name_ru']
    except requests.RequestException as e:
        raise ParserException(f'Fail make request. query: {query}') from e
    except NotFoundException as e:
        raise ParserException('Invalid result.') from e
    except (KeyError, IndexError) as e:
        raise ParserException(f'Fail get street id or house number. value: {response["result"]}') from e



def get_offers(street_id: int, house_number: str) -> List[Dict[str, Any]]:
    """Get offers by address

    Args:
        street_id (int): Street id
        house_number (str): House number

    Returns:
        List[Dict[str, Any]]: Offers
    """

    url = 'https://api.n1.ru/api/v1/offers/'
    params = _offers_params.copy()
    params['filter_or[addresses][0][street_id]'] = street_id
    params['filter_or[addresses][0][house_number]'] = house_number
    offset, count, offers = 0, 1, []

    while offset < count: # while do
        try:
            r = requests.get(url, params=params, headers=_headers)
            response = r.json()
            count = response['metadata']['resultset']['count']
        except requests.RequestException as e:
            raise ParserException(
                f'Fail make request. street_id: {street_id}, house_number: {house_number}'
            ) from e
        except KeyError as e:
            raise ParserException('It was not possible to get the number of offers') from e
        
        offers.extend(response.get('result', []))
        offset += 25
        time.sleep(0.5)
    
    return offers


def parse_raw_offer(raw_offer: Dict[str, Any]) -> Offer:
    """Parse raw data to offer object

    Args:
        raw_offer (Dict[str, Any]): Raw data

    Raises:
        Exception: If KeyError or IndexError

    Returns:
        Offer: Offer object
    """

    try:
        params = raw_offer['params']
        raw_address = params['house_addresses'][0]

        return Offer(
            offer_id=raw_offer['_id'],
            date=dt.date.today(),
            url='https:' + raw_offer['url'],
            address=f'{raw_address["street"]["name_ru"]}, {raw_address["house_number"]}',
            area=params['total_area'],
            floor=params['floor'],
            release_date=params['release_date']['year'],
            price=params['price'],
            house_material=params['house_material_type']['title'],
            lat=params['location']['lat'],
            lon=params['location']['lon'],
        )
    except (KeyError, IndexError) as e:
        raise ParserException(f'Fail parse raw offer. value: {raw_offer}') from e


def save_offers(offers: List[Offer]) -> Path:
    """Save offer list to file

    Args:
        offers (List[Offer]): Offers list
    
    Returns:
        Path: File path
    """

    directory = Path(__file__).resolve().parent / CSV_DIR

    if not directory.exists():
        try:
            os.mkdir(str(directory))
        except OSError  as e:
            raise ParserException(f'Fail create dicrectory. value: {directory}') from e
    
    path = directory / f'{dt.date.today()}.csv'
    write_header = not path.exists()
    try:
        with open(path, 'a') as f:
            writer = csv.DictWriter(f, fieldnames=Offer.fields(), delimiter=';')
            
            if write_header:
                writer.writeheader()
            writer.writerows(map(vars, offers))
        
        return path
    except OSError  as e:
        raise ParserException(f'Fail write to file. path: {path}') from e


def parse_by_raw_address(address: str) -> None:
    """Parse offers pipeline

    Args:
        address (str): Raw addres
    """

    logger.info(f'Search address: {address}')
    street_id, house_number = search_address(address)
    logger.debug(f'Found street id: {street_id}, house number: {house_number}')

    raw_offers = get_offers(street_id, house_number)
    logger.debug(f'Raw offers cound {len(raw_offers)}')

    offers = [parse_raw_offer(x) for x in raw_offers]
    if offers:
        logger.debug(f'Parsed offers count {len(raw_offers)}')
    else:
        logger.warning('Offers not found.')
        return

    offers = {offer.offer_id: offer for offer in offers} # unique offers
    path = save_offers(list(offers.values()))
    logger.info(f'Writed to {path}')


def main():
    config_logger(logger)
    logger.info('Start parsing...')

    for address in ADDRESSES:
        try:
            parse_by_raw_address(address)
        except KeyboardInterrupt:
            break
        except ParserException as e:
            logger.exception(e)
            continue
        except:
            logger.exception('Something went wrong.')


if __name__ == '__main__':
    main()
