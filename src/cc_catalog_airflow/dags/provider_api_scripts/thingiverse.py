"""
Content Provider:       Thingiverse

ETL Process:            Use the API to identify all CC0 3D Models.

Output:                 TSV file containing the 3D models, their respective images and meta-data.

Notes:                  https://www.thingiverse.com/developers/getting-started
                        All API requests require authentication.
                        Rate limiting is 300 per 5 minute window.
"""
import argparse
from datetime import datetime, timedelta
import json
import logging
import os
import time

from modules.etlMods import request_content, delay_processing, sanitize_string, write_to_file

MAX_THINGS = 30
LICENSE = 'pd0'
TOKEN = os.environ['THINGIVERSE_TOKEN']
DELAY = 5.0  # seconds
FILE = 'thingiverse_{}.tsv'.format(int(time.time()))


logging.basicConfig(
    format='%(asctime)s: [%(levelname)s - Thingiverse API] =======> %(message)s', level=logging.INFO)


def request_batch_things(_page):

    url = 'https://api.thingiverse.com/newest?access_token={1}&per_page={2}&page={0}'.format(
        _page, TOKEN, MAX_THINGS)

    result = request_content(url)
    if result:
        return map(lambda x: x['id'], result)

    return None


def get_metadata(_thing, _date):

    url = 'https://api.thingiverse.com/things/{0}?access_token={1}'.format(
        _thing, TOKEN)
    license_text = 'Creative Commons - Public Domain Dedication'
    creator = None
    creator_url = None
    extracted = []

    logging.info('Processing thing: {}'.format(_thing))

    result = request_content(url)
    if result:

        # verify the date
        mod_date = result.get('modified', '')
        if mod_date:
            mod_date = mod_date.split('T')[0].strip()
            if datetime.strptime(mod_date, '%Y-%m-%d') < datetime.strptime(_date, '%Y-%m-%d'):
                return '-1'

        start_time = time.time()

        # validate CC0 license
        if not (('license' in result) and (license_text.lower() in result['license'].lower())):
            logging.warning(
                'License not detected => https://www.thingiverse.com/thing:{}'.format(_thing))
            delay_processing(start_time, DELAY)
            return None
        else:
            license_ = 'CC0'
            version = '1.0'

        # get meta data

        # description of the work
        description = sanitize_string(result.get('description', ''))

        # title for the 3D model
        title = sanitize_string(result.get('name', ''))

        # the landing page
        if 'public_url' in result:
            foreign_url = result['public_url'].strip()
        else:
            foreign_url = 'https://www.thingiverse.com/thing:{}'.format(_thing)

        # creator of the 3D model
        if 'creator' in result:
            if ('first_name' in result['creator']) and ('last_name' in result['creator']):
                creator = '{} {}'.format(
                    sanitize_string(result['creator']['first_name']),
                    sanitize_string(result['creator']['last_name']))

            if (creator.strip() == '') and ('name' in result['creator']):
                creator = sanitize_string(result['creator']['name'])

            if 'public_url' in result['creator']:
                creator_url = result['creator']['public_url'].strip()

        # get the tags
        delay_processing(start_time, DELAY)
        logging.info('Requesting tags for thing: {}'.format(_thing))
        start_time = time.time()
        tags = request_content(url.replace(_thing, '{0}/tags'.format(_thing)))
        tags_list = None

        if tags:
            tags_list = list(map(lambda tag: {'name': str(
                tag['name'].strip()), 'provider': 'thingiverse'}, tags))

        # get 3D models and their respective images
        delay_processing(start_time, DELAY)
        logging.info('Requesting images for thing: {}'.format(_thing))

        image_list = request_content(
            url.replace(_thing, '{}/files'.format(_thing)))
        if image_list is None:
            logging.warning('Image Not Detected!')
            delay_processing(start_time, DELAY)
            return None

        for img in image_list:
            meta_data = {}
            thumbnail = None
            image_url = None

            meta_data['description'] = description

            if ('default_image' in img) and img['default_image']:
                if 'url' not in img['default_image']:
                    logging.warning('3D Model Not Detected!')
                    continue

                meta_data['3d_model'] = img['default_image']['url']
                foreign_id = str(img['default_image']['id'])
                images = img['default_image']['sizes']

                for image_size in images:

                    if str(image_size['type']).strip().lower() == 'display':

                        if str(image_size['size']).lower() == 'medium':
                            thumbnail = image_size['url'].strip()

                        if str(image_size['size']).lower() == 'large':
                            image_url = image_size['url'].strip()

                        elif image_url is None:
                            image_url = thumbnail

                    else:
                        continue

                if image_url is None:
                    logging.warning('Image Not Detected!')
                    continue

                extracted.append([
                    image_url if not foreign_id else foreign_id,
                    foreign_url,
                    image_url,
                    thumbnail,
                    '\\N',
                    '\\N',
                    '\\N',
                    license_,
                    str(version),
                    creator,
                    creator_url,
                    title,
                    '\\N' if not meta_data else json.dumps(meta_data),
                    '\\N' if not tags_list else json.dumps(tags_list),
                    'f',
                    'thingiverse',
                    'thingiverse'])

        write_to_file(extracted, FILE)

        return len(extracted)


def execute_job(_date):
    page = 1
    result = 0
    is_valid = True
    tmp_counter = 0

    while is_valid:  # temporary control flow

        batch = request_batch_things(page)

        if batch:
            batch = list(batch)
            tmp = list(
                filter(None, list(map(lambda thing: get_metadata(str(thing), _date), batch))))

            if '-1' in tmp:
                is_valid = False
                tmp = tmp.remove('-1')

            if tmp:
                tmp_counter = sum(tmp)

            result += tmp_counter
            tmp_counter = 0

        page += 1

    logging.info('Total CC0 3D Models: {}'.format(result))


def main():
    logging.info('Begin: Thingiverse API requests')
    mode = 'date: '

    parser = argparse.ArgumentParser(
        description='Thingiverse API Job', add_help=True)
    parser.add_argument('--mode', choices=['default', 'newest'],
                        help='Identify all CC0 3D models from the previous day [default] or the current date [newest].')

    args = parser.parse_args()
    if args.mode:

        if str(args.mode) == 'newest':
            param = datetime.strftime(datetime.now(), '%Y-%m-%d')
        else:
            param = datetime.strftime(
                datetime.now() - timedelta(1), '%Y-%m-%d')

        mode += param if param is not None else ''
        logging.info('Processing {}'.format(mode))

        if param:
            execute_job(param)

    logging.info('Terminated!')


if __name__ == '__main__':
    main()
