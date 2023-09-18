import csv
import json
import logging
import os.path
import signal
import uuid
from concurrent.futures import ThreadPoolExecutor
from os.path import exists
from time import sleep
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from httpx import TimeoutException, ProxyError, Client, Response, RemoteProtocolError, HTTPStatusError
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from prh_fi_scraper.helpers import read_json_lines, format_whitespaces, get_text_from_pdf
from prh_fi_scraper.httpx_util import get_httpx_session, DEFAULT_FORCELIST
from prh_fi_scraper.scraper_exceptions import RateLimitError, CompanyNotFound


class PrhFiScraper:
    def __init__(self, rotating_proxy: str, output_csv_file: str, data_dir: str, id_file: str, max_threads: int = 1):

        self.max_threads = max_threads

        self.rotating_proxy = rotating_proxy

        self.html_dir = os.path.join(data_dir, 'HTML')
        self.pdf_dir = os.path.join(data_dir, 'PDF')
        if not exists(self.pdf_dir):
            os.makedirs(self.pdf_dir)
        if not exists(self.html_dir):
            os.makedirs(self.html_dir)
        self.id_file = id_file
        self.output_csv_file = output_csv_file

        self._progress_bar: tqdm = None
        self._input_queue = list()

        self.is_stop_requested = False
        signal.signal(signal.SIGINT, self._handle_interrupt)

    def download(self, input_ids: list[str]):

        if self.is_stop_requested:
            return

        self._input_queue.clear()
        downloaded_ids = {f.replace('.pdf', '') for f in os.listdir(self.pdf_dir)}
        self._input_queue.extend([biz_id for biz_id in input_ids if biz_id not in downloaded_ids])

        if not self._input_queue:
            logging.info('All files are already downloaded.')
            return

        with logging_redirect_tqdm():
            with tqdm(unit='id(s)', desc='Downloading the HTML and PDF files', total=len(self._input_queue), position=0, leave=True, dynamic_ncols=True) as self._progress_bar:
                with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                    futures = [executor.submit(self._download_worker) for _ in range(0, self.max_threads)]
                    while any(not future.done() for future in futures):
                        sleep(1)

    def _download_worker(self):
        PDF_URL = 'https://virre.prh.fi/novus/reportdisplay'
        SEARCH_URL = 'https://virre.prh.fi/novus/companySearch'

        current_id = ''
        httpx_client: Client = None

        while self._input_queue and not self.is_stop_requested:
            try:
                try:
                    current_id = str(self._input_queue.pop())
                except IndexError:
                    return

                if httpx_client is None:
                    next_proxy = self._get_next_proxy()
                    httpx_client = get_httpx_session(proxy=next_proxy, timeout=45)

                init_response = httpx_client.get(SEARCH_URL)
                bs_object = self._load_bs_object_from_response(init_response)
                csrf_token, execution = self._parse_csrf_and_execution(bs_object)

                search_post_data = {
                    'execution': execution,
                    'registrationNumber': '',
                    'nameStateCode': '',
                    'name': '',
                    'companyStateCode': '',
                    '_companyFormCode': '1',
                    '_domicileCode': '1',
                    'businessId': current_id,
                    '_csrf': csrf_token,
                    '_eventId_search': 'Hae',
                    '_exactNameMatch': 'on',
                }
                search_response = httpx_client.post(SEARCH_URL, data=search_post_data)
                bs_object = self._load_bs_object_from_response(search_response)

                company_tag = bs_object.select_one('a[href*="companyId="]')
                if company_tag is None:
                    raise CompanyNotFound(current_id)

                company_url = urljoin(SEARCH_URL, company_tag.get('href'))
                company_response = httpx_client.get(company_url)
                bs_object = self._load_bs_object_from_response(company_response)
                csrf_token, execution = self._parse_csrf_and_execution(bs_object)

                if bs_object.select(f'td:-soup-contains("{current_id}")') is None:
                    raise Exception('Invalid HTML.')

                pdf_post_data = {
                    'execution': execution,
                    '_csrf': csrf_token,
                    '_eventId_createElectronicTRExtract': ''
                }
                httpx_client.post(SEARCH_URL, data=pdf_post_data)
                company_pdf_data = httpx_client.get(PDF_URL).content

                if company_pdf_data[0:5].decode('utf-8') != '%PDF-':
                    raise Exception('Invalid PDF.')

                company_html_file = os.path.join(self.html_dir, current_id) + '.html'
                with open(company_html_file, 'w', encoding='utf-8') as f:
                    f.write(company_response.text)

                company_pdf_file = os.path.join(self.pdf_dir, current_id) + '.pdf'
                with open(company_pdf_file, 'wb') as f:
                    f.write(company_pdf_data)

                self._progress_bar.update(1)

            except (ProxyError, RateLimitError) as ex:
                if '403' in str(ex) or '407' in str(ex):
                    logging.critical('Proxy Authentication error. Please make sure the username and the password are correct. | The thread will now exit.')
                    return

                # get a new session(new proxy) if something happens with the proxy or the website rate limits the IP
                httpx_client.close()
                httpx_client = None

                self._input_queue.append(current_id)

            except HTTPStatusError as ex:
                if ex.response.status_code in DEFAULT_FORCELIST:
                    self._input_queue.append(current_id)
                else:
                    # this shouldn't happen, but I am leaving it just in case something changes in the future.
                    logging.critical('{} | The thread will now exit.'.format(ex), stack_info=False, exc_info=False)
                    return

            except Exception as ex:
                if not isinstance(ex, CompanyNotFound):
                    self._input_queue.append(current_id)
                else:
                    self._progress_bar.update(1)

                if not isinstance(ex, (TimeoutException, RemoteProtocolError)):
                    logging.exception('{} | {}'.format(ex.__class__.__name__, ex), stack_info=False, exc_info=False)

    def scrape_ids_from_api(self) -> list[str]:
        """
        I had to segment the API by year, because it has 30 seconds timeout
        and after reaching a certain point, around 250000 IDs, it just timeouts indefinitely.
        :return:
        """

        if self.is_stop_requested:
            return

        API_URL = 'https://avoindata.prh.fi/tr/v1'

        STEP = 100
        # there are no records before 1896
        MIN_YEAR = 1896
        MAX_YEAR = 2024
        RETRIES = 15

        completed_years = [line['year'] for line in read_json_lines(self.id_file)]
        years_to_scrape = [year for year in range(MIN_YEAR, MAX_YEAR) if year not in completed_years]

        if not years_to_scrape:
            logging.info('All IDs are already scraped. Delete the ids.jsonl file if you want to re-scrape them.')
        else:
            with get_httpx_session() as httpx_session:
                with logging_redirect_tqdm():
                    for current_year in tqdm(years_to_scrape, desc='Scraping the business IDs from the API', unit='year(s)', position=0, leave=True, dynamic_ncols=True):
                        try:
                            # totalResults=false is faster and seems less buggy, but we don't know the total number of results.
                            # so we scrape until we got empty results
                            current_business_ids = {
                                'year': current_year,
                                'ids': []
                            }

                            for current_offset in range(0, 500_000, STEP):
                                search_data = {
                                    'totalResults': 'false',
                                    'maxResults': STEP,
                                    'resultsFrom': current_offset,
                                    'companyRegistrationFrom': f'{current_year}-01-01',
                                    'companyRegistrationTo': f'{current_year + 1}-01-01'
                                }
                                for retry in range(1, RETRIES):
                                    try:
                                        if self.is_stop_requested:
                                            return
                                        json_data = httpx_session.get(API_URL, params=search_data).json()
                                    except:
                                        if retry == RETRIES - 1:
                                            raise
                                    else:
                                        break

                                current_ids = [result['businessId'].strip() for result in json_data['results']]
                                current_business_ids['ids'].extend(current_ids)

                                if len(current_ids) < STEP:
                                    break

                            current_line = json.dumps(current_business_ids) + '\n'
                            with open(self.id_file, 'a') as ids_file:
                                ids_file.write(current_line)

                        except Exception as ex:
                            logging.exception('{} | {}'.format(ex.__class__.__name__, ex), stack_info=False, exc_info=False)

        return list({current_id for item in read_json_lines(self.id_file) for current_id in item['ids']})

    def parse(self):
        if self.is_stop_requested:
            return

        column_headers = [
            'CompanyId',
            'Nimi',
            'Y-tunnus',
            'Kaupparekisterinumero',
            'Kotipaikka',
            'Yritysmuoto',
            'Yrityksen kieli',
            'Rekisteröintiajankohta',
            'Viimeisin rekisteröintiajankohta',
            'Yrityksen tiedot järjestelmässä alkaen',
            'Yrityksen tila',
            'Yrityksellä on yrityskiinnityksiä',
            'Yhteystiedot',
            'Yrityksellä on edunsaajatietoja',
            'Postiosoite',
            'Käyntiosoite',
            'Puhelin',
            'Sähköposti',
            'Faksi',
            'Www',
            'Lakkaamisajankohta',
            'Yrityksen tilanne',
            'PDF Text',
        ]

        completed_ids = {}
        write_header = True
        if exists(self.output_csv_file):
            write_header = False
            with open(self.output_csv_file, mode='r', newline='', encoding='utf-8') as csv_file:
                csv_reader = csv.DictReader(csv_file, fieldnames=column_headers)
                completed_ids = {line['Y-tunnus'] for line in csv_reader}

        input_html_files = [f for f in os.listdir(self.html_dir) if f.endswith('.html') and f.replace('.html', '') not in completed_ids]

        if not input_html_files:
            logging.info('All files are already parsed.')
            return

        with logging_redirect_tqdm():
            with open(self.output_csv_file, mode='a', newline='', encoding='utf-8') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=column_headers)

                if write_header:
                    csv_writer.writeheader()

                for html_file in tqdm(input_html_files, desc='Parsing the HTML and PDF files', unit='file(s)', position=0, leave=True, dynamic_ncols=True):
                    if self.is_stop_requested:
                        return
                    try:

                        with open(os.path.join(self.html_dir, html_file), 'r', encoding='utf-8') as f:
                            html = f.read()

                        bs_object = BeautifulSoup(html, 'html.parser')
                        company_id = bs_object.select_one('input[name="companyId"]').get('value')
                        data_rows = bs_object.select('table.table-bordered tbody tr')
                        current_csv_line = dict.fromkeys(column_headers)
                        current_csv_line['CompanyId'] = company_id
                        for data_row in data_rows:
                            header = format_whitespaces(data_row.select_one('td:nth-of-type(1)').text)
                            value = format_whitespaces(data_row.select_one('td:nth-of-type(2)').text)

                            if header not in column_headers:
                                continue

                            current_csv_line[header] = value

                        pdf_file_name = os.path.join(self.pdf_dir, html_file.replace('.html', '.pdf'))
                        current_csv_line['PDF Text'] = get_text_from_pdf(pdf_file_name)

                        csv_writer.writerow(current_csv_line)

                    except Exception as ex:
                        logging.exception('{} | {}'.format(ex.__class__.__name__, ex), stack_info=False, exc_info=False)

    def _get_next_proxy(self) -> str:
        random_string = str(uuid.uuid4()).replace('-', '')
        return self.rotating_proxy.format(session=random_string)

    @staticmethod
    def _load_bs_object_from_response(response: Response) -> BeautifulSoup:
        RATE_LIMIT_ERROR = 'Huomaa, että tänä aikana voi kuitenkin käyttää prh.fi-sivuja normaalisti.'
        if RATE_LIMIT_ERROR in response.text:
            raise RateLimitError()
        return BeautifulSoup(response.text, 'html.parser')

    @staticmethod
    def _parse_csrf_and_execution(bs_object: BeautifulSoup) -> tuple[str, str]:
        csrf_token = bs_object.select_one('input[name="_csrf"]').get('value')
        execution = bs_object.select_one('input[name="execution"]').get('value')
        return csrf_token, execution

    def _handle_interrupt(self, signum, frame):
        logging.info('CTR+C was pressed. The script will now exit.')
        self.is_stop_requested = True
