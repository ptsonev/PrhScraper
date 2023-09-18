from logging import config

from prh_fi_scraper import PrhFiScraper
from settings import THREADS, DATA_DIR, OUTPUT_CSV_FILE, ID_FILE, ROTATING_PROXY


def main():
    config.fileConfig('log.ini')
    prh_scraper = PrhFiScraper(rotating_proxy=ROTATING_PROXY,
                               max_threads=THREADS,
                               output_csv_file=OUTPUT_CSV_FILE,
                               data_dir=DATA_DIR,
                               id_file=ID_FILE)

    all_ids = prh_scraper.scrape_ids_from_api()
    prh_scraper.download(all_ids)
    prh_scraper.parse()


if __name__ == '__main__':
    main()
