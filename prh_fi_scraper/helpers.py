import re
from os.path import exists

from PyPDF2 import PdfReader
from jsonlines import jsonlines


def read_json_lines(input_file: str):
    if exists(input_file):
        with jsonlines.open(input_file, 'r') as ids_file:
            return [line for line in ids_file]
    return []


def format_whitespaces(input_string: str) -> str:
    if not input_string:
        return ''
    return re.sub('\s+', ' ', input_string).strip()


def get_text_from_pdf(input_pdf_file: str) -> str:
    reader = PdfReader(input_pdf_file)
    pages_text = []
    for page in reader.pages:
        current_page_text = page.extract_text()
        pages_text.append(current_page_text)
    return '\n'.join(pages_text)
