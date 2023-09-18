import httpx
from httpx import Response

DEFAULT_FORCELIST = [400, 401, 402, 405, 406, 408, 429, 500, 501, 502, 503, 504]

DEFAULT_REQUEST_HEADERS = {
    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}


def get_httpx_session(proxy: str = None,
                      timeout: int = 45,
                      follow_redirects: bool = True,
                      raise_on_status: bool = True) -> httpx.Client:
    transport = httpx.HTTPTransport(retries=5)
    if proxy:
        httpx_client = httpx.Client(proxies=proxy, transport=transport, follow_redirects=follow_redirects, verify=False)
    else:
        httpx_client = httpx.Client(transport=transport, follow_redirects=follow_redirects, verify=False)

    if raise_on_status:
        httpx_client.event_hooks['response'].append(response_hook)

    httpx_client.encoding = 'utf-8'
    httpx_client.headers = DEFAULT_REQUEST_HEADERS.copy()
    httpx_client.timeout = timeout

    return httpx_client


def response_hook(response: Response, *args, **kwargs):
    ignore_status_codes = [301, 302, 307, 308]  # redirect codes
    if response.status_code not in ignore_status_codes:
        return response.raise_for_status()
    return response
