# -*- coding: utf-8 -*-
"""
.. :module:: connection
   :platform: Linux
   :synopsis: Define Request Wrapper for making Api calls.

.. moduleauthor:: Ashwani Agarwal (agarw288@purdue.edu) (March 17, 2022)
"""

import requests
import logger as log
from requests.exceptions import (
    ChunkedEncodingError
)

MAX_RETRY_COUNT = 3

class RequestWrapper():
    """Define HTTP API Request Wrapper class."""

    def __init__(self):
        pass

    def make_request(self, method, url, headers=None, params=None,
                     data=None, timeout=60):
        """Invoke HTTP API call

        :param method: (HTTPMethods) http methods (eg: get, post, put, delete)
        :param url: (string) Fully qualified URL and Path.
        :param headers: (dict) HTTP request headers.
        :param params: (string) Query parameters.
        :param data: optional data valid only for PUT and POST.
        :param timeout: (int) request timeout in seconds (default 60 seconds).
        """
        retry = 0
        while True:
            try:
                retry += 1
                request = getattr(requests, method.lower())
                resp = request(url, headers=headers, data=data,
                            timeout=timeout, params=params)
                resp.raise_for_status()
            except requests.HTTPError:
                log.do_error(f"HTTP request error for {url}, status code: {resp.status_code}, error: {resp.text}")
                exception = Exception(f"HTTP request error for {url}, status code: {resp.status_code}, error: {resp.text}")
                if resp.status_code >= 500 and retry >= MAX_RETRY_COUNT:
                    raise exception
                elif resp.status_code < 500:
                    raise exception
            except (requests.ConnectionError, requests.Timeout, ChunkedEncodingError) as ex:
                log.do_error(f"HTTP request timeout for {url}, error: {repr(ex)}")
                raise ex
            except (requests.RequestException, Exception) as ex:
                log.do_error(f"HTTP request for {url} failed with general error, error: {repr(ex)}")
                raise ex
            else:
                return resp