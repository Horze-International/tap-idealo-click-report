import io
from logging import Logger
from types import resolve_bases
import requests
from requests.api import head
from singer import metrics
import backoff
import singer
import time
import json
import zipfile

BASE_URL = "https://businessapi.idealo.com/"

LOGGER = singer.get_logger()

class RateLimitException(Exception):
    pass


class Client:
    def __init__(self, config):
        # self.session = requests.Session()
        self.client_id = config.get("client_id")
        self.client_password = config.get("client_password")
        self.shop_id = config.get("shop_id")
        self.site = config.get("site")
        self.user_agent = config.get("user_agent")

        self.access_token = self.authorize(self)
        self.session = requests.Session()

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def authorize(self, params={}, url=None, url_extra=""):
        #returns a token by logging in to the ideoalo API

        with metrics.http_request_timer("authorize") as timer:
            url = BASE_URL + "api/v1/" + "oauth/token"
            headers = {'content-type': 'application/json'}
            if self.user_agent:
                headers["User-Agent"] = self.user_agent
            response = requests.post(url, auth=(self.client_id, self.client_password), headers = headers)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 502]:
            raise RateLimitException()
        response.raise_for_status()
        return response.json()["access_token"]

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def download_request(self, start_date, end_date):
        #returns a datastream for the csv File
        with metrics.http_request_timer("create_report") as timer:
            url = BASE_URL + "api/v1/shops/" + self.shop_id + "/" + "click-reports"
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer " + self.access_token,
            }
            data = {
                "from": start_date,
                "to": end_date,
                "site": self.site
            }
            response = requests.post(url, headers=headers, data=json.dumps(data))
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 502]:
            raise RateLimitException()
        if("The date should be in the past" in response.text): #checks whether the end date was set correctly
            return -1
        response.raise_for_status()
        LOGGER.debug(response.json())
        status = response.json()["status"]
        report_id = response.json()["id"]

        # file was requested now poll for availability
        while status == "PROCESSING": #maybe a timeout for 1 minute or sth. similar
            LOGGER.info("Check whether the report is online")
            with metrics.http_request_timer("poll_report status") as timer:
                url = BASE_URL + "api/v1/shops/" + self.shop_id + "/" + "click-reports" + "/" + report_id
                headers = {"Authorization": "Bearer " + self.access_token,}
                if self.user_agent:
                    headers["User-Agent"] = self.user_agent
                request = requests.Request("GET", url, headers=headers)
                response = self.session.send(request.prepare())
                timer.tags[metrics.Tag.http_status_code] = response.status_code
            if response.status_code in [429, 502]:
                raise RateLimitException()
            response.raise_for_status()
            LOGGER.debug(response.json())
            status = response.json()["status"]
            if status == "PROCESSING":
                LOGGER.info("waiting 1 sec")
                time.sleep(1)
        
        #it is no longer processing - so either FAILDED or SUCCESSFUL
        if status == "FAILED":
            raise Exception()
        else: ## if successful
            ##download the code
            with metrics.http_request_timer("download_status") as timer:
                url = BASE_URL + "api/v1/shops/" + self.shop_id + "/" + "click-reports" + "/" + report_id + "/download"
                headers = {"Authorization": "Bearer " + self.access_token}
                request = requests.Request("GET", url, headers=headers)
                response = self.session.send(request.prepare())
                timer.tags[metrics.Tag.http_status_code] = response.status_code
            if response.status_code in [429, 502]:
                raise RateLimitException()
            response.raise_for_status()

            #extract the files inside the zipfile
            zf = zipfile.ZipFile(io.BytesIO(response.content), 'r')

            for filename in zf.namelist():
                try:
                    LOGGER.info("read File %s" % filename)
                    return io.StringIO(zf.read(filename).decode())
                except KeyError:
                    LOGGER.critical('ERROR: Did not find %s in zip file' % filename)
    
    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request(self, tap_stream_id, params={}, url=None, url_extra=""):
        with metrics.http_request_timer(tap_stream_id) as timer:
            url = url or BASE_URL + "/api/v1/shops/" + self.shop_id + "/" + "click_reports" + url_extra
            headers = {"Authorization": "Bearer " + self.access_token}
            if self.user_agent:
                headers["User-Agent"] = self.user_agent
            request = requests.Request("GET", url, headers=headers, params=params)
            response = self.session.send(request.prepare())
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        if response.status_code in [429, 502]:
            raise RateLimitException()
        response.raise_for_status()
        return response.json()