import sys
import requests
import json
import prison
import aiohttp
import asyncio
import re
from auth import *

class SupersetClient:
    def __init__(self, superset_url, username, password):
        self.superset_url = superset_url
        self.username = username
        self.password = password
        self.access_token = None
        self.csrf_token = None
        self.guest_token = None
        self.user_me = None
        self.cookies = {}
        self.total_requests = 0  # 总请求数
        self.error_requests = 0  # 错误请求数
        self.ignore_error_tables = set([])  # 忽略错误的表名

    def error(self, message):
        print(f"Error: {message}", file=sys.stderr)
        sys.exit(77)

    def get_access_token(self):
        url = f"{self.superset_url}/api/v1/security/login"
        payload = {
            "password": self.password,
            "provider": "db",
            "username": self.username
        }
        response = requests.post(url, json=payload)
        if response.status_code != 200:
            self.error(f"Failed to login: {response.text}")

        access_token = response.json().get('access_token')
        if not access_token:
            self.error(f"Failed to extract access_token from the response: {response.text}")

        self.access_token = access_token

    def get_csrf_token(self):
        url_csrf = f"{self.superset_url}/api/v1/security/csrf_token/"
        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.get(url_csrf, headers=headers, cookies=self.cookies)
        if response.status_code != 200:
            self.error(f"Failed to get CSRF token: {response.text}")

        csrf_token = response.json().get('result')
        if not csrf_token:
            self.error(f"Failed to extract csrf_token from the response: {response.text}")

        self.csrf_token = csrf_token
        self.cookies = response.cookies

    def get_cookie_from_login(self):
        url = f"{self.superset_url}/login/"

        payload = {
            "csrf_token": self.csrf_token,
            "username": self.username,
            "password": self.password
        }
        response = requests.post(url, data=payload, cookies=self.cookies)
        self.cookies = response.cookies

    async def asyncRequest(self, session, url, method='GET', params = None):
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.access_token}"
            }

            request_table_name = re.search(r'value:\'?([0-9a-zA-Z:_\-\.]+)\'?', params.get('q')).group(1)
            print(request_table_name)

            async with session.request(method, url, headers=headers, cookies=self.cookies, params=params) as response:
                self.total_requests += 1
                if response.status != 200:
                    text = await response.text()
                    self.error(f"Failed getting dataset: {text}")
                result_json = await response.json()
                result = result_json.get('result')

                if (len(result) != 1):
                    print(f"Error!!! {len(result)} items")
                    self.error_requests += 1

                response_table_name = result[0].get('table_name')
                print(f"response for {request_table_name}")

                if (request_table_name != response_table_name):
                    if request_table_name not in self.ignore_error_tables:
                        print(f"Error!!! {request_table_name} != {response_table_name}")
                        # print(result[0])
                        print(result_json)
                        self.error_requests += 1
                return result

        except Exception as e:
            print(e)
            print("Unable to get url {} due to {}.".format(url, e.__class__))
            self.error_requests += 1
    

    def print_error_rate(self):
        if self.total_requests == 0:
            print("No requests made yet.")
        else:
            rate = self.error_requests / self.total_requests
            print(f"Error rate: {self.error_requests}/{self.total_requests} = {rate:.2%}")

            
    def get_guest_token(self):
        url = f"{self.superset_url}/api/v1/security/guest_token"
        headers = {
            'Content-Type': 'application/json',
            'X-CSRFToken': self.csrf_token,
            "Authorization": f"Bearer {self.access_token}",
        }
        payload = {
            "user": {
                "username": "admin",
                "first_name": "Stan",
                "last_name": "Lee"
            },
            "resources": [
                { "type": "dashboard", "id": "11" }
            ],
            "rls": []
        }
        response = requests.post(url, json=payload, cookies=self.cookies, headers=headers)

        if response.status_code != 200:
            self.error(f"Failed getting guest token: {response.text}")

        guess_token = response.json().get('token')
        self.guess_token = guess_token

    def list_dashboards(self):
        query = {
            "columns": ["dashboard_title", "id"],
            "page": 0,
            "page_size": 1000
        }
        encoded_query = requests.utils.quote(json.dumps(query))
        url = f"{self.superset_url}/api/v1/dashboard/?q={encoded_query}"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "X-CSRFToken": self.csrf_token
        }
        response = requests.get(url, headers=headers, cookies=self.cookies)
        if response.status_code != 200:
            self.error(f"Failed getting dashboards: {response.text}")

        result = response.json().get('result')
        if not result:
            self.error(f"Failed getting dashboards: {response.text}")

        for dashboard in result:
            title = dashboard['dashboard_title']
            dashboard_id = dashboard['id']
            print(f"Dashboard: {title} - id: {dashboard_id}")

    def me(self):
        url = f"{self.superset_url}/api/v1/me"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.get(url, headers=headers, cookies=self.cookies)
        if response.status_code != 200:
            self.error(f"Failed getting me: {response.text}")

        result = response.json().get('result')
        self.user_me = result

    def get_dataset_by_name(self, table_name):
        url = f"{self.superset_url}/api/v1/dataset"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        params = {
            'q': prison.dumps({
                'filters': [
                    { 'col': 'table_name', 'opr': 'eq', 'value': table_name }
                ]
            })
        }
        response = requests.get(url, headers=headers, cookies=self.cookies, params=params)
        if response.status_code != 200:
            self.error(f"Failed getting dataset: {response.text}")

        result = response.json().get('result')
        return result

    def list_datasets(self):
        url = f"{self.superset_url}/api/v1/dataset"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        response = requests.get(url=url, headers=headers, cookies=self.cookies)
        if response.status_code != 200:
            self.error(f"Failed getting dataset: {response.text}")

        result = response.json().get('result')
        return result

    async def extress_dataset_api(self, batch_size=10):
        datasets = self.list_datasets()

        queries = []
        for dataset in datasets:
            table_name = dataset.get("table_name")
            for i in range(1, batch_size):
                queries.append({
                    'url': f"{self.superset_url}/api/v1/dataset",
                    'params': {
                        'q': prison.dumps({
                            "filters": [
                                { "col": "table_name", "opr": "eq", "value": table_name }
                            ]
                        })
                    }
                })

        async with aiohttp.ClientSession() as session:
            ret = await asyncio.gather(
                *(self.asyncRequest(
                    session=session,
                    method='GET',
                    url=query.get('url'),
                    params=query.get('params')
                ) for query in queries)
            )


async def main(argv):
    client = SupersetClient(superset_host, superset_username, superset_password)

    client.get_access_token()
    client.get_csrf_token()
    client.get_cookie_from_login()
    print('COOKIES[get_cookie_from_login]', client.cookies)

    client.list_dashboards() # <-- NO requiere cookie del login

    client.me() # <-- requiere cookie del login
    print('ME:', client.user_me)

    client.get_guest_token()  # <-- NO requiere cookie del login
    print('guess_token:', client.guess_token)

    # dataset = client.get_dataset_by_name(name = 'ficheros')
    # print('dataset:', dataset)

    await client.extress_dataset_api()

    # 统计响应错误率（可以选择忽略某个表）
    client.print_error_rate()


if __name__ == "__main__":
    asyncio.run( main(sys.argv[1:]) )