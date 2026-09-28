import base64
import time

import requests

RETRYABLE_STATUS = (429, 500, 502, 503, 504)


def get_auth_headers(auth, secret=None):
    headers = {}
    params = {}
    if not auth:
        return headers, params

    auth_type = auth.get("type", "none")
    if auth_type == "bearer":
        headers["Authorization"] = "Bearer " + secret
    elif auth_type == "basic":
        token = base64.b64encode((auth["username"] + ":" + secret).encode()).decode()
        headers["Authorization"] = "Basic " + token
    elif auth_type == "api_key":
        if auth.get("location", "header") == "header":
            headers[auth["key_name"]] = secret
        else:
            params[auth["key_name"]] = secret
    return headers, params


def call_api(url, method="GET", headers=None, params=None, timeout=60, max_retries=3):
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.request(
                method, url, headers=headers, params=params, timeout=timeout
            )
        except requests.RequestException as error:
            last_error = str(error)
            if attempt == max_retries:
                raise RuntimeError(method + " " + url + " failed: " + last_error)
            time.sleep(2 ** attempt)
            continue

        if response.status_code < 400:
            return response.json()

        if response.status_code not in RETRYABLE_STATUS or attempt == max_retries:
            raise RuntimeError(
                method + " " + url + " returned " + str(response.status_code) + ": " + response.text[:300]
            )

        wait = response.headers.get("Retry-After")
        time.sleep(float(wait) if wait else 2 ** attempt)

    raise RuntimeError(method + " " + url + " failed after retries: " + str(last_error))


def extract_records(payload, record_path=None):
    if record_path:
        for part in record_path.split("."):
            if not isinstance(payload, dict):
                return []
            payload = payload.get(part, [])
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return [payload]
    return []


def fetch_all_pages(source, headers, params, max_pages=500):
    url = source["api_url"]
    method = source.get("method", "GET")
    timeout = source.get("timeout_seconds", 60)
    max_retries = source.get("max_retries", 3)
    pagination = source.get("pagination") or {"type": "none"}
    page_type = pagination.get("type", "none")
    page_size = pagination.get("page_size", 100)

    records = []
    page = pagination.get("start_page", 1)
    offset = 0

    for _ in range(max_pages):
        page_params = dict(params)
        if page_type == "page":
            page_params[pagination["page_param"]] = page
            if pagination.get("size_param"):
                page_params[pagination["size_param"]] = page_size
        elif page_type == "offset":
            page_params[pagination["offset_param"]] = offset
            page_params[pagination["limit_param"]] = page_size

        payload = call_api(url, method, headers, page_params, timeout, max_retries)
        batch = extract_records(payload, source.get("record_path"))
        records.extend(batch)

        if page_type == "none" or not batch or len(batch) < page_size:
            break

        page += 1
        offset += len(batch)

    return records
