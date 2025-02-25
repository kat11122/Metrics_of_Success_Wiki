import requests
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

print_lock = Lock()


def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)


def fetch_pageviews(wikidata_id):
    print("Fetching pageviews for " + wikidata_id)
    wikidata_url = f"https://www.wikidata.org/wiki/{wikidata_id}"

    url = f"https://www.wikidata.org/w/api.php"
    params = {
        "action": "wbgetentities",
        "ids": wikidata_id,
        "format": "json",
        "props": "sitelinks"
    }

    headers = {
        "User-Agent": "UniversityProject/1.0 (contact: sexoce4246@kvegg.com)"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        sitelinks = data.get("entities", {}).get(wikidata_id, {}).get("sitelinks", {})
        enwiki_title = sitelinks.get("enwiki", {}).get("title")
        dewiki_title = sitelinks.get("dewiki", {}).get("title")
        nlwiki_title = sitelinks.get("nlwiki", {}).get("title")
        ruwiki_title = sitelinks.get("ruwiki", {}).get("title")
        itwiki_title = sitelinks.get("itwiki", {}).get("title")
        frwiki_title = sitelinks.get("frwiki", {}).get("title")
        huwiki_title = sitelinks.get("huwiki", {}).get("title")
        bewiki_title = sitelinks.get("bewiki", {}).get("title")
        eswiki_title = sitelinks.get("eswiki", {}).get("title")

        view_count = 0
        if enwiki_title:
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/en.wikipedia/all-access/all-agents/"
                f"{enwiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views
        if dewiki_title:
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/de.wikipedia/all-access/all-agents/"
                f"{dewiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views
        if nlwiki_title:
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/nl.wikipedia/all-access/all-agents/"
                f"{nlwiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views
        if ruwiki_title:
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/ru.wikipedia/all-access/all-agents/"
                f"{ruwiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views
        if itwiki_title:
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/it.wikipedia/all-access/all-agents/"
                f"{itwiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views
        if frwiki_title:
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/fr.wikipedia/all-access/all-agents/"
                f"{frwiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views
        if huwiki_title:
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/hu.wikipedia/all-access/all-agents/"
                f"{huwiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views
        if eswiki_title:
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/es.wikipedia/all-access/all-agents/"
                f"{eswiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views
        if bewiki_title :
            pageview_url = (
                "https://wikimedia.org/api/rest_v1/metrics/pageviews/"
                f"per-article/be.wikipedia/all-access/all-agents/"
                f"{bewiki_title}/monthly/20230101/20231231"
            )
            pv_response = requests.get(pageview_url, headers=headers)

            if pv_response.status_code == 200:
                pv_data = pv_response.json()
                views = sum([item['views'] for item in pv_data.get('items', [])])
                view_count += views

        num_wikis = len(sitelinks.keys())
        return view_count, num_wikis
    else:
        raise Exception(f"{wikidata_id} is not a valid wikidata id")

    return None


def crawl_wikidata():
    data_path = "data/Person.xlsx"
    df = pd.read_excel(data_path)

    df = df[~df["Wikidata"].isna()]
    df["Wikidata"] = df["Wikidata"].astype(str)

    def process_row(row):
        return fetch_pageviews(row["Wikidata"])

    df_lock = Lock()

    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_row = {executor.submit(fetch_pageviews, row["Wikidata"]): index for index, row in df.iterrows()}
        for future in as_completed(future_to_row):
            index = future_to_row[future]
            try:
                pageviews, num_wikis = future.result()
                with df_lock:  # Ensure thread-safe DataFrame updates
                    df.at[index, "Pageviews"] = pageviews
                    df.at[index, "num_wikis"] = num_wikis
            except Exception as e:
                thread_safe_print(f"Error processing row {index}: {e}")

    output_path = "data/Person_with_Pageviews.xlsx"
    df = df[~df["Pageviews"].isna()]
    df.to_excel(output_path, index=False)
    return df


if __name__ == "__main__":
    crawl_wikidata()
