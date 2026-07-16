import logging
import time
from string import Formatter
from typing import List, Dict, Any
from urllib.parse import urlparse

from news.config import (
    ANCHOR_CATEGORIES,
    ANCHOR_HOST,
    ANCHOR_URL_TEMPLATE,
)


def normalize_anchor_article_link(url: str) -> str:

    if not url:
        return ""

    parsed = urlparse(url)

    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def create_headless_chrome_driver():

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError as e:
        raise RuntimeError(
            "라이브러리가 설치되어 있지 않습니다."
        ) from e

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1280,2400")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    return webdriver.Chrome(options=options)


def validate_anchor_headline_settings() -> None:
    if not ANCHOR_HOST:
        raise RuntimeError("ANCHOR_HOST 환경 변수가 설정되지 않았습니다.")

    if not ANCHOR_CATEGORIES:
        raise RuntimeError(
            "ANCHOR_CATEGORIES 환경 변수가 설정되지 않았습니다."
        )

    if not ANCHOR_URL_TEMPLATE:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE 환경 변수가 설정되지 않았습니다."
        )

    try:
        template_fields = {
            field_name
            for _, field_name, _, _ in Formatter().parse(
                ANCHOR_URL_TEMPLATE
            )
            if field_name is not None
        }
    except ValueError as e:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE 형식이 올바르지 않습니다."
        ) from e

    if "category_id" not in template_fields:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE에 {category_id}가 필요합니다."
        )

    unsupported_fields = template_fields - {"host", "category_id"}

    if unsupported_fields:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE에는 "
            "{host}와 {category_id}만 사용할 수 있습니다."
        )


def build_anchor_category_url(category_id: int) -> str:
    validate_anchor_headline_settings()

    try:
        category_url = ANCHOR_URL_TEMPLATE.format(
            host=ANCHOR_HOST,
            category_id=category_id,
        )
    except (AttributeError, IndexError, KeyError, ValueError) as e:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE 형식이 올바르지 않습니다."
        ) from e

    parsed_url = urlparse(category_url)
    category_host = (parsed_url.hostname or "").lower()

    if parsed_url.scheme not in {"http", "https"} or not category_host:
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE은 완전한 HTTP(S) URL이어야 합니다."
        )

    if not (
        category_host == ANCHOR_HOST
        or category_host.endswith(f".{ANCHOR_HOST}")
    ):
        raise RuntimeError(
            "ANCHOR_URL_TEMPLATE의 호스트가 "
            "ANCHOR_HOST와 일치하지 않습니다."
        )

    return category_url


def collect_anchor_category_headlines(
    category_id: int,
    more_click_count: int = 3,
    wait_seconds: int = 10
) -> List[Dict[str, Any]]:

    category_url = build_anchor_category_url(category_id)

    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException
    except ImportError as e:
        raise RuntimeError(
            "라이브러리가 설치되어 있지 않습니다. "
        ) from e

    category_name = ANCHOR_CATEGORIES.get(category_id, str(category_id))

    driver = create_headless_chrome_driver()
    wait = WebDriverWait(driver, wait_seconds)

    try:
        logging.info(f"카테고리 접속: {category_name}")

        driver.get(category_url)

        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "a.sa_text_title")
            )
        )

        for click_index in range(more_click_count):
            try:
                more_button = wait.until(
                    EC.element_to_be_clickable(
                        (By.CSS_SELECTOR, "._CONTENT_LIST_LOAD_MORE_BUTTON")
                    )
                )

                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});",
                    more_button
                )
                time.sleep(0.5)
                driver.execute_script("arguments[0].click();", more_button)
                time.sleep(1.5)

                logging.info(
                    f"클릭 완료: "
                    f"category={category_name}, count={click_index + 1}"
                )

            except TimeoutException:
                logging.info(
                    f"클릭 불가: category={category_name}"
                )
                break

        article_elements = driver.find_elements(
            By.CSS_SELECTOR,
            "a.sa_text_title"
        )

        items = []
        seen_links = set()

        for element in article_elements:
            try:
                title = element.find_element(By.CSS_SELECTOR, "strong").text.strip()
            except Exception:
                title = element.text.strip()

            link = normalize_anchor_article_link(
                element.get_attribute("href") or ""
            )

            if not title or not link:
                continue

            if link in seen_links:
                continue

            seen_links.add(link)

            items.append(
                {
                    "title": title,
                    "description": "",
                    "link": link,
                    "originallink": link,
                    "pubDate": "",
                    "pubLabel": "news",
                    "_source_type": "anchor_headline",
                    "_category_id": category_id,
                    "_category_name": category_name
                }
            )

        logging.info(
            f"수집 완료: category={category_name}, count={len(items)}"
        )

        return items

    finally:
        driver.quit()


def collect_anchor_headlines(
    category_ids: List[int] | None = None,
    more_click_count: int = 3
) -> List[Dict[str, Any]]:

    validate_anchor_headline_settings()

    category_ids = category_ids or list(ANCHOR_CATEGORIES.keys())
    all_items = []

    for category_id in category_ids:
        category_items = collect_anchor_category_headlines(
            category_id=category_id,
            more_click_count=more_click_count
        )

        all_items.extend(category_items)

    return all_items
