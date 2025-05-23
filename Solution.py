import time
import re
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver

def fetch_ibdb_data():
    print(f"[{datetime.now()}] Starting scrape...")

    # Initialize browser
    browser = webdriver.Chrome()
    browser.set_page_load_timeout(500)

    root_url = "https://www.ibdb.com"
    scraped_results = []

    try:
        browser.get("https://www.ibdb.com/shows")
        time.sleep(5)
        main_soup = BeautifulSoup(browser.page_source, "html.parser")

        listings = main_soup.find("div", class_="page-wrapper xtrr")\
            .find("div", class_="shows-page")\
            .find("div", class_="row bgcolor-greyWhite2")\
            .find("div", class_="xt-c-box row")\
            .find("div", id="current")\
            .find("div", class_="row show-images xt-iblocks")\
            .find_all("div", class_="xt-iblock")

        # Load existing data for deduplication
        try:
            old_data = pd.read_csv("shows.csv")
            existing_keys = set(zip(old_data["Title"], old_data["Date"]))
        except FileNotFoundError:
            old_data = pd.DataFrame()
            existing_keys = set()

        for show_item in listings:
            show_anchor = show_item.find("div", class_="xt-iblock-inner").find("a", href=True)
            if not show_anchor:
                continue

            show_url = root_url + show_anchor['href']
            try:
                browser.get(show_url)
                time.sleep(5)
            except Exception as err:
                print(f"Timeout or error while loading {show_url}: {err}")
                continue

            show_detail_soup = BeautifulSoup(browser.page_source, "html.parser")
            detail_wrapper = show_detail_soup.find("body", class_="winOS")

            if not detail_wrapper:
                print("Could not find: body.winOS")
                continue

            layout_main = detail_wrapper.find("div", class_=re.compile("^production-page"))\
                .find("div", class_=re.compile("^xt-c-box"))\
                .find("div", class_="row xt-fixed-sidebar-row")

            left_panel = layout_main.find("div", class_=re.compile("col l4.*xt-l-col-left"))\
                .find("div", class_=re.compile("production-info-panel"))\
                .find("div", class_=re.compile("xt-fixed-sidebar"))\
                .find("div", class_=re.compile("jsfixed-placeholder"))\
                .find("div", class_=re.compile("jsfixed-block"))

            logo_info = left_panel.find("div", class_=re.compile("xt-fixed-block main-logo-wrapper"))\
                .find("div", class_="row logo")\
                .find("div", class_="col s12")\
                .find("div", class_="logo-block xt-logo-block sdf")

            image_src = logo_info.find("div", class_="xt-logo-img").find("img")['src']
            performance_title = logo_info.find("div", class_="title").find("div").find("h3").text.strip()
            category = left_panel.find("div", attrs={"data-id": "part-b"})\
                .find("div", class_="row wrapper hide-on-small-and-down")\
                .find("div").find("i").text.strip()

            details_section = left_panel.find("div", attrs={"data-id": "part-b"})\
                .find("div", class_="xt-info-block")\
                .find_all("div", class_="row wrapper")

            right_columns = layout_main.find("div", class_=re.compile("col l8.*xt-l-col-right"))\
                .find_all("div", class_="row")

            venue_wrapper = right_columns[1]
            venue_section = venue_wrapper.find(id="venues")
            if not venue_section:
                venue_wrapper = right_columns[2]
                venue_section = venue_wrapper.find(id="venues")
            if not venue_section:
                raise Exception("Missing div with id='venues'")

            theatre_info = venue_section.find("div", class_=re.compile("col s12 m4 theatre"))
            venue_blocks = theatre_info.find_all("div", class_="row")

            location_name = venue_blocks[1].find("a").text.strip()
            performance_date = venue_blocks[1].find("i").text.strip()

            if (performance_title, performance_date) in existing_keys:
                continue

            scraped_results.append({
                "Detail_Link": show_url,
                "Image Link": image_src,
                "Show Type": category,
                "Date": performance_date,
                "Theatre Name": location_name,
                "Title": performance_title,
            })

            print(f"Scraped: {performance_title} - {performance_date}")

    except Exception as scrape_err:
        print(f"Scraper error: {scrape_err}")
    finally:
        browser.quit()

    # Save new data
    if scraped_results:
        new_data = pd.DataFrame(scraped_results)
        final_data = pd.concat([old_data, new_data], ignore_index=True)
        final_data.drop_duplicates(subset=["Title", "Date"], inplace=True)
        final_data.to_csv("shows.csv", index=False)
    else:
        print("No new data found.")


# Run the scraper once
fetch_ibdb_data()
