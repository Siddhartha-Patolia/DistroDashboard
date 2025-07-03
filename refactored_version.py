import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
import logging
from selenium.webdriver.chrome.options import Options
import chromedriver_autoinstaller
from pyvirtualdisplay import Display
from typing import List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IntradayInvesting:
    """
    Scrapes historical intraday data from Investing.com using Selenium.
    """

    def __init__(self, url: str, interval: str):
        self.url = url
        self.interval = interval

    def fetch_data_investing(self) -> pd.DataFrame:
        """
        Attempts to fetch data from Investing.com with multiple retries.
        Returns:
            pd.DataFrame: DataFrame with columns ['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
        """
        max_attempts = 20
        for attempt in range(max_attempts):
            try:
                logger.info(f"Attempt {attempt + 1} to fetch data from {self.url}")
                data = self._start_web_scraper(self.url, self.interval)
                logger.info("Data fetched successfully.")
                return data
            except Exception as e:
                logger.warning(f"Error fetching data (attempt {attempt + 1}): {e}")
        logger.error("Max attempts reached. Returning empty DataFrame.")
        columns = ["Datetime", "Adj Close", "Close", "High", "Low", "Open", "Volume"]
        return pd.DataFrame(columns=columns)

    def _start_web_scraper(self, url: str, interval: str) -> pd.DataFrame:
        """
        Launches a headless Chrome browser and scrapes the historical data table.
        """
        display = Display(visible=0, size=(1200, 1200))
        display.start()
        chromedriver_autoinstaller.install()

        chrome_options = Options()
        options = [
            "--window-size=1200,1200",
            "--ignore-certificate-errors",
            "--headless",
            "--disable-gpu",
            "--disable-extensions",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--remote-debugging-port=9222",
        ]
        for option in options:
            chrome_options.add_argument(option)
        # If JavaScript is required, comment out the next two lines
        # prefs = {"profile.managed_default_content_settings.javascript": 2}
        # chrome_options.add_experimental_option("prefs", prefs)

        driver = webdriver.Chrome(options=chrome_options)
        try:
            driver.get(url)
            logger.info(f"Opened URL: {url}")

            # Extract column headers
            fgbl_columns = self._extract_table_headers(driver)
            logger.info(f"Extracted columns: {fgbl_columns}")

            # Extract table data
            fgbl_row_data = self._extract_table_rows(driver)
            if not fgbl_row_data:
                logger.warning("No table data found on the page.")

            return self._export_csv(fgbl_columns, fgbl_row_data, interval)
        finally:
            driver.quit()
            display.stop()

    def _extract_table_headers(self, driver) -> List[str]:
        """
        Extracts table headers from the page.
        """
        try:
            table_heading = driver.find_elements(By.XPATH, "//thead")
            for table in table_heading:
                if table.text:
                    return table.text.split("\n")
        except Exception as e:
            logger.warning(f"Could not extract table headers: {e}")
        # Default headers if extraction fails
        return ["Date", "Price", "Open", "High", "Low", "Vol.", "Change %"]

    def _extract_table_rows(self, driver) -> List[List[str]]:
        """
        Extracts table row data from the page.
        """
        table_data = driver.find_elements(
            By.XPATH, "//tr[contains(@class, 'historical-data')]"
        )
        fgbl_row_data = []
        for row in table_data:
            row_text = row.text.replace(",", "")
            fgbl_row_data.append(row_text.split(" "))
        return fgbl_row_data

    def _export_csv(
        self,
        table_heading_list: List[str],
        table_row_list: List[List[str]],
        interval: str,
    ) -> pd.DataFrame:
        """
        Converts scraped table data to a DataFrame and processes it.
        """
        table_new_row_list = []
        for row in table_row_list:
            if len(row) >= 7:
                date = "-".join(row[0:3])
                templist = [date] + row[3:]
                table_new_row_list.append(templist)
            else:
                logger.warning(f"Skipping malformed row: {row}")
        fgbl_df = pd.DataFrame(table_new_row_list, columns=table_heading_list)
        return self._process_csv(fgbl_df, interval)

    def _process_csv(self, csv_file: pd.DataFrame, interval: str) -> pd.DataFrame:
        """
        Cleans and formats the DataFrame.
        """
        fgbl_csv = csv_file.copy()
        expected_columns = [
            "Datetime",
            "Close",
            "Open",
            "High",
            "Low",
            "Vol.",
            "Change %",
        ]
        if len(fgbl_csv.columns) != len(expected_columns):
            fgbl_csv.columns = expected_columns[: len(fgbl_csv.columns)]
        else:
            fgbl_csv.columns = expected_columns

        fgbl_csv["Adj Close"] = fgbl_csv["Close"]
        fgbl_csv["Volume"] = fgbl_csv["Vol."]
        fgbl_csv = fgbl_csv[
            ["Datetime", "Adj Close", "Close", "High", "Low", "Open", "Volume"]
        ]

        # Convert numeric columns from string to float
        for col in ["Adj Close", "Close", "High", "Low", "Open", "Volume"]:
            fgbl_csv[col] = pd.to_numeric(
                fgbl_csv[col].astype(str).str.replace("%", ""), errors="coerce"
            )

        if interval in ["1d", "1w", "1mo"]:
            fgbl_csv["Datetime"] = pd.to_datetime(fgbl_csv["Datetime"], errors="coerce")
            fgbl_csv["Datetime"] = fgbl_csv["Datetime"].dt.strftime("%Y-%m-%d")
            fgbl_csv.set_index("Datetime", inplace=True)
            fgbl_csv.index.name = "Datetime"

        fgbl_csv.sort_index(inplace=True)
        fgbl_csv.dropna(how="all", inplace=True)
        return fgbl_csv


if __name__ == "__main__":
    url = "https://in.investing.com/rates-bonds/euro-bund-historical-data"
    interval = "1d"
    fgbl_object = IntradayInvesting(url=url, interval=interval)
    df = fgbl_object.fetch_data_investing()
    print(df)
