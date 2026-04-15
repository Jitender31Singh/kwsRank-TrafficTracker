import logging
import os
from dotenv import load_dotenv
from datetime import date, timedelta
from dailyPageData import load_all_page_data



load_dotenv()

FILE_ID = os.getenv("PAGE_RANKING_FILE_ID")
LOCAL_FILE = "temp_monthly.xlsx"
    
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting SEO pipeline...")

    today = date.today()

    # Start of current week (Monday)
    start_of_current_week = today - timedelta(days=today.weekday())

    # End of previous week (Sunday)
    end_date = start_of_current_week - timedelta(days=1)

    # Start of previous week (Monday)
    start_date = end_date - timedelta(days=6)
    formatted_date = start_date.strftime("%d %B")+" - "+end_date.strftime("%d %B")
    
    sheets_to_update = [
           "weekly sheet",
        ]
    

    load_all_page_data(start_date, end_date, formatted_date, LOCAL_FILE, FILE_ID, sheets_to_update=sheets_to_update)  
   
if __name__ == "__main__":
    main()