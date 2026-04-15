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
        # First day of current month
    first_day_current_month = date.today().replace(day=1)

    # Last day of previous month
    end_date = first_day_current_month - timedelta(days=1)

    # First day of previous month
    start_date = end_date.replace(day=1)
    formatted_date = start_date.strftime("%B")
    
    sheets_to_update = [
           "monthly sheet",
        ]
    

    load_all_page_data(start_date, end_date, formatted_date, LOCAL_FILE, FILE_ID, sheets_to_update=sheets_to_update)  
   
if __name__ == "__main__":
    main()