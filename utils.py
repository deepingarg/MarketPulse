import pandas as pd
import datetime
import logging
from data_storage import get_available_dates

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_date_range(reference_date, num_days):
    """
    Calculate start and end dates given a reference date and number of days
    
    Args:
        reference_date (str): Reference date in YYYY-MM-DD format
        num_days (int): Number of days to look back
        
    Returns:
        tuple: (start_date_str, end_date_str)
    """
    try:
        # Convert reference date to datetime
        if isinstance(reference_date, str):
            ref_date = datetime.datetime.strptime(reference_date, "%Y-%m-%d").date()
        else:
            ref_date = reference_date
        
        # Calculate start date
        start_date = ref_date - datetime.timedelta(days=num_days)
        
        # Convert to string format
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = ref_date.strftime("%Y-%m-%d")
        
        return start_date_str, end_date_str
    
    except Exception as e:
        logger.error(f"Error calculating date range: {str(e)}")
        # Return a default range
        if isinstance(reference_date, str):
            return reference_date, reference_date
        else:
            today = datetime.date.today()
            return (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

def get_previous_trading_day(reference_date):
    """
    Get the previous trading day from the available data
    
    Args:
        reference_date (str): Reference date in YYYY-MM-DD format
        
    Returns:
        str: Previous trading day in YYYY-MM-DD format or None if not available
    """
    try:
        # Get all available dates
        available_dates = get_available_dates()
        
        if not available_dates or reference_date not in available_dates:
            logger.warning(f"Reference date {reference_date} not in available dates")
            return None
        
        # Find the index of the reference date
        idx = available_dates.index(reference_date)
        
        # If it's the first date or no earlier date is available, return None
        if idx <= 0:
            logger.warning(f"No previous trading day available for {reference_date}")
            return None
        
        # Return the previous date
        return available_dates[idx - 1]
    
    except Exception as e:
        logger.error(f"Error getting previous trading day: {str(e)}")
        return None

def get_next_trading_day(reference_date):
    """
    Get the next trading day from the available data
    
    Args:
        reference_date (str): Reference date in YYYY-MM-DD format
        
    Returns:
        str: Next trading day in YYYY-MM-DD format or None if not available
    """
    try:
        # Get all available dates
        available_dates = get_available_dates()
        
        if not available_dates or reference_date not in available_dates:
            logger.warning(f"Reference date {reference_date} not in available dates")
            return None
        
        # Find the index of the reference date
        idx = available_dates.index(reference_date)
        
        # If it's the last date or no later date is available, return None
        if idx >= len(available_dates) - 1:
            logger.warning(f"No next trading day available for {reference_date}")
            return None
        
        # Return the next date
        return available_dates[idx + 1]
    
    except Exception as e:
        logger.error(f"Error getting next trading day: {str(e)}")
        return None

def format_currency(value):
    """
    Format a value as Indian currency
    
    Args:
        value (float): Value to format
        
    Returns:
        str: Formatted currency string
    """
    try:
        if value >= 10000000:  # 1 crore
            return f"₹{value/10000000:.2f} Cr"
        elif value >= 100000:  # 1 lakh
            return f"₹{value/100000:.2f} L"
        else:
            return f"₹{value:.2f}"
    
    except Exception as e:
        logger.error(f"Error formatting currency: {str(e)}")
        return f"₹{value}"

def clean_symbol(symbol):
    """
    Clean a stock symbol for display
    
    Args:
        symbol (str): Stock symbol (e.g., "RELIANCE.NS")
        
    Returns:
        str: Cleaned symbol (e.g., "RELIANCE")
    """
    try:
        # Remove .NS or .BO suffix
        if '.' in symbol:
            return symbol.split('.')[0]
        return symbol
    
    except Exception as e:
        logger.error(f"Error cleaning symbol: {str(e)}")
        return symbol

def format_percentage(value):
    """
    Format a value as a percentage
    
    Args:
        value (float): Value to format
        
    Returns:
        str: Formatted percentage string
    """
    try:
        if value > 0:
            return f"+{value:.2f}%"
        else:
            return f"{value:.2f}%"
    
    except Exception as e:
        logger.error(f"Error formatting percentage: {str(e)}")
        return f"{value}%"

if __name__ == "__main__":
    # Test functionality
    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")
    
    start_date, end_date = get_date_range(today_str, 7)
    print(f"Date range from today back 7 days: {start_date} to {end_date}")
    
    # Test formatting
    print(f"1 crore formatted: {format_currency(10000000)}")
    print(f"1 lakh formatted: {format_currency(100000)}")
    print(f"Regular value formatted: {format_currency(5000)}")
    
    # Test symbol cleaning
    print(f"Cleaned symbol: {clean_symbol('RELIANCE.NS')}")
    
    # Test percentage formatting
    print(f"Positive percentage: {format_percentage(2.5)}")
    print(f"Negative percentage: {format_percentage(-1.75)}")
