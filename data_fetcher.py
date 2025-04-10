import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import datetime
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_nifty50_symbols():
    """
    Fetch the list of Nifty 50 stocks
    
    Returns:
        list: List of Nifty 50 stock symbols with .NS suffix
    """
    try:
        # Try to fetch from NSE website
        url = "https://www.nseindia.com/market-data/live-equity-market"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
        }
        
        # First attempt with direct NSE website
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                # This would need to be adjusted based on actual NSE website structure
                # which may change over time
                # This is a placeholder for the actual extraction logic
                return None  # Will fall back to the next method
            else:
                logger.warning(f"Failed to fetch Nifty 50 list from NSE website: {response.status_code}")
                return None
        except Exception as e:
            logger.warning(f"Error fetching from NSE website: {str(e)}")
        
        # Fallback to Wikipedia or another source
        wiki_url = "https://en.wikipedia.org/wiki/NIFTY_50"
        response = requests.get(wiki_url, headers=headers)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Find the table with Nifty 50 components
            tables = soup.find_all('table', {'class': 'wikitable'})
            
            symbols = []
            if tables:
                # Look for the table with the right structure
                for table in tables:
                    headers = [th.get_text().strip() for th in table.find_all('th')]
                    if 'Symbol' in headers or 'Ticker' in headers:
                        # Get the index of the symbol/ticker column
                        symbol_idx = headers.index('Symbol') if 'Symbol' in headers else headers.index('Ticker')
                        
                        # Extract symbols from the table rows
                        for row in table.find_all('tr')[1:]:  # Skip header row
                            cols = row.find_all('td')
                            if len(cols) > symbol_idx:
                                symbol = cols[symbol_idx].get_text().strip()
                                if symbol:
                                    # Add .NS suffix for Yahoo Finance
                                    symbols.append(f"{symbol}.NS")
                        
                        if symbols:
                            break
            
            if not symbols:
                # If we couldn't extract from the tables, fall back to a hardcoded list
                logger.warning("Could not extract symbols from Wikipedia, using hardcoded list")
                return get_hardcoded_nifty50()
            
            return symbols
        else:
            logger.warning(f"Failed to fetch Nifty 50 list from Wikipedia: {response.status_code}")
            return get_hardcoded_nifty50()
    
    except Exception as e:
        logger.error(f"Error fetching Nifty 50 stocks: {str(e)}")
        return get_hardcoded_nifty50()

def get_hardcoded_nifty50():
    """
    Return a hardcoded list of Nifty 50 stocks in case the fetching fails
    
    Returns:
        list: List of Nifty 50 stock symbols with .NS suffix
    """
    # This list may need to be updated periodically as the index composition changes
    return [
        'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
        'HINDUNILVR.NS', 'ITC.NS', 'SBIN.NS', 'BHARTIARTL.NS', 'KOTAKBANK.NS',
        'LT.NS', 'AXISBANK.NS', 'ASIANPAINT.NS', 'MARUTI.NS', 'HCLTECH.NS',
        'SUNPHARMA.NS', 'TATAMOTORS.NS', 'ULTRACEMCO.NS', 'TITAN.NS', 'BAJFINANCE.NS',
        'NESTLEIND.NS', 'WIPRO.NS', 'TECHM.NS', 'ADANIPORTS.NS', 'POWERGRID.NS',
        'M&M.NS', 'NTPC.NS', 'GRASIM.NS', 'BAJAJFINSV.NS', 'HDFCLIFE.NS',
        'DIVISLAB.NS', 'JSWSTEEL.NS', 'TATACONSUM.NS', 'SBILIFE.NS', 'HINDALCO.NS',
        'DRREDDY.NS', 'TATASTEEL.NS', 'BRITANNIA.NS', 'INDUSINDBK.NS', 'CIPLA.NS',
        'EICHERMOT.NS', 'COALINDIA.NS', 'ONGC.NS', 'BPCL.NS', 'UPL.NS',
        'IOC.NS', 'HEROMOTOCO.NS', 'APOLLOHOSP.NS', 'ADANIENT.NS', 'BAJAJ-AUTO.NS'
    ]

def fetch_stock_data(symbol, start_date, end_date):
    """
    Fetch stock data for a given symbol and date range using yfinance
    
    Args:
        symbol (str): Stock symbol (with .NS suffix for NSE stocks)
        start_date (datetime.date): Start date for data fetching
        end_date (datetime.date): End date for data fetching
        
    Returns:
        pd.DataFrame: DataFrame with stock price data
    """
    try:
        # Convert dates to strings
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # Add one day to end_date to include the end date in the results
        next_day = (end_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        
        # Fetch data from Yahoo Finance
        stock_data = yf.download(symbol, start=start_str, end=next_day)
        
        # If data is empty, return None
        if stock_data.empty:
            logger.warning(f"No data available for {symbol} from {start_str} to {end_str}")
            return None
        
        # Add symbol column for identification
        stock_data['Symbol'] = symbol
        
        # Reset index to make Date a column
        stock_data = stock_data.reset_index()
        
        # Ensure all expected columns are present
        expected_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Symbol']
        for col in expected_columns:
            if col not in stock_data.columns:
                if col == 'Adj Close':
                    # If Adj Close is missing, use Close
                    stock_data['Adj Close'] = stock_data['Close']
                elif col == 'Volume':
                    # If Volume is missing, set to 0
                    stock_data['Volume'] = 0
        
        logger.info(f"Successfully fetched data for {symbol} from {start_str} to {end_str}")
        
        return stock_data
    
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {str(e)}")
        return None

def fetch_multiple_stocks(symbols, start_date, end_date, progress_callback=None):
    """
    Fetch data for multiple stocks with rate limiting and progress tracking
    
    Args:
        symbols (list): List of stock symbols
        start_date (datetime.date): Start date
        end_date (datetime.date): End date
        progress_callback (function, optional): Callback function to report progress
        
    Returns:
        dict: Dictionary mapping symbols to their data DataFrames
    """
    results = {}
    
    for i, symbol in enumerate(symbols):
        # Fetch data for the symbol
        data = fetch_stock_data(symbol, start_date, end_date)
        
        if data is not None:
            results[symbol] = data
        
        # Report progress if callback is provided
        if progress_callback:
            progress = (i + 1) / len(symbols)
            progress_callback(progress, symbol, data is not None)
        
        # Rate limiting to avoid exceeding API limits
        if i < len(symbols) - 1:  # Don't sleep after the last request
            time.sleep(0.5)  # 500ms delay between requests
    
    return results

if __name__ == "__main__":
    # Test functionality
    symbols = get_nifty50_symbols()
    print(f"Found {len(symbols)} Nifty 50 symbols")
    
    if symbols:
        # Test fetching data for a single symbol
        today = datetime.date.today()
        week_ago = today - datetime.timedelta(days=7)
        
        print(f"Fetching data for {symbols[0]} from {week_ago} to {today}")
        data = fetch_stock_data(symbols[0], week_ago, today)
        
        if data is not None:
            print(data.head())
