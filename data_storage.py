import pandas as pd
import os
import glob
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data directory
DATA_DIR = "data"

def ensure_data_dir():
    """Ensure that the data directory exists"""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        logger.info(f"Created data directory: {DATA_DIR}")

def save_data(data, symbol, date_str):
    """
    Save stock data to CSV files organized by date
    
    Args:
        data (pd.DataFrame): Stock data to save
        symbol (str): Stock symbol
        date_str (str): Date string in YYYY-MM-DD format
    
    Returns:
        bool: True if successful, False otherwise
    """
    ensure_data_dir()
    
    try:
        # Create date directory if it doesn't exist
        date_dir = os.path.join(DATA_DIR, date_str)
        if not os.path.exists(date_dir):
            os.makedirs(date_dir)
        
        # Clean symbol name for filename
        clean_symbol = symbol.replace('.', '_').replace(':', '_').replace('/', '_')
        
        # File path
        file_path = os.path.join(date_dir, f"{clean_symbol}.csv")
        
        # Save data to CSV
        data.to_csv(file_path, index=False)
        
        logger.info(f"Saved data for {symbol} on {date_str} to {file_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error saving data for {symbol} on {date_str}: {str(e)}")
        return False

def load_data(date_str):
    """
    Load stock data for a specific date
    
    Args:
        date_str (str): Date string in YYYY-MM-DD format
    
    Returns:
        pd.DataFrame: Combined DataFrame with all stocks for the date
    """
    ensure_data_dir()
    
    try:
        # Date directory
        date_dir = os.path.join(DATA_DIR, date_str)
        
        if not os.path.exists(date_dir):
            logger.warning(f"No data directory found for date: {date_str}")
            return pd.DataFrame()
        
        # Get all CSV files in the date directory
        csv_files = glob.glob(os.path.join(date_dir, "*.csv"))
        
        if not csv_files:
            logger.warning(f"No CSV files found for date: {date_str}")
            return pd.DataFrame()
        
        # Load and combine all CSVs
        dfs = []
        for file in csv_files:
            try:
                df = pd.read_csv(file)
                
                # Convert Date column to datetime if it exists
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'])
                
                dfs.append(df)
            except Exception as e:
                logger.error(f"Error loading file {file}: {str(e)}")
        
        if not dfs:
            logger.warning(f"No valid data files for date: {date_str}")
            return pd.DataFrame()
        
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        
        logger.info(f"Loaded data for {date_str} with {len(combined_df)} rows")
        return combined_df
    
    except Exception as e:
        logger.error(f"Error loading data for {date_str}: {str(e)}")
        return pd.DataFrame()

def load_stock_data(symbol, start_date_str, end_date_str):
    """
    Load data for a specific stock across a date range
    
    Args:
        symbol (str): Stock symbol
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format
    
    Returns:
        pd.DataFrame: DataFrame with stock data for the specified range
    """
    ensure_data_dir()
    
    try:
        # Clean symbol for filename matching
        clean_symbol = symbol.replace('.', '_').replace(':', '_').replace('/', '_')
        
        # Load from database first
        from database_manager import load_stock_data_from_db
        df = load_stock_data_from_db(symbol, start_date_str, end_date_str)
        
        if not df.empty:
            return df
            
        # Fallback to CSV if database is empty
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        
        # Get all date directories in the range
        date_dirs = []
        for date_str in os.listdir(DATA_DIR):
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d").date()
                if start_date <= date <= end_date:
                    date_dirs.append(date_str)
            except ValueError:
                continue
        
        if not date_dirs:
            logger.warning(f"No data found in the range {start_date_str} to {end_date_str}")
            return pd.DataFrame()
        
        # Clean symbol for filename matching
        clean_symbol = symbol.replace('.', '_').replace(':', '_').replace('/', '_')
        
        # Load data from each date directory
        dfs = []
        for date_str in sorted(date_dirs):
            file_path = os.path.join(DATA_DIR, date_str, f"{clean_symbol}.csv")
            
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    
                    # Convert Date column to datetime if it exists
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                    
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Error loading file {file_path}: {str(e)}")
        
        if not dfs:
            logger.warning(f"No data found for {symbol} in the range {start_date_str} to {end_date_str}")
            return pd.DataFrame()
        
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True).sort_values('Date')
        
        logger.info(f"Loaded data for {symbol} from {start_date_str} to {end_date_str} with {len(combined_df)} rows")
        return combined_df
    
    except Exception as e:
        logger.error(f"Error loading data for {symbol} from {start_date_str} to {end_date_str}: {str(e)}")
        return pd.DataFrame()

def get_available_dates():
    """
    Get a list of all dates for which data is available
    
    Returns:
        list: List of available dates in YYYY-MM-DD format
    """
    ensure_data_dir()
    
    try:
        # Get all subdirectories in the data directory
        dates = []
        for item in os.listdir(DATA_DIR):
            item_path = os.path.join(DATA_DIR, item)
            
            # Check if it's a directory and follows the date format
            if os.path.isdir(item_path):
                try:
                    # Validate date format
                    datetime.strptime(item, "%Y-%m-%d")
                    dates.append(item)
                except ValueError:
                    # Skip directories that don't match the date format
                    continue
        
        return sorted(dates)
    
    except Exception as e:
        logger.error(f"Error getting available dates: {str(e)}")
        return []

def get_available_symbols(date_str=None):
    """
    Get a list of all available stock symbols
    
    Args:
        date_str (str, optional): Date to get symbols for. If None, gets symbols across all dates.
    
    Returns:
        list: List of available stock symbols
    """
    ensure_data_dir()
    
    try:
        symbols = set()
        
        if date_str:
            # Get symbols for a specific date
            date_dir = os.path.join(DATA_DIR, date_str)
            
            if not os.path.exists(date_dir):
                logger.warning(f"No data directory found for date: {date_str}")
                return []
            
            # Get all CSV files in the date directory
            csv_files = glob.glob(os.path.join(date_dir, "*.csv"))
            
            for file in csv_files:
                # Extract symbol from filename
                filename = os.path.basename(file)
                symbol = filename.replace('_', '.').replace('.csv', '')
                symbols.add(symbol)
        
        else:
            # Get symbols across all dates
            dates = get_available_dates()
            
            for date in dates:
                date_dir = os.path.join(DATA_DIR, date)
                csv_files = glob.glob(os.path.join(date_dir, "*.csv"))
                
                for file in csv_files:
                    # Extract symbol from filename
                    filename = os.path.basename(file)
                    symbol = filename.replace('_', '.').replace('.csv', '')
                    symbols.add(symbol)
        
        return sorted(list(symbols))
    
    except Exception as e:
        logger.error(f"Error getting available symbols: {str(e)}")
        return []

if __name__ == "__main__":
    # Test functionality
    ensure_data_dir()
    
    print("Available dates:", get_available_dates())
    
    # Create sample data
    sample_data = pd.DataFrame({
        'Date': [datetime.now()],
        'Open': [100.0],
        'High': [105.0],
        'Low': [99.0],
        'Close': [102.0],
        'Volume': [1000000],
        'Symbol': ['TEST.NS']
    })
    
    # Save sample data
    today_str = datetime.now().strftime("%Y-%m-%d")
    save_data(sample_data, 'TEST.NS', today_str)
    
    # Load and verify
    loaded_data = load_data(today_str)
    print(loaded_data)
