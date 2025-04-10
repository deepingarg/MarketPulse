import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
# Import database functions as primary data source
from database_manager import (
    load_from_db as load_data,
    load_stock_data_from_db as load_stock_data,
    get_available_dates_from_db as get_available_dates,
    get_available_symbols_from_db as get_available_symbols
)
# Import CSV functions as fallback
from data_storage import (
    load_data as load_data_csv,
    load_stock_data as load_stock_data_csv
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_price_change(data, previous_day_data=None):
    """
    Calculate price changes between current data and previous day
    
    Args:
        data (pd.DataFrame): Current day's stock data
        previous_day_data (pd.DataFrame, optional): Previous day's stock data
        
    Returns:
        pd.DataFrame: DataFrame with price changes
    """
    try:
        # Reset index if needed to get Symbol and Date as columns
        if 'Symbol' not in data.columns and isinstance(data.index, pd.MultiIndex):
            data = data.reset_index()
        
        # If previous_day_data is provided, also ensure Symbol column exists
        if previous_day_data is not None and 'Symbol' not in previous_day_data.columns and isinstance(previous_day_data.index, pd.MultiIndex):
            previous_day_data = previous_day_data.reset_index()
            
        # If no previous day data provided, try to get it
        if previous_day_data is None:
            # Get available dates
            dates = get_available_dates()
            
            if len(dates) < 2:
                logger.warning("Insufficient historical data for price change calculation")
                
                # Calculate intraday changes instead
                changes = []
                for symbol, group in data.groupby('Symbol'):
                    if len(group) > 0:
                        row = group.iloc[-1]
                        
                        # Calculate intraday change
                        change = row['Close'] - row['Open']
                        change_pct = (change / row['Open'] * 100) if row['Open'] > 0 else 0
                        
                        changes.append({
                            'Symbol': symbol,
                            'Close': row['Close'],
                            'Previous': row['Open'],
                            'Change': change,
                            'Change_Pct': change_pct  # Use underscore instead of space
                        })
                
                return pd.DataFrame(changes)
            
            # Get current date from data if possible
            if 'Date' in data.columns:
                current_date = data['Date'].iloc[0].strftime('%Y-%m-%d')
            else:
                current_date = max(dates)
            
            current_idx = dates.index(current_date) if current_date in dates else len(dates) - 1
            
            # Get previous trading day
            if current_idx > 0:
                previous_date = dates[current_idx - 1]
                previous_day_data = load_data(previous_date)
                
                # Reset index if needed to get Symbol as column
                if 'Symbol' not in previous_day_data.columns and isinstance(previous_day_data.index, pd.MultiIndex):
                    previous_day_data = previous_day_data.reset_index()
            else:
                logger.warning("No previous day data available")
                return pd.DataFrame()
        
        # Calculate price changes
        changes = []
        for symbol, group in data.groupby('Symbol'):
            if len(group) > 0:
                current_row = group.iloc[-1]
                
                # Find previous price for the same symbol
                symbol_previous_data = previous_day_data[previous_day_data['Symbol'] == symbol]
                if not symbol_previous_data.empty:
                    previous_price = symbol_previous_data['Close'].iloc[-1]
                    
                    # Calculate change
                    change = current_row['Close'] - previous_price
                    change_pct = (change / previous_price * 100) if previous_price > 0 else 0
                    
                    changes.append({
                        'Symbol': symbol,
                        'Close': current_row['Close'],
                        'Previous': previous_price,
                        'Change': change,
                        'Change_Pct': change_pct  # Use underscore instead of space
                    })
        
        return pd.DataFrame(changes)
    
    except Exception as e:
        logger.error(f"Error calculating price changes: {str(e)}")
        return pd.DataFrame()

def calculate_moving_averages(symbol, start_date_str, end_date_str, short_window=5, long_window=20):
    """
    Calculate moving averages for a stock
    
    Args:
        symbol (str): Stock symbol
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format
        short_window (int): Short moving average window
        long_window (int): Long moving average window
        
    Returns:
        pd.DataFrame: DataFrame with original data and moving averages
    """
    try:
        # Load stock data
        data = load_stock_data(symbol, start_date_str, end_date_str)
        
        if data.empty:
            logger.warning(f"No data available for {symbol} from {start_date_str} to {end_date_str}")
            return None
        
        # Need to have enough data for the long window
        if len(data) < long_window:
            logger.warning(f"Insufficient data for {symbol} to calculate {long_window}-day MA")
            return None
        
        # Sort by date to ensure correct calculation
        data = data.sort_values('Date')
        
        # Calculate moving averages
        data[f'MA_{short_window}'] = data['Close'].rolling(window=short_window).mean()
        data[f'MA_{long_window}'] = data['Close'].rolling(window=long_window).mean()
        
        # Drop NaN values
        data = data.dropna()
        
        return data
    
    except Exception as e:
        logger.error(f"Error calculating moving averages for {symbol}: {str(e)}")
        return None

def detect_spikes(symbol, start_date_str, end_date_str, threshold=2.0):
    """
    Detect significant price or volume spikes
    
    Args:
        symbol (str): Stock symbol
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format
        threshold (float): Standard deviation threshold for spike detection
        
    Returns:
        pd.DataFrame: DataFrame with detected spikes
    """
    try:
        # Load stock data
        data = load_stock_data(symbol, start_date_str, end_date_str)
        
        if data.empty:
            logger.warning(f"No data available for {symbol} from {start_date_str} to {end_date_str}")
            return None
        
        # Need to have enough data for meaningful statistics
        if len(data) < 5:
            logger.warning(f"Insufficient data for {symbol} to detect spikes")
            return None
        
        # Sort by date
        data = data.sort_values('Date')
        
        # Calculate daily returns
        data['Return'] = data['Close'].pct_change() * 100
        
        # Calculate volume change
        data['Volume_Change'] = data['Volume'].pct_change() * 100
        
        # Calculate means and standard deviations
        return_mean = data['Return'].mean()
        return_std = data['Return'].std()
        volume_mean = data['Volume_Change'].mean()
        volume_std = data['Volume_Change'].std()
        
        # Detect spikes
        spikes = []
        for _, row in data.iterrows():
            # Check for NaN values in the metrics
            if pd.isna(row['Return']) or pd.isna(row['Volume_Change']):
                continue
                
            # Check for price spikes
            price_zscore = abs((row['Return'] - return_mean) / return_std) if return_std > 0 else 0
            price_spike = price_zscore > threshold
            
            # Check for volume spikes
            volume_zscore = abs((row['Volume_Change'] - volume_mean) / volume_std) if volume_std > 0 else 0
            volume_spike = volume_zscore > threshold
            
            if price_spike or volume_spike:
                spike_type = []
                if price_spike:
                    direction = "up" if row['Return'] > 0 else "down"
                    spike_type.append(f"price {direction}")
                if volume_spike:
                    spike_type.append("volume")
                
                spikes.append({
                    'Date': row['Date'],
                    'Close': row['Close'],
                    'Return': row['Return'],
                    'Volume': row['Volume'],
                    'Volume_Change': row['Volume_Change'],
                    'Type': ", ".join(spike_type),
                    'Severity': max(price_zscore, volume_zscore)
                })
        
        return pd.DataFrame(spikes) if spikes else pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Error detecting spikes for {symbol}: {str(e)}")
        return None

def get_best_performers(start_date_str, end_date_str, limit=10, metric='return'):
    """
    Get the best performing stocks in a given period
    
    Args:
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format
        limit (int): Number of stocks to return
        metric (str): Performance metric ('return', 'volatility', 'volume')
        
    Returns:
        pd.DataFrame: DataFrame with the best performers
    """
    try:
        # Get all available symbols
        symbols = get_available_symbols()
        
        if not symbols:
            logger.warning("No symbols available for performance ranking")
            return pd.DataFrame()
        
        # Calculate performance for each symbol
        performances = []
        
        for symbol in symbols:
            # Load stock data
            data = load_stock_data(symbol, start_date_str, end_date_str)
            
            if data.empty or len(data) < 2:
                continue
            
            # Sort by date
            data = data.sort_values('Date')
            
            # Calculate start and end prices
            start_price = data.iloc[0]['Close']
            end_price = data.iloc[-1]['Close']
            
            # Calculate return
            return_pct = ((end_price / start_price) - 1) * 100
            
            # Calculate volatility (standard deviation of daily returns)
            data['Daily_Return'] = data['Close'].pct_change() * 100
            volatility = data['Daily_Return'].std()
            
            # Calculate average volume
            avg_volume = data['Volume'].mean()
            
            # Add to performances list
            performances.append({
                'Symbol': symbol,
                'Start_Price': start_price,
                'End_Price': end_price,
                'Return (%)': return_pct,
                'Volatility (%)': volatility,
                'Avg_Volume': avg_volume
            })
        
        if not performances:
            logger.warning(f"No performance data available for period {start_date_str} to {end_date_str}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        perf_df = pd.DataFrame(performances)
        
        # Sort based on selected metric
        if metric == 'return':
            perf_df = perf_df.sort_values('Return (%)', ascending=False)
        elif metric == 'volatility':
            perf_df = perf_df.sort_values('Volatility (%)', ascending=False)
        elif metric == 'volume':
            perf_df = perf_df.sort_values('Avg_Volume', ascending=False)
        
        # Return top performers
        return perf_df.head(limit)
    
    except Exception as e:
        logger.error(f"Error getting best performers: {str(e)}")
        return pd.DataFrame()

def get_worst_performers(start_date_str, end_date_str, limit=10, metric='return'):
    """
    Get the worst performing stocks in a given period
    
    Args:
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format
        limit (int): Number of stocks to return
        metric (str): Performance metric ('return', 'volatility', 'volume')
        
    Returns:
        pd.DataFrame: DataFrame with the worst performers
    """
    try:
        # Get all available symbols
        symbols = get_available_symbols()
        
        if not symbols:
            logger.warning("No symbols available for performance ranking")
            return pd.DataFrame()
        
        # Calculate performance for each symbol
        performances = []
        
        for symbol in symbols:
            # Load stock data
            data = load_stock_data(symbol, start_date_str, end_date_str)
            
            if data.empty or len(data) < 2:
                continue
            
            # Sort by date
            data = data.sort_values('Date')
            
            # Calculate start and end prices
            start_price = data.iloc[0]['Close']
            end_price = data.iloc[-1]['Close']
            
            # Calculate return
            return_pct = ((end_price / start_price) - 1) * 100
            
            # Calculate volatility (standard deviation of daily returns)
            data['Daily_Return'] = data['Close'].pct_change() * 100
            volatility = data['Daily_Return'].std()
            
            # Calculate average volume
            avg_volume = data['Volume'].mean()
            
            # Add to performances list
            performances.append({
                'Symbol': symbol,
                'Start_Price': start_price,
                'End_Price': end_price,
                'Return (%)': return_pct,
                'Volatility (%)': volatility,
                'Avg_Volume': avg_volume
            })
        
        if not performances:
            logger.warning(f"No performance data available for period {start_date_str} to {end_date_str}")
            return pd.DataFrame()
        
        # Convert to DataFrame
        perf_df = pd.DataFrame(performances)
        
        # Sort based on selected metric (ascending for worst performers)
        if metric == 'return':
            perf_df = perf_df.sort_values('Return (%)', ascending=True)
        elif metric == 'volatility':
            perf_df = perf_df.sort_values('Volatility (%)', ascending=True)
        elif metric == 'volume':
            perf_df = perf_df.sort_values('Avg_Volume', ascending=True)
        
        # Return worst performers
        return perf_df.head(limit)
    
    except Exception as e:
        logger.error(f"Error getting worst performers: {str(e)}")
        return pd.DataFrame()

def analyze_volume(symbol, start_date_str, end_date_str):
    """
    Analyze trading volume for a stock
    
    Args:
        symbol (str): Stock symbol
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: DataFrame with volume analysis
    """
    try:
        # Load stock data
        data = load_stock_data(symbol, start_date_str, end_date_str)
        
        if data.empty:
            logger.warning(f"No data available for {symbol} from {start_date_str} to {end_date_str}")
            return None
        
        # Sort by date
        data = data.sort_values('Date')
        
        # Calculate volume metrics
        data['Volume_MA_5'] = data['Volume'].rolling(window=5).mean()
        data['Volume_Change'] = data['Volume'].pct_change() * 100
        
        # Calculate volume-price correlation
        if len(data) >= 3:  # Need at least 3 points for correlation
            price_changes = data['Close'].pct_change()
            volume_changes = data['Volume'].pct_change()
            
            # Remove NaN values
            valid_data = pd.concat([price_changes, volume_changes], axis=1).dropna()
            
            if len(valid_data) >= 3:
                correlation = valid_data.iloc[:, 0].corr(valid_data.iloc[:, 1])
                data['Volume_Price_Corr'] = correlation
        
        return data
    
    except Exception as e:
        logger.error(f"Error analyzing volume for {symbol}: {str(e)}")
        return None

def stocks_above_ma(date_str, ma_window=10):
    """
    Find stocks trading above their moving average
    
    Args:
        date_str (str): Date in YYYY-MM-DD format
        ma_window (int): Moving average window
        
    Returns:
        pd.DataFrame: DataFrame with stocks above their MA
    """
    try:
        # Get available dates
        dates = get_available_dates()
        
        if not dates or date_str not in dates:
            logger.warning(f"No data available for date {date_str}")
            return pd.DataFrame()
        
        # Get all symbols
        symbols = get_available_symbols(date_str)
        
        if not symbols:
            logger.warning(f"No symbols available for date {date_str}")
            return pd.DataFrame()
        
        # Find index of the current date
        current_idx = dates.index(date_str)
        
        # Need at least MA_window days of historical data
        if current_idx < ma_window:
            logger.warning(f"Insufficient historical data for {ma_window}-day MA calculation")
            return pd.DataFrame()
        
        # Get the date for MA_window days ago
        start_date = dates[current_idx - ma_window]
        
        # Check each symbol
        results = []
        for symbol in symbols:
            # Load stock data for the period
            data = load_stock_data(symbol, start_date, date_str)
            
            if data.empty or len(data) < ma_window:
                continue
            
            # Sort by date
            data = data.sort_values('Date')
            
            # Calculate moving average
            data[f'MA_{ma_window}'] = data['Close'].rolling(window=ma_window).mean()
            
            # Get the latest row
            latest = data.iloc[-1]
            
            # Check if close price is above MA
            if latest['Close'] > latest[f'MA_{ma_window}']:
                results.append({
                    'Symbol': symbol,
                    'Close': latest['Close'],
                    f'MA_{ma_window}': latest[f'MA_{ma_window}'],
                    'Difference (%)': ((latest['Close'] / latest[f'MA_{ma_window}']) - 1) * 100
                })
        
        # Convert to DataFrame and sort by difference
        if results:
            result_df = pd.DataFrame(results)
            return result_df.sort_values('Difference (%)', ascending=False)
        else:
            return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Error finding stocks above MA: {str(e)}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Test functionality
    today_str = datetime.now().strftime("%Y-%m-%d")
    a_week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    # Test moving averages
    print(f"Testing moving averages for 'RELIANCE.NS' from {a_week_ago} to {today_str}")
    ma_data = calculate_moving_averages('RELIANCE.NS', a_week_ago, today_str, 2, 5)
    if ma_data is not None:
        print(ma_data.tail())
