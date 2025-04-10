import pandas as pd
import numpy as np
import re
import datetime
import logging
from data_storage import load_data, load_stock_data, get_available_dates, get_available_symbols
from analysis import (
    get_price_change, calculate_moving_averages, 
    detect_spikes, get_best_performers, 
    get_worst_performers, analyze_volume,
    stocks_above_ma
)
from visualizations import (
    plot_stock_price, plot_comparison, 
    plot_moving_averages, plot_volume_analysis,
    plot_top_performers
)
from utils import get_date_range

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def preprocess_query(query):
    """
    Preprocess the query text
    
    Args:
        query (str): The raw query string
        
    Returns:
        str: Preprocessed query
    """
    # Convert to lowercase
    query = query.lower()
    
    # Remove punctuation except for dots in stock symbols (e.g., RELIANCE.NS)
    query = re.sub(r'[^\w\s.]', ' ', query)
    
    # Replace multiple spaces with a single space
    query = re.sub(r'\s+', ' ', query).strip()
    
    return query

def extract_date_range(query, context_date):
    """
    Extract date range from query
    
    Args:
        query (str): Preprocessed query
        context_date (str): Current context date (YYYY-MM-DD)
        
    Returns:
        tuple: (start_date_str, end_date_str)
    """
    # Default to context_date for end date
    end_date_str = context_date
    
    # Convert context_date to datetime
    context_date_dt = datetime.datetime.strptime(context_date, "%Y-%m-%d").date()
    
    # Default start date is one week ago
    start_date_dt = context_date_dt - datetime.timedelta(days=7)
    
    # Check for time expressions
    if 'today' in query:
        # Keep end_date as context_date
        pass
    elif 'yesterday' in query:
        # Find the previous available date
        available_dates = get_available_dates()
        if available_dates and context_date in available_dates:
            idx = available_dates.index(context_date)
            if idx > 0:
                end_date_str = available_dates[idx - 1]
                start_date_dt = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date() - datetime.timedelta(days=1)
    elif 'this week' in query:
        # Start from the beginning of the current week
        start_date_dt = context_date_dt - datetime.timedelta(days=context_date_dt.weekday())
    elif 'this month' in query:
        # Start from the beginning of the current month
        start_date_dt = context_date_dt.replace(day=1)
    elif 'last week' in query:
        # Previous week
        end_of_last_week = context_date_dt - datetime.timedelta(days=context_date_dt.weekday() + 1)
        start_date_dt = end_of_last_week - datetime.timedelta(days=6)
        end_date_str = end_of_last_week.strftime("%Y-%m-%d")
    elif 'last month' in query:
        # Previous month
        if context_date_dt.month == 1:
            previous_month = context_date_dt.replace(year=context_date_dt.year - 1, month=12, day=1)
        else:
            previous_month = context_date_dt.replace(month=context_date_dt.month - 1, day=1)
        
        start_date_dt = previous_month
        # Last day of previous month
        if previous_month.month == 12:
            end_of_month = previous_month.replace(year=previous_month.year + 1, month=1, day=1) - datetime.timedelta(days=1)
        else:
            end_of_month = previous_month.replace(month=previous_month.month + 1, day=1) - datetime.timedelta(days=1)
        
        end_date_str = end_of_month.strftime("%Y-%m-%d")
    
    # Check for specific time ranges
    day_match = re.search(r'(\d+)\s+days?', query)
    week_match = re.search(r'(\d+)\s+weeks?', query)
    month_match = re.search(r'(\d+)\s+months?', query)
    
    if day_match:
        days = int(day_match.group(1))
        start_date_dt = context_date_dt - datetime.timedelta(days=days)
    elif week_match:
        weeks = int(week_match.group(1))
        start_date_dt = context_date_dt - datetime.timedelta(days=weeks * 7)
    elif month_match:
        months = int(month_match.group(1))
        # Approximate month calculation
        month_num = context_date_dt.month - months
        year_adj = 0
        while month_num <= 0:
            month_num += 12
            year_adj -= 1
        
        start_date_dt = context_date_dt.replace(year=context_date_dt.year + year_adj, month=month_num, day=1)
    
    # Convert to string format
    start_date_str = start_date_dt.strftime("%Y-%m-%d")
    
    return start_date_str, end_date_str

def extract_symbols(query):
    """
    Extract stock symbols from query
    
    Args:
        query (str): Preprocessed query
        
    Returns:
        list: List of stock symbols
    """
    # Get all available symbols
    all_symbols = get_available_symbols()
    if not all_symbols:
        return []
    
    # Create a set of lowercase symbols and their base names (without .NS)
    all_symbols_lower = {s.lower(): s for s in all_symbols}
    all_symbols_base = {s.lower().split('.')[0]: s for s in all_symbols}
    
    # Look for exact matches with .NS
    extracted_symbols = []
    for s in all_symbols_lower:
        if s in query:
            extracted_symbols.append(all_symbols_lower[s])
    
    # If no exact matches, look for base symbol names
    if not extracted_symbols:
        for s in all_symbols_base:
            # Make sure it's a standalone word
            pattern = r'\b' + re.escape(s) + r'\b'
            if re.search(pattern, query):
                extracted_symbols.append(all_symbols_base[s])
    
    return extracted_symbols

def extract_number(query):
    """
    Extract a number from the query
    
    Args:
        query (str): Preprocessed query
        
    Returns:
        int: Extracted number or default value
    """
    # Look for numbers
    number_match = re.search(r'\b(\d+)\b', query)
    if number_match:
        return int(number_match.group(1))
    
    # Look for words representing numbers
    number_words = {
        'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
        'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10
    }
    
    for word, value in number_words.items():
        if re.search(r'\b' + word + r'\b', query):
            return value
    
    # Default value
    return 5  # Common default for "top N" queries

def identify_query_intent(query):
    """
    Identify the intent of the query
    
    Args:
        query (str): Preprocessed query
        
    Returns:
        str: Query intent
    """
    # Define intent patterns
    intent_patterns = {
        'top_gainers': [
            r'(top|best).*gain', r'gain.*most', r'perform.*best',
            r'highest.*return', r'most.*profit', r'biggest.*rise'
        ],
        'top_losers': [
            r'(top|worst).*los', r'los.*most', r'perform.*worst',
            r'lowest.*return', r'most.*loss', r'biggest.*drop', r'biggest.*fall'
        ],
        'price_trend': [
            r'(price|trend|movement|chart|graph).*for', r'show.*price',
            r'how.*price', r'price.*history', r'price.*trend'
        ],
        'compare_stocks': [
            r'compare', r'vs', r'versus', r'against', r'difference.*between',
            r'perform.*better', r'which.*better'
        ],
        'moving_average': [
            r'moving.*average', r'ma', r'above.*average', r'below.*average',
            r'cross.*average', r'average.*price'
        ],
        'volume_analysis': [
            r'volume', r'trading.*volume', r'high.*volume', r'unusual.*volume'
        ],
        'price_spike': [
            r'spike', r'jump', r'surge', r'plunge', r'crash', r'sudden',
            r'anomaly', r'unusual.*movement'
        ],
        'current_price': [
            r'current.*price', r'what.*price', r'latest.*price',
            r'price.*now', r'how much.*cost'
        ]
    }
    
    # Check each pattern
    for intent, patterns in intent_patterns.items():
        for pattern in patterns:
            if re.search(pattern, query):
                return intent
    
    # Default intent
    return 'general_info'

def process_query(query, context_date):
    """
    Process a natural language query
    
    Args:
        query (str): The user's query
        context_date (str): Current context date (YYYY-MM-DD)
        
    Returns:
        tuple: (result, explanation, visualization)
    """
    try:
        # Preprocess the query
        processed_query = preprocess_query(query)
        logger.info(f"Processed query: {processed_query}")
        
        # Extract information from the query
        start_date, end_date = extract_date_range(processed_query, context_date)
        symbols = extract_symbols(processed_query)
        limit = extract_number(processed_query)
        
        # Identify query intent
        intent = identify_query_intent(processed_query)
        logger.info(f"Query intent: {intent}")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Detected symbols: {symbols}")
        logger.info(f"Limit: {limit}")
        
        # Process the query based on intent
        if intent == 'top_gainers':
            # Find top gainers
            result = get_best_performers(start_date, end_date, limit=limit)
            explanation = f"Showing the top {limit} gaining stocks from {start_date} to {end_date}"
            visualization = plot_top_performers(result, metric='Return (%)', top_n=limit, ascending=False) if not result.empty else None
        
        elif intent == 'top_losers':
            # Find top losers
            result = get_worst_performers(start_date, end_date, limit=limit)
            explanation = f"Showing the top {limit} losing stocks from {start_date} to {end_date}"
            visualization = plot_top_performers(result, metric='Return (%)', top_n=limit, ascending=True) if not result.empty else None
        
        elif intent == 'price_trend':
            # Show price trend for a stock
            if not symbols:
                return None, "Could not identify any stock symbols in your query. Please specify a stock symbol.", None
            
            # Take the first symbol for analysis
            symbol = symbols[0]
            result = load_stock_data(symbol, start_date, end_date)
            explanation = f"Showing price trend for {symbol} from {start_date} to {end_date}"
            visualization = plot_stock_price(symbol, start_date, end_date)
        
        elif intent == 'compare_stocks':
            # Compare multiple stocks
            if len(symbols) < 2:
                if len(symbols) == 1:
                    # Get some popular stocks to compare with
                    all_symbols = get_available_symbols()
                    popular_symbols = ['RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS']
                    compare_symbols = [s for s in popular_symbols if s in all_symbols and s != symbols[0]]
                    symbols.extend(compare_symbols[:2])  # Add 2 popular stocks
                else:
                    return None, "Could not identify enough stock symbols to compare. Please specify at least two stock symbols.", None
            
            # Limit to maximum 5 symbols for readability
            if len(symbols) > 5:
                symbols = symbols[:5]
                
            result = pd.DataFrame()
            for symbol in symbols:
                data = load_stock_data(symbol, start_date, end_date)
                if not data.empty:
                    result = pd.concat([result, data])
            
            explanation = f"Comparing performance of {', '.join(symbols)} from {start_date} to {end_date}"
            visualization = plot_comparison(symbols, start_date, end_date)
        
        elif intent == 'moving_average':
            # Analyze moving averages
            if not symbols:
                # Check if query asks for stocks above/below MA
                if 'above' in processed_query or 'over' in processed_query:
                    # Find stocks trading above their MA
                    ma_days = 10  # Default
                    
                    # Extract MA period if specified
                    ma_match = re.search(r'(\d+)[ -]day', processed_query)
                    if ma_match:
                        ma_days = int(ma_match.group(1))
                    
                    result = stocks_above_ma(context_date, ma_window=ma_days)
                    
                    if result.empty:
                        return None, f"Could not find stocks trading above their {ma_days}-day moving average.", None
                    
                    result = result.head(limit)
                    explanation = f"Showing top {limit} stocks trading above their {ma_days}-day moving average as of {context_date}"
                    visualization = plot_top_performers(result, metric='Difference (%)', top_n=limit) if not result.empty else None
                
                else:
                    return None, "Could not identify any stock symbols in your query. Please specify a stock symbol for moving average analysis.", None
            else:
                # Take the first symbol for analysis
                symbol = symbols[0]
                
                # Extract MA periods if specified
                short_window = 5  # Default
                long_window = 20  # Default
                
                short_ma_match = re.search(r'(\d+)[ -]day.*short', processed_query)
                long_ma_match = re.search(r'(\d+)[ -]day.*long', processed_query)
                
                if short_ma_match:
                    short_window = int(short_ma_match.group(1))
                
                if long_ma_match:
                    long_window = int(long_ma_match.group(1))
                
                # Ensure short window is shorter than long window
                if short_window >= long_window:
                    short_window, long_window = 5, 20
                
                # Calculate moving averages
                ma_data = calculate_moving_averages(symbol, start_date, end_date, short_window, long_window)
                
                if ma_data is None:
                    return None, f"Insufficient data to calculate moving averages for {symbol}.", None
                
                result = ma_data
                explanation = f"Showing {short_window}-day and {long_window}-day moving averages for {symbol} from {start_date} to {end_date}"
                visualization = plot_moving_averages(ma_data, symbol, short_window, long_window)
        
        elif intent == 'volume_analysis':
            # Analyze trading volume
            if not symbols:
                # Find high volume stocks
                explanation = f"Showing stocks with high trading volume as of {context_date}"
                
                # Load data for the day
                daily_data = load_data(context_date)
                
                if daily_data.empty:
                    return None, f"No data available for {context_date}.", None
                
                # Calculate average volume
                result = daily_data.groupby('Symbol')['Volume'].mean().reset_index()
                result = result.sort_values('Volume', ascending=False).head(limit)
                
                # Add symbol names for display
                visualization = None
            else:
                # Take the first symbol for analysis
                symbol = symbols[0]
                
                # Analyze volume
                volume_data = analyze_volume(symbol, start_date, end_date)
                
                if volume_data is None:
                    return None, f"Insufficient data to analyze volume for {symbol}.", None
                
                result = volume_data
                explanation = f"Showing volume analysis for {symbol} from {start_date} to {end_date}"
                visualization = plot_volume_analysis(volume_data, symbol)
        
        elif intent == 'price_spike':
            # Detect price spikes
            if not symbols:
                return None, "Could not identify any stock symbols in your query. Please specify a stock symbol for spike detection.", None
            
            # Take the first symbol for analysis
            symbol = symbols[0]
            
            # Detect spikes
            spikes = detect_spikes(symbol, start_date, end_date)
            
            if spikes is None or spikes.empty:
                return None, f"No significant price or volume spikes detected for {symbol} in the specified period.", None
            
            result = spikes
            explanation = f"Showing detected price and volume spikes for {symbol} from {start_date} to {end_date}"
            
            # Show price chart with spike points
            stock_data = load_stock_data(symbol, start_date, end_date)
            visualization = plot_stock_price(symbol, start_date, end_date)
        
        elif intent == 'current_price':
            # Get current price
            if not symbols:
                return None, "Could not identify any stock symbols in your query. Please specify a stock symbol.", None
            
            # Take the first symbol for analysis
            symbol = symbols[0]
            
            # Load latest data
            latest_data = load_data(context_date)
            
            if latest_data.empty:
                return None, f"No data available for {context_date}.", None
            
            # Filter for the symbol
            symbol_data = latest_data[latest_data['Symbol'] == symbol]
            
            if symbol_data.empty:
                return None, f"No data available for {symbol} on {context_date}.", None
            
            # Get the latest price
            latest_price = symbol_data.iloc[0]['Close']
            
            result = f"The latest price of {symbol} as of {context_date} is ₹{latest_price:.2f}"
            explanation = f"Showing current price for {symbol}"
            visualization = None
        
        else:  # General info
            if symbols:
                # Show general info for the symbol
                symbol = symbols[0]
                
                # Load data
                stock_data = load_stock_data(symbol, start_date, end_date)
                
                if stock_data.empty:
                    return None, f"No data available for {symbol} from {start_date} to {end_date}.", None
                
                # Calculate basic statistics
                start_price = stock_data.iloc[0]['Close']
                end_price = stock_data.iloc[-1]['Close']
                price_change = ((end_price / start_price) - 1) * 100
                
                high = stock_data['High'].max()
                low = stock_data['Low'].min()
                avg_volume = stock_data['Volume'].mean()
                
                # Create result summary
                result = f"""
                Symbol: {symbol}
                Period: {start_date} to {end_date}
                
                Last Price: ₹{end_price:.2f}
                Change: {price_change:.2f}%
                
                High: ₹{high:.2f}
                Low: ₹{low:.2f}
                Average Volume: {avg_volume:.0f}
                """
                
                explanation = f"Showing general information for {symbol} from {start_date} to {end_date}"
                visualization = plot_stock_price(symbol, start_date, end_date)
            
            else:
                # Show market summary
                # Load data for the day
                daily_data = load_data(context_date)
                
                if daily_data.empty:
                    return None, f"No data available for {context_date}.", None
                
                # Calculate price changes
                price_changes = get_price_change(daily_data)
                
                if price_changes.empty:
                    return None, f"Could not calculate price changes for {context_date}.", None
                
                # Market summary
                gainers = price_changes[price_changes['Change %'] > 0]
                losers = price_changes[price_changes['Change %'] < 0]
                
                num_gainers = len(gainers)
                num_losers = len(losers)
                total_stocks = len(price_changes)
                
                avg_change = price_changes['Change %'].mean()
                
                result = f"""
                Market Summary for {context_date}
                
                Total Stocks: {total_stocks}
                Gainers: {num_gainers} ({num_gainers/total_stocks*100:.1f}%)
                Losers: {num_losers} ({num_losers/total_stocks*100:.1f}%)
                
                Average Change: {avg_change:.2f}%
                
                Top Gainer: {price_changes.iloc[0]['Symbol']} ({price_changes.iloc[0]['Change %']:.2f}%)
                Top Loser: {price_changes.iloc[-1]['Symbol']} ({price_changes.iloc[-1]['Change %']:.2f}%)
                """
                
                explanation = f"Showing market summary for {context_date}"
                visualization = None
        
        return result, explanation, visualization
    
    except Exception as e:
        logger.error(f"Error processing query: {str(e)}")
        return None, f"An error occurred while processing your query: {str(e)}", None

if __name__ == "__main__":
    # Test the query processor
    test_queries = [
        "Which stock gained the most today?",
        "Show me price trends for INFY over the last 7 days",
        "What are the top 5 stocks trading above their 10-day average?",
        "Compare TCS and RELIANCE performance this week"
    ]
    
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    for query in test_queries:
        print(f"\nProcessing query: {query}")
        result, explanation, _ = process_query(query, today_str)
        
        print(f"Explanation: {explanation}")
        if isinstance(result, pd.DataFrame):
            print(f"Result: DataFrame with {len(result)} rows")
        else:
            print(f"Result: {result}")
