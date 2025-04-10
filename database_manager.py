import os
import pandas as pd
import logging
from sqlalchemy import create_engine, Column, String, Float, Date, text, MetaData, Table, delete, select, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get database URL from environment variable
DATABASE_URL = os.environ.get('DATABASE_URL')

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)

# Define StockData model
class StockData(Base):
    __tablename__ = 'stock_data'
    
    id = Column(String, primary_key=True)  # Composite key: symbol_date
    symbol = Column(String, index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    
    def __repr__(self):
        return f"<StockData(symbol='{self.symbol}', date='{self.date}')>"


def initialize_db():
    """Create all tables if they don't exist"""
    try:
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        return False


def save_to_db(data, symbol, date_str=None):
    """
    Save stock data to database
    
    Args:
        data (pd.DataFrame): Stock data to save
        symbol (str): Stock symbol
        date_str (str, optional): Date string in YYYY-MM-DD format. If None, uses date from DataFrame.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if data is None or data.empty:
            logger.warning(f"No data to save for {symbol}")
            return False
        
        # Make a copy to avoid modifying the original DataFrame
        df = data.copy()
        
        # Reset index if Date is the index
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()
            
        # Handle the case where 'Date' column might not exist
        if 'Date' not in df.columns:
            # If date_str is provided, use it
            if date_str:
                df['Date'] = pd.to_datetime(date_str)
            # If df has a 'date' column (lowercase), use it
            elif 'date' in df.columns:
                df['Date'] = df['date']
            else:
                logger.error(f"Date column not found in data for {symbol} and no date_str provided")
                return False
            
        # Convert Date to datetime if it's not already
        if not pd.api.types.is_datetime64_dtype(df['Date']):
            df['Date'] = pd.to_datetime(df['Date'])
            
        # Add symbol column if it doesn't exist
        if 'Symbol' not in df.columns:
            df['Symbol'] = symbol
        
        # Rename columns to match SQLAlchemy model
        df = df.rename(columns={
            'Date': 'date',
            'Symbol': 'symbol',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        
        # Create ID column (symbol_date)
        # Safely format the date to handle both datetime and Series objects
        def format_date(date_val):
            if hasattr(date_val, 'strftime'):
                return date_val.strftime('%Y-%m-%d')
            else:
                # Convert to string in case it's not a datetime object
                return str(date_val).split(' ')[0]
                
        df['id'] = df.apply(lambda row: f"{row['symbol']}_{format_date(row['date'])}", axis=1)
        
        # Keep only necessary columns
        columns = ['id', 'symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        df = df[[col for col in columns if col in df.columns]]
        
        # Insert data into database
        with Session() as session:
            # For each row, check if it exists and update or insert
            for _, row in df.iterrows():
                # Check if the record exists
                existing = session.query(StockData).filter_by(id=row['id']).first()
                
                if existing:
                    # Update existing record
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        if col in df.columns:
                            setattr(existing, col, row[col])
                else:
                    # Insert new record
                    session.add(StockData(
                        id=row['id'],
                        symbol=row['symbol'],
                        date=row['date'],
                        open=row.get('open'),
                        high=row.get('high'),
                        low=row.get('low'),
                        close=row.get('close'),
                        volume=row.get('volume')
                    ))
            
            session.commit()
            
        logger.info(f"Successfully saved {len(df)} records for {symbol} to database")
        return True
        
    except Exception as e:
        logger.error(f"Error saving {symbol} data to database: {str(e)}")
        return False


def load_from_db(date_str):
    """
    Load stock data for a specific date from the database
    
    Args:
        date_str (str): Date string in YYYY-MM-DD format
    
    Returns:
        pd.DataFrame: Combined DataFrame with all stocks for the date
    """
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        # Query the database
        with Session() as session:
            query = session.query(StockData).filter(StockData.date == date_obj)
            results = query.all()
            
            if not results:
                logger.warning(f"No data found for date {date_str}")
                return pd.DataFrame()
                
            # Convert results to a DataFrame
            data = []
            for result in results:
                data.append({
                    'Symbol': result.symbol,
                    'Date': result.date,
                    'Open': result.open,
                    'High': result.high,
                    'Low': result.low,
                    'Close': result.close,
                    'Volume': result.volume
                })
                
            df = pd.DataFrame(data)
            
            # Set Date and Symbol as index
            if not df.empty:
                df = df.set_index(['Date', 'Symbol'])
                
            logger.info(f"Successfully loaded {len(df)} records for {date_str} from database")
            return df
            
    except Exception as e:
        logger.error(f"Error loading data for {date_str} from database: {str(e)}")
        return pd.DataFrame()


def load_stock_data_from_db(symbol, start_date_str, end_date_str):
    """
    Load data for a specific stock across a date range from the database
    
    Args:
        symbol (str): Stock symbol
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format
    
    Returns:
        pd.DataFrame: DataFrame with stock data for the specified range
    """
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        
        # Query the database
        with Session() as session:
            query = session.query(StockData).filter(
                StockData.symbol == symbol,
                StockData.date >= start_date,
                StockData.date <= end_date
            ).order_by(StockData.date)
            
            results = query.all()
            
            if not results:
                logger.warning(f"No data found for {symbol} between {start_date_str} and {end_date_str}")
                return pd.DataFrame()
                
            # Convert results to a DataFrame
            data = []
            for result in results:
                data.append({
                    'Date': result.date,
                    'Open': result.open,
                    'High': result.high,
                    'Low': result.low,
                    'Close': result.close,
                    'Volume': result.volume
                })
                
            df = pd.DataFrame(data)
            
            # Set Date as index
            if not df.empty:
                df = df.set_index('Date')
                
            logger.info(f"Successfully loaded {len(df)} records for {symbol} from database")
            return df
            
    except Exception as e:
        logger.error(f"Error loading data for {symbol} from database: {str(e)}")
        return pd.DataFrame()


def get_available_dates_from_db():
    """
    Get a list of all dates for which data is available in the database
    
    Returns:
        list: List of available dates in YYYY-MM-DD format
    """
    try:
        with Session() as session:
            query = session.query(StockData.date).distinct().order_by(StockData.date)
            results = query.all()
            
            dates = [result[0].strftime('%Y-%m-%d') for result in results]
            
            logger.info(f"Found {len(dates)} available dates in database")
            return dates
            
    except Exception as e:
        logger.error(f"Error getting available dates from database: {str(e)}")
        return []


def get_available_symbols_from_db(date_str=None):
    """
    Get a list of all available stock symbols from the database
    
    Args:
        date_str (str, optional): Date to get symbols for. If None, gets symbols across all dates.
    
    Returns:
        list: List of available stock symbols
    """
    try:
        with Session() as session:
            if date_str:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                query = session.query(StockData.symbol).filter(StockData.date == date_obj).distinct()
            else:
                query = session.query(StockData.symbol).distinct()
                
            results = query.all()
            
            symbols = [result[0] for result in results]
            
            if date_str:
                logger.info(f"Found {len(symbols)} available symbols for {date_str} in database")
            else:
                logger.info(f"Found {len(symbols)} available symbols in database")
                
            return symbols
            
    except Exception as e:
        if date_str:
            logger.error(f"Error getting available symbols for {date_str} from database: {str(e)}")
        else:
            logger.error(f"Error getting available symbols from database: {str(e)}")
        return []


def clear_data_for_date(date_str):
    """
    Clear all stock data for a specific date
    
    Args:
        date_str (str): Date string in YYYY-MM-DD format
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        with Session() as session:
            query = delete(StockData).where(StockData.date == date_obj)
            result = session.execute(query)
            session.commit()
            
            logger.info(f"Cleared {result.rowcount} records for {date_str} from database")
            return True
            
    except Exception as e:
        logger.error(f"Error clearing data for {date_str} from database: {str(e)}")
        return False


def migrate_csv_to_db():
    """
    Migrate existing CSV data to the database
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        from data_storage import get_available_dates, load_data
        
        # Get all available dates from CSV
        csv_dates = get_available_dates()
        
        if not csv_dates:
            logger.info("No CSV data to migrate")
            return True
            
        logger.info(f"Starting migration of {len(csv_dates)} dates from CSV to database")
        
        # For each date, load data from CSV and save to database
        for date_str in csv_dates:
            logger.info(f"Migrating data for {date_str}")
            
            # Load data from CSV
            df = load_data(date_str)
            
            if df.empty:
                logger.warning(f"No data found for {date_str} in CSV")
                continue
                
            # Reset index to get Date and Symbol as columns
            df = df.reset_index()
            
            # Group by Symbol and save each stock's data
            for symbol, group in df.groupby('Symbol'):
                save_to_db(group, symbol)
                
        logger.info("Migration from CSV to database completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error migrating CSV data to database: {str(e)}")
        return False