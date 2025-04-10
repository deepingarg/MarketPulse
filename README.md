
# Indian Stock Market Analysis 📈

A Streamlit-based web application for analyzing Indian stock market data, focusing on Nifty 50 stocks.

## Features

- **Real-time Data Fetching**: Fetch stock data from Yahoo Finance API
- **Technical Analysis**: 
  - Price trends and moving averages
  - Volume analysis
  - Performance comparison
  - Price spike detection
- **Natural Language Queries**: Ask questions about stocks in plain English
- **Interactive Visualizations**: 
  - Candlestick charts
  - Performance comparisons
  - Volume analysis charts
  - Moving average indicators

## Getting Started

1. Click the "Run" button in the Replit interface
2. Wait for the Streamlit server to start
3. The app will be available at the URL shown in the console

## Usage

### Fetching Data
1. Navigate to the "Fetch Data" section
2. Select date range and stocks
3. Click "Fetch Data" to download market data

### Analysis
- Use "Stock Analysis" for technical indicators
- Try "Query Assistant" for natural language analysis
- Explore "Data Visualization" for custom charts

### Data Storage
- Stock data is stored in both database and CSV formats
- Automatic data migration between storage systems
- Historical data is preserved and organized by date

## Dependencies

- streamlit
- yfinance
- pandas
- plotly
- SQLAlchemy
- BeautifulSoup4

## Project Structure

- `app.py`: Main Streamlit application
- `data_fetcher.py`: Stock data fetching logic
- `analysis.py`: Technical analysis functions
- `visualizations.py`: Plotting and chart creation
- `nlp_processor.py`: Natural language query processing
- `database_manager.py`: Database operations
- `data_storage.py`: CSV data storage operations
- `utils.py`: Utility functions

## Contributing

Feel free to fork this project and submit pull requests with improvements!

## License

This project is open source and available under the MIT License.
