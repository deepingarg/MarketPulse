import streamlit as st
import pandas as pd
import datetime
import os
from data_fetcher import fetch_stock_data, get_nifty50_symbols
from database_manager import (
    initialize_db, save_to_db, load_from_db, load_stock_data_from_db,
    get_available_dates_from_db as get_available_dates,
    get_available_symbols_from_db, migrate_csv_to_db
)
# Import CSV functions for potential fallback
from data_storage import save_data as save_data_csv, load_data as load_data_csv, get_available_dates as get_available_dates_csv
from analysis import (
    get_price_change, calculate_moving_averages, 
    detect_spikes, get_best_performers, 
    get_worst_performers, analyze_volume
)
from visualizations import (
    plot_stock_price, plot_comparison, 
    plot_moving_averages, plot_volume_analysis
)
from nlp_processor import process_query
from utils import get_date_range

# Set page config
st.set_page_config(
    page_title="Indian Stock Market Analysis",
    page_icon="📈",
    layout="wide"
)

# Create data directory if it doesn't exist
if not os.path.exists("data"):
    os.makedirs("data")

# Initialize the database
initialize_db()

# Check for data in the database and migrate if needed
db_dates = get_available_dates()
csv_dates = get_available_dates_csv()

# If database is empty but CSV data exists, auto-migrate
if not db_dates and csv_dates:
    st.sidebar.warning("Database is empty. Migrating data from CSV...")
    # Use st.spinner (not available in sidebar)
    migration_status = st.sidebar.empty()
    migration_status.info("Migrating data from CSV to database...")
    success = migrate_csv_to_db()
    if success:
        migration_status.success("✅ Data migration completed successfully")
        # Refresh available dates after migration
        db_dates = get_available_dates()
    else:
        migration_status.error("❌ Error during data migration")

# Add a database migration option in the sidebar
if st.sidebar.checkbox("Database Options", False):
    st.sidebar.info("Database operations")
    
    if st.sidebar.button("Migrate CSV Data to Database"):
        # Use empty placeholder for status updates
        migration_status_manual = st.sidebar.empty()
        migration_status_manual.info("Migrating data from CSV to database...")
        success = migrate_csv_to_db()
        if success:
            migration_status_manual.success("✅ Data migration completed successfully")
            # Refresh available dates after migration
            db_dates = get_available_dates()
        else:
            migration_status_manual.error("❌ Error during data migration")

# Main title
st.title("📊 Indian Stock Market Analyzer")
st.markdown("---")

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Choose a function:",
    ["Home", "Fetch Data", "Stock Analysis", "Query Assistant", "Data Visualization"]
)

# Home page
if page == "Home":
    st.header("Welcome to the Indian Stock Market Analyzer")
    
    st.markdown("""
    This tool allows you to:
    - Fetch and store Indian stock market data
    - Analyze stock performance
    - Query stock information using natural language
    - Visualize stock trends
                
    ### Getting Started
    1. Go to the **Fetch Data** section to download the latest stock data
    2. Use the **Stock Analysis** section to perform technical analysis
    3. Try the **Query Assistant** to ask questions in plain English
    4. Explore the **Data Visualization** section for charts and graphs
    """)
    
    # Check and show last updated data
    dates = get_available_dates()
    if dates:
        st.success(f"📅 Data available from {min(dates)} to {max(dates)}")
        
        # Show sample of available data
        st.subheader("Sample of Available Data")
        latest_date = max(dates)
        sample_data = load_from_db(latest_date)
        
        if not sample_data.empty:
            st.dataframe(sample_data.head(5))
        else:
            st.warning("No data available for the latest date. Please fetch data first.")
    else:
        st.warning("No data available. Please go to the 'Fetch Data' section to download stock data.")

# Fetch Data page
elif page == "Fetch Data":
    st.header("Fetch Indian Stock Market Data")
    
    # Date selector
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input(
            "Start Date",
            datetime.date.today() - datetime.timedelta(days=30),
            max_value=datetime.date.today()
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            datetime.date.today(),
            max_value=datetime.date.today()
        )
    
    # Validate date range
    if start_date > end_date:
        st.error("Error: End date must fall after start date.")
    
    # Stock selection
    st.subheader("Select Stocks")
    
    selection_method = st.radio(
        "Selection Method",
        ["Nifty 50", "Custom Symbols"]
    )
    
    if selection_method == "Nifty 50":
        fetch_all = st.checkbox("Fetch all Nifty 50 stocks", value=True)
        
        if not fetch_all:
            # Get Nifty50 symbols
            symbols = get_nifty50_symbols()
            if symbols:
                selected_symbols = st.multiselect(
                    "Select specific Nifty 50 stocks:",
                    options=symbols,
                    default=symbols[:5]
                )
            else:
                st.error("Failed to fetch Nifty 50 symbols. Please use custom symbols instead.")
                selected_symbols = []
        else:
            selected_symbols = get_nifty50_symbols()
            if selected_symbols:
                st.info(f"Will fetch data for all {len(selected_symbols)} Nifty 50 stocks")
            else:
                st.error("Failed to fetch Nifty 50 symbols.")
                selected_symbols = []
    
    else:  # Custom Symbols
        custom_symbols_input = st.text_input(
            "Enter stock symbols (comma-separated, e.g., RELIANCE.NS, TCS.NS, INFY.NS)",
            "RELIANCE.NS, TCS.NS, HDFCBANK.NS, INFY.NS, ICICIBANK.NS"
        )
        selected_symbols = [symbol.strip() for symbol in custom_symbols_input.split(",") if symbol.strip()]
    
    # Fetch button
    if st.button("Fetch Data") and selected_symbols:
        with st.spinner("Fetching stock data..."):
            progress_bar = st.progress(0)
            
            successful_fetches = 0
            failed_fetches = 0
            
            for i, symbol in enumerate(selected_symbols):
                try:
                    # Update progress
                    progress = (i + 1) / len(selected_symbols)
                    progress_bar.progress(progress)
                    
                    # Fetch data for the symbol
                    stock_data = fetch_stock_data(symbol, start_date, end_date)
                    
                    if stock_data is not None and not stock_data.empty:
                        # Save data by date
                        # Check if index is DatetimeIndex
                        if isinstance(stock_data.index, pd.DatetimeIndex):
                            # Group by date
                            for date, group in stock_data.groupby(stock_data.index.date):
                                date_str = date.strftime("%Y-%m-%d")
                                # Save to database
                                save_to_db(group, symbol, date_str)
                                # Also save to CSV as backup
                                save_data_csv(group, symbol, date_str)
                        else:
                            # Handle case where index might not be a DatetimeIndex
                            date_str = datetime.date.today().strftime("%Y-%m-%d")
                            # Save to database
                            save_to_db(stock_data, symbol, date_str)
                            # Also save to CSV as backup
                            save_data_csv(stock_data, symbol, date_str)
                        
                        successful_fetches += 1
                        st.success(f"Successfully fetched data for {symbol}")
                    else:
                        failed_fetches += 1
                        st.error(f"No data available for {symbol}")
                
                except Exception as e:
                    failed_fetches += 1
                    st.error(f"Error fetching data for {symbol}: {str(e)}")
            
            # Final status
            if successful_fetches > 0:
                st.success(f"✅ Successfully fetched data for {successful_fetches} stocks.")
            if failed_fetches > 0:
                st.warning(f"⚠️ Failed to fetch data for {failed_fetches} stocks.")

# Stock Analysis page
elif page == "Stock Analysis":
    st.header("Stock Analysis")
    
    # Get available dates
    dates = get_available_dates()
    
    if not dates:
        st.warning("No data available. Please fetch data first.")
    else:
        # Date selection
        selected_date = st.selectbox(
            "Select Date for Analysis",
            sorted(dates, reverse=True)
        )
        
        # Load data for the selected date
        data = load_from_db(selected_date)
        
        if data.empty:
            st.warning(f"No data available for {selected_date}")
        else:
            st.subheader(f"Data for {selected_date}")
            
            # Reset index to get Symbol and Date as columns
            data_reset = data.reset_index()
            
            # Display available stocks
            available_symbols = data_reset['Symbol'].unique().tolist()
            
            analysis_type = st.selectbox(
                "Select Analysis Type",
                ["Price Change Analysis", "Moving Averages", "Volume Analysis", "Performance Ranking"]
            )
            
            if analysis_type == "Price Change Analysis":
                # Calculate price changes
                price_changes = get_price_change(data)
                
                # Display price changes
                st.subheader("Price Changes (vs Previous Day)")
                
                # Filter options
                filter_option = st.radio(
                    "Filter",
                    ["All Stocks", "Top Gainers", "Top Losers"]
                )
                
                if filter_option == "Top Gainers":
                    filtered_data = price_changes.sort_values("Change_Pct", ascending=False).head(10)
                    st.success("🚀 Top 10 Gainers")
                elif filter_option == "Top Losers":
                    filtered_data = price_changes.sort_values("Change_Pct", ascending=True).head(10)
                    st.error("📉 Top 10 Losers")
                else:
                    filtered_data = price_changes.sort_values("Change_Pct", ascending=False)
                
                # Display table and visualization
                st.dataframe(filtered_data)
                
                # Plot for selected stocks
                st.subheader("Detailed View")
                selected_symbol = st.selectbox(
                    "Select Stock for Detailed View",
                    options=available_symbols
                )
                
                # Date range for visualization
                num_days = st.slider(
                    "Select number of previous days to show",
                    min_value=5,
                    max_value=30,
                    value=10
                )
                
                start_date, end_date = get_date_range(selected_date, num_days)
                
                # Plot stock price
                fig = plot_stock_price(selected_symbol, start_date, end_date)
                st.plotly_chart(fig, use_container_width=True)
                
                # Detect spikes
                spikes = detect_spikes(selected_symbol, start_date, end_date)
                if not spikes.empty:
                    st.subheader("Detected Anomalies")
                    st.dataframe(spikes)
            
            elif analysis_type == "Moving Averages":
                st.subheader("Moving Average Analysis")
                
                selected_symbol = st.selectbox(
                    "Select Stock",
                    options=available_symbols
                )
                
                # Date range selection
                num_days = st.slider(
                    "Select number of previous days to analyze",
                    min_value=15,
                    max_value=60,
                    value=30
                )
                
                start_date, end_date = get_date_range(selected_date, num_days)
                
                # Moving average parameters
                col1, col2 = st.columns(2)
                with col1:
                    short_window = st.number_input("Short Window (days)", min_value=5, max_value=20, value=5)
                with col2:
                    long_window = st.number_input("Long Window (days)", min_value=10, max_value=50, value=20)
                
                # Calculate and display moving averages
                ma_data = calculate_moving_averages(selected_symbol, start_date, end_date, short_window, long_window)
                
                if ma_data is not None:
                    # Plot moving averages
                    fig = plot_moving_averages(ma_data, selected_symbol, short_window, long_window)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Analysis insights
                    last_row = ma_data.iloc[-1]
                    
                    if last_row['Close'] > last_row[f'MA_{short_window}'] > last_row[f'MA_{long_window}']:
                        st.success(f"🟢 Bullish trend: Price > {short_window}-day MA > {long_window}-day MA")
                    elif last_row[f'MA_{short_window}'] > last_row['Close'] > last_row[f'MA_{long_window}']:
                        st.warning(f"🟡 Mixed signals: {short_window}-day MA > Price > {long_window}-day MA")
                    elif last_row[f'MA_{short_window}'] > last_row[f'MA_{long_window}'] > last_row['Close']:
                        st.error(f"🔴 Price below both MAs: {short_window}-day MA > {long_window}-day MA > Price")
                    elif last_row[f'MA_{short_window}'] < last_row[f'MA_{long_window}'] and last_row['Close'] > last_row[f'MA_{short_window}']:
                        st.warning(f"🟡 Possible trend reversal: Price > {short_window}-day MA > {long_window}-day MA")
                else:
                    st.error(f"Insufficient data for moving average calculation. Need at least {long_window} days of data.")
            
            elif analysis_type == "Volume Analysis":
                st.subheader("Volume Analysis")
                
                selected_symbol = st.selectbox(
                    "Select Stock",
                    options=available_symbols
                )
                
                # Date range selection
                num_days = st.slider(
                    "Select number of previous days to analyze",
                    min_value=5,
                    max_value=30,
                    value=10
                )
                
                start_date, end_date = get_date_range(selected_date, num_days)
                
                # Analyze volume
                volume_data = analyze_volume(selected_symbol, start_date, end_date)
                
                if volume_data is not None:
                    # Plot volume analysis
                    fig = plot_volume_analysis(volume_data, selected_symbol)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Volume insights
                    avg_volume = volume_data['Volume'].mean()
                    last_volume = volume_data['Volume'].iloc[-1]
                    
                    st.metric(
                        "Latest Trading Volume", 
                        f"{int(last_volume):,}", 
                        f"{int(last_volume - avg_volume):,}"
                    )
                    
                    if last_volume > avg_volume * 1.5:
                        st.info("📈 Significantly higher volume than average - could indicate strong movement")
                    elif last_volume < avg_volume * 0.5:
                        st.info("📉 Significantly lower volume than average - could indicate weak movement")
                else:
                    st.error("Insufficient data for volume analysis.")
            
            elif analysis_type == "Performance Ranking":
                st.subheader("Stock Performance Ranking")
                
                # Time period selection
                period = st.radio(
                    "Select Time Period",
                    ["1 Day", "1 Week", "1 Month"]
                )
                
                if period == "1 Day":
                    days = 1
                elif period == "1 Week":
                    days = 7
                else:  # 1 Month
                    days = 30
                
                start_date, end_date = get_date_range(selected_date, days)
                
                # Get top performers
                top_performers = get_best_performers(start_date, end_date, limit=10)
                worst_performers = get_worst_performers(start_date, end_date, limit=10)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.success("🏆 Top Performers")
                    if not top_performers.empty:
                        st.dataframe(top_performers)
                    else:
                        st.warning("Insufficient data to calculate top performers")
                
                with col2:
                    st.error("⚠️ Worst Performers")
                    if not worst_performers.empty:
                        st.dataframe(worst_performers)
                    else:
                        st.warning("Insufficient data to calculate worst performers")
                
                # Compare selected stocks
                st.subheader("Compare Selected Stocks")
                
                selected_symbols = st.multiselect(
                    "Select stocks to compare",
                    options=available_symbols,
                    default=top_performers['Symbol'].head(3).tolist() if not top_performers.empty else available_symbols[:3]
                )
                
                if selected_symbols:
                    fig = plot_comparison(selected_symbols, start_date, end_date)
                    st.plotly_chart(fig, use_container_width=True)

# Query Assistant page
elif page == "Query Assistant":
    st.header("Natural Language Query Assistant")
    
    st.markdown("""
    Ask questions about stock data in plain English. For example:
    - Which stock gained the most today?
    - Show me price trends for INFY over the last 7 days
    - What are the top 5 stocks trading above their 10-day average?
    - Compare TCS and RELIANCE performance this week
    """)
    
    # Date context
    dates = get_available_dates()
    
    if not dates:
        st.warning("No data available. Please fetch data first.")
    else:
        # Set context date
        context_date = st.selectbox(
            "Set date context for 'today' in queries",
            sorted(dates, reverse=True)
        )
        
        # Query input
        query = st.text_input("Enter your query", "Which stock gained the most today?")
        
        if st.button("Process Query"):
            with st.spinner("Processing query..."):
                # Process the query
                result, explanation, visualization = process_query(query, context_date)
                
                # Display results
                st.subheader("Result")
                
                if result is not None:
                    if isinstance(result, pd.DataFrame):
                        st.dataframe(result)
                    else:
                        st.write(result)
                    
                    st.info(f"📝 Interpretation: {explanation}")
                    
                    # Show visualization if available
                    if visualization is not None:
                        st.plotly_chart(visualization, use_container_width=True)
                else:
                    st.error("Sorry, I couldn't process that query. Please try a different question.")
        
        # Query examples
        with st.expander("Query Examples"):
            st.markdown("""
            ### Example Queries
            
            **General Queries:**
            - Which stock gained the most today?
            - What are the worst performing stocks this week?
            - Show me stocks with high trading volume today
            
            **Specific Stock Queries:**
            - Show price trends for INFY over the last 7 days
            - Compare TCS and RELIANCE performance
            - What is the current price of HDFCBANK?
            
            **Technical Analysis Queries:**
            - Which stocks are trading above their 10-day moving average?
            - Show me stocks with price breakouts today
            - Find stocks with unusual volume today
            """)

# Data Visualization page
elif page == "Data Visualization":
    st.header("Data Visualization")
    
    # Get available dates
    dates = get_available_dates()
    
    if not dates:
        st.warning("No data available. Please fetch data first.")
    else:
        # Date selection
        selected_date = st.selectbox(
            "Select Date",
            sorted(dates, reverse=True)
        )
        
        # Load data for the selected date
        data = load_from_db(selected_date)
        
        if data.empty:
            st.warning(f"No data available for {selected_date}")
        else:
            # Reset index to get Symbol and Date as columns
            data_reset = data.reset_index()
            
            # Display available stocks
            available_symbols = data_reset['Symbol'].unique().tolist()
            
            # Visualization type
            viz_type = st.selectbox(
                "Select Visualization",
                ["Price Movement", "Volume Analysis", "Moving Average Comparison", "Correlation Matrix"]
            )
            
            if viz_type == "Price Movement":
                # Stock selection
                selected_symbols = st.multiselect(
                    "Select Stocks to Visualize",
                    options=available_symbols,
                    default=available_symbols[:3] if len(available_symbols) >= 3 else available_symbols
                )
                
                # Date range
                num_days = st.slider(
                    "Number of Days to Visualize",
                    min_value=5,
                    max_value=30,
                    value=10
                )
                
                start_date, end_date = get_date_range(selected_date, num_days)
                
                if selected_symbols:
                    # Create comparison visualization
                    fig = plot_comparison(selected_symbols, start_date, end_date)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("Please select at least one stock to visualize")
            
            elif viz_type == "Volume Analysis":
                # Stock selection
                selected_symbol = st.selectbox(
                    "Select Stock",
                    options=available_symbols
                )
                
                # Date range
                num_days = st.slider(
                    "Number of Days to Analyze",
                    min_value=5,
                    max_value=30,
                    value=10
                )
                
                start_date, end_date = get_date_range(selected_date, num_days)
                
                # Analyze volume
                volume_data = analyze_volume(selected_symbol, start_date, end_date)
                
                if volume_data is not None:
                    # Plot volume analysis
                    fig = plot_volume_analysis(volume_data, selected_symbol)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error("Insufficient data for volume analysis")
            
            elif viz_type == "Moving Average Comparison":
                # Stock selection
                selected_symbol = st.selectbox(
                    "Select Stock",
                    options=available_symbols
                )
                
                # Date range
                num_days = st.slider(
                    "Number of Days to Analyze",
                    min_value=15,
                    max_value=60,
                    value=30
                )
                
                start_date, end_date = get_date_range(selected_date, num_days)
                
                # Moving average parameters
                col1, col2 = st.columns(2)
                with col1:
                    short_window = st.number_input("Short Window (days)", min_value=5, max_value=20, value=5)
                with col2:
                    long_window = st.number_input("Long Window (days)", min_value=10, max_value=50, value=20)
                
                # Calculate and display moving averages
                ma_data = calculate_moving_averages(selected_symbol, start_date, end_date, short_window, long_window)
                
                if ma_data is not None:
                    # Plot moving averages
                    fig = plot_moving_averages(ma_data, selected_symbol, short_window, long_window)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error(f"Insufficient data for moving average calculation. Need at least {long_window} days of data.")
            
            elif viz_type == "Correlation Matrix":
                st.subheader("Stock Price Correlation Matrix")
                
                # Date range
                num_days = st.slider(
                    "Number of Days for Correlation Analysis",
                    min_value=5,
                    max_value=30,
                    value=10
                )
                
                start_date, end_date = get_date_range(selected_date, num_days)
                
                # Select stocks for correlation
                selected_symbols = st.multiselect(
                    "Select Stocks for Correlation Analysis",
                    options=available_symbols,
                    default=available_symbols[:5] if len(available_symbols) >= 5 else available_symbols
                )
                
                if len(selected_symbols) < 2:
                    st.warning("Please select at least 2 stocks for correlation analysis")
                else:
                    # Create composite dataframe for selected period and symbols
                    price_data = {}
                    
                    for symbol in selected_symbols:
                        # Load data for each symbol across the date range
                        symbol_data = pd.DataFrame()
                        for date in pd.date_range(start=start_date, end=end_date):
                            date_str = date.strftime("%Y-%m-%d")
                            daily_data = load_from_db(date_str)
                            if not daily_data.empty:
                                # Reset index to get Symbol as column
                                daily_data_reset = daily_data.reset_index()
                                symbol_price = daily_data_reset[daily_data_reset['Symbol'] == symbol]
                                if not symbol_price.empty:
                                    symbol_data = pd.concat([symbol_data, symbol_price])
                        
                        if not symbol_data.empty:
                            price_data[symbol] = symbol_data.set_index('Date')['Close']
                    
                    if price_data:
                        # Create DataFrame with all price series
                        correlation_df = pd.DataFrame(price_data)
                        
                        # Calculate correlation matrix
                        correlation_matrix = correlation_df.corr()
                        
                        # Plot heatmap
                        import plotly.figure_factory as ff
                        
                        fig = ff.create_annotated_heatmap(
                            z=correlation_matrix.values,
                            x=correlation_matrix.columns.tolist(),
                            y=correlation_matrix.index.tolist(),
                            annotation_text=correlation_matrix.round(2).values,
                            showscale=True,
                            colorscale='Viridis'
                        )
                        
                        fig.update_layout(
                            title='Correlation Matrix (Stock Prices)',
                            height=600,
                            width=800
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        st.markdown("""
                        ### Interpreting the Correlation Matrix
                        - Values close to 1 indicate strong positive correlation (stocks move together)
                        - Values close to -1 indicate strong negative correlation (stocks move in opposite directions)
                        - Values close to 0 indicate little to no correlation
                        """)
                    else:
                        st.error("Insufficient data for correlation analysis")

# Footer
st.markdown("---")
st.markdown("Indian Stock Market Analyzer | Data source: Yahoo Finance")
