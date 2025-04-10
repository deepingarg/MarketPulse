import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import logging
from data_storage import load_stock_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def plot_stock_price(symbol, start_date_str, end_date_str):
    """
    Create a candlestick chart for a stock

    Args:
        symbol (str): Stock symbol
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format

    Returns:
        plotly.graph_objects.Figure: Plotly figure object
    """
    try:
        # Load stock data from database
        from database_manager import load_stock_data_from_db
        data = load_stock_data_from_db(symbol, start_date_str, end_date_str)

        # Fallback to CSV if database is empty
        if data.empty:
            data = load_stock_data(symbol, start_date_str, end_date_str)

        if data.empty:
            logger.warning(f"No data available for {symbol} from {start_date_str} to {end_date_str}")

            # Create empty figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for the selected period",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(title=f"{symbol} - No Data Available")
            return fig

        # Sort by date
        data = data.sort_values('Date')

        # Create candlestick chart
        fig = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.7, 0.3],
            subplot_titles=(f"{symbol} Price Chart", "Volume")
        )

        # Reset index if Date is in the index
        if isinstance(data.index, pd.DatetimeIndex):
            data = data.reset_index()
        elif 'Date' not in data.columns and 'date' in data.columns:
            data = data.rename(columns={'date': 'Date'})

        # Add candlestick trace
        fig.add_trace(
            go.Candlestick(
                x=data['Date'],
                open=data['Open'],
                high=data['High'],
                low=data['Low'],
                close=data['Close'],
                name="Price"
            ),
            row=1, col=1
        )

        # Add volume trace
        fig.add_trace(
            go.Bar(
                x=data['Date'],
                y=data['Volume'],
                name="Volume",
                marker_color='rgba(0, 0, 255, 0.5)'
            ),
            row=2, col=1
        )

        # Update layout
        fig.update_layout(
            title=f"{symbol} Stock Price ({start_date_str} to {end_date_str})",
            xaxis_title="Date",
            yaxis_title="Price",
            xaxis_rangeslider_visible=False,
            height=600,
            showlegend=False
        )

        return fig

    except Exception as e:
        logger.error(f"Error creating stock price chart for {symbol}: {str(e)}")

        # Create error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating chart: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        fig.update_layout(title=f"Error - {symbol}")
        return fig

def plot_comparison(symbols, start_date_str, end_date_str, normalize=True):
    """
    Create a comparison chart for multiple stocks

    Args:
        symbols (list): List of stock symbols
        start_date_str (str): Start date in YYYY-MM-DD format
        end_date_str (str): End date in YYYY-MM-DD format
        normalize (bool): Whether to normalize prices to percentage changes

    Returns:
        plotly.graph_objects.Figure: Plotly figure object
    """
    try:
        # Initialize figure
        fig = go.Figure()

        # Process each symbol
        valid_data = False
        for symbol in symbols:
            # Load stock data from database
            from database_manager import load_stock_data_from_db
            data = load_stock_data_from_db(symbol, start_date_str, end_date_str)
            
            if data.empty:
                logger.warning(f"No data available for {symbol} from {start_date_str} to {end_date_str}")
                continue

            # Reset index if Date is in the index
            if isinstance(data.index, pd.DatetimeIndex):
                data = data.reset_index()
            elif 'date' in data.columns:
                data = data.rename(columns={'date': 'Date'})

            # Ensure Date column is datetime
            data['Date'] = pd.to_datetime(data['Date'])
            
            # Sort by date
            data = data.sort_values('Date')

            # Normalize data if requested
            if normalize and not data.empty:
                base_price = data.iloc[0]['Close']
                data['Normalized'] = (data['Close'] / base_price - 1) * 100
                y_values = data['Normalized']
                y_axis_title = "% Change"
            else:
                y_values = data['Close']
                y_axis_title = "Price"

            # Add line to the figure
            fig.add_trace(
                go.Scatter(
                    x=data['Date'],
                    y=y_values,
                    mode='lines',
                    name=symbol
                )
            )

            valid_data = True

        if not valid_data:
            logger.warning("No valid data for any of the selected symbols")

            # Create empty figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No data available for the selected symbols and period",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(title="No Data Available")
            return fig

        # Update layout
        title = "Stock Price Comparison" if not normalize else "Stock Performance Comparison (% Change)"
        title += f" ({start_date_str} to {end_date_str})"

        fig.update_layout(
            title=title,
            xaxis_title="Date",
            yaxis_title=y_axis_title,
            height=500,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        return fig

    except Exception as e:
        logger.error(f"Error creating comparison chart: {str(e)}")

        # Create error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating chart: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        fig.update_layout(title="Error - Comparison Chart")
        return fig

def plot_moving_averages(ma_data, symbol, short_window=5, long_window=20):
    """
    Create a chart with moving averages

    Args:
        ma_data (pd.DataFrame): DataFrame with moving average data
        symbol (str): Stock symbol
        short_window (int): Short moving average window
        long_window (int): Long moving average window

    Returns:
        plotly.graph_objects.Figure: Plotly figure object
    """
    try:
        if ma_data is None or ma_data.empty:
            logger.warning(f"No moving average data available for {symbol}")

            # Create empty figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No moving average data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(title=f"{symbol} - No Moving Average Data")
            return fig

        # Create figure with secondary y-axis
        fig = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.7, 0.3],
            subplot_titles=(f"{symbol} with Moving Averages", "Volume")
        )

        # Add price trace
        fig.add_trace(
            go.Scatter(
                x=ma_data['Date'],
                y=ma_data['Close'],
                mode='lines',
                name="Close Price",
                line=dict(color='black')
            ),
            row=1, col=1
        )

        # Add short MA trace
        fig.add_trace(
            go.Scatter(
                x=ma_data['Date'],
                y=ma_data[f'MA_{short_window}'],
                mode='lines',
                name=f"{short_window}-day MA",
                line=dict(color='blue')
            ),
            row=1, col=1
        )

        # Add long MA trace
        fig.add_trace(
            go.Scatter(
                x=ma_data['Date'],
                y=ma_data[f'MA_{long_window}'],
                mode='lines',
                name=f"{long_window}-day MA",
                line=dict(color='red')
            ),
            row=1, col=1
        )

        # Add volume trace
        fig.add_trace(
            go.Bar(
                x=ma_data['Date'],
                y=ma_data['Volume'],
                name="Volume",
                marker_color='rgba(0, 0, 255, 0.5)'
            ),
            row=2, col=1
        )

        # Update layout
        fig.update_layout(
            title=f"{symbol} with {short_window}-day and {long_window}-day Moving Averages",
            xaxis_title="Date",
            yaxis_title="Price",
            height=600,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        return fig

    except Exception as e:
        logger.error(f"Error creating moving averages chart for {symbol}: {str(e)}")

        # Create error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating chart: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        fig.update_layout(title=f"Error - {symbol} Moving Averages")
        return fig

def plot_volume_analysis(volume_data, symbol):
    """
    Create a volume analysis chart

    Args:
        volume_data (pd.DataFrame): DataFrame with volume data
        symbol (str): Stock symbol

    Returns:
        plotly.graph_objects.Figure: Plotly figure object
    """
    try:
        if volume_data is None or volume_data.empty:
            logger.warning(f"No volume data available for {symbol}")

            # Create empty figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No volume data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(title=f"{symbol} - No Volume Data")
            return fig

        # Create figure with secondary y-axis
        fig = make_subplots(
            rows=2, cols=1, 
            shared_xaxes=True,
            vertical_spacing=0.1,
            row_heights=[0.5, 0.5],
            subplot_titles=(f"{symbol} Price", "Trading Volume")
        )

        # Add price trace
        fig.add_trace(
            go.Scatter(
                x=volume_data['Date'],
                y=volume_data['Close'],
                mode='lines',
                name="Close Price"
            ),
            row=1, col=1
        )

        # Add volume bars
        fig.add_trace(
            go.Bar(
                x=volume_data['Date'],
                y=volume_data['Volume'],
                name="Volume",
                marker_color='rgba(0, 0, 255, 0.5)'
            ),
            row=2, col=1
        )

        # Add volume MA if available
        if 'Volume_MA_5' in volume_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=volume_data['Date'],
                    y=volume_data['Volume_MA_5'],
                    mode='lines',
                    name="5-day Volume MA",
                    line=dict(color='red')
                ),
                row=2, col=1
            )

        # Update layout
        fig.update_layout(
            title=f"{symbol} Volume Analysis",
            xaxis_title="Date",
            height=600,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        fig.update_yaxes(title_text="Price", row=1, col=1)
        fig.update_yaxes(title_text="Volume", row=2, col=1)

        return fig

    except Exception as e:
        logger.error(f"Error creating volume analysis chart for {symbol}: {str(e)}")

        # Create error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating chart: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        fig.update_layout(title=f"Error - {symbol} Volume Analysis")
        return fig

def plot_performance_distribution(performance_data, metric='Return (%)'):
    """
    Create a distribution plot for stock performance

    Args:
        performance_data (pd.DataFrame): DataFrame with performance metrics
        metric (str): Metric to visualize

    Returns:
        plotly.graph_objects.Figure: Plotly figure object
    """
    try:
        if performance_data is None or performance_data.empty:
            logger.warning("No performance data available")

            # Create empty figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No performance data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(title="No Performance Data")
            return fig

        # Create histogram
        fig = px.histogram(
            performance_data,
            x=metric,
            nbins=20,
            title=f"Distribution of {metric} Across Stocks"
        )

        # Add mean line
        mean_value = performance_data[metric].mean()
        fig.add_vline(
            x=mean_value,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Mean: {mean_value:.2f}",
            annotation_position="top right"
        )

        # Update layout
        fig.update_layout(
            xaxis_title=metric,
            yaxis_title="Number of Stocks",
            height=400
        )

        return fig

    except Exception as e:
        logger.error(f"Error creating performance distribution chart: {str(e)}")

        # Create error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating chart: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        fig.update_layout(title="Error - Performance Distribution")
        return fig

def plot_top_performers(performance_data, metric='Return (%)', top_n=10, ascending=False):
    """
    Create a bar chart of top/bottom performers

    Args:
        performance_data (pd.DataFrame): DataFrame with performance metrics
        metric (str): Metric to visualize
        top_n (int): Number of stocks to show
        ascending (bool): If True, show worst performers; if False, show best performers

    Returns:
        plotly.graph_objects.Figure: Plotly figure object
    """
    try:
        if performance_data is None or performance_data.empty:
            logger.warning("No performance data available")

            # Create empty figure with message
            fig = go.Figure()
            fig.add_annotation(
                text="No performance data available",
                xref="paper", yref="paper",
                x=0.5, y=0.5, showarrow=False
            )
            fig.update_layout(title="No Performance Data")
            return fig

        # Sort and filter data
        sorted_data = performance_data.sort_values(metric, ascending=ascending).head(top_n)

        # Create bar chart
        title = f"{'Bottom' if ascending else 'Top'} {top_n} Stocks by {metric}"

        fig = px.bar(
            sorted_data,
            x='Symbol',
            y=metric,
            title=title,
            text=metric,
            color=metric,
            color_continuous_scale='RdYlGn' if not ascending else 'YlGnRd'
        )

        # Update layout
        fig.update_layout(
            xaxis_title="Stock Symbol",
            yaxis_title=metric,
            height=500
        )

        # Format text display
        fig.update_traces(
            texttemplate='%{text:.2f}',
            textposition='outside'
        )

        return fig

    except Exception as e:
        logger.error(f"Error creating top performers chart: {str(e)}")

        # Create error figure
        fig = go.Figure()
        fig.add_annotation(
            text=f"Error creating chart: {str(e)}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        fig.update_layout(title="Error - Top Performers")
        return fig