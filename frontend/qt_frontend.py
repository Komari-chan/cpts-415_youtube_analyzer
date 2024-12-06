import sys
import os
import time
import pandas as pd
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QPushButton, QVBoxLayout, QLabel, QWidget,
    QLineEdit, QHBoxLayout, QTableWidget, QTableWidgetItem, QProgressBar,
    QDialog
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QPixmap
# Get the directory of the current script (frontend/)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory (milestone4/)
parent_dir = os.path.dirname(current_dir)
# Add parent directory to sys.path
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
from backend.data_analysis import fetch_video_data, compute_statistics, compute_top_videos
from backend.visualization import (
    plot_views_distribution, plot_category_statistics,
    plot_trend_of_views_and_ratings, plot_correlation_table,
    plot_combined_trend_with_dual_axes, plot_views_by_length_range
)
from backend.mongo_connection import get_mongo_collection
from backend.filter_data import filter_videos
from pyspark.sql import SparkSession
from backend.spark_analysis import spark_analyze_data, analyze_related_videos
from backend.spark_visualization import generate_visualizations
from backend.spark_main import merge_and_rename_spark_output, clean_and_prepare_data
import subprocess

from backend.mongo_preprocessing import (
    get_mongo_collection,
    fetch_filtered_data,
    precompute_category_statistics,
    bin_video_lengths,
    precompute_top_videos,
)

class Worker(QThread):
    progress_signal = pyqtSignal(int)
    result_signal = pyqtSignal(object)

    def __init__(self, task_function, *args):
        super().__init__()
        self.task_function = task_function
        self.args = args

    def run(self):
        try:
            progress = 0
            self.progress_signal.emit(progress)

            # Call task function and pass a progress callback
            def progress_callback(step):
                nonlocal progress
                progress = step
                self.progress_signal.emit(progress)

            result = self.task_function(*self.args, progress_callback)
            self.result_signal.emit(result)
        except Exception as e:
            self.result_signal.emit(e)
        finally:
            self.progress_signal.emit(100)

def generate_visualizations_task(df_videos, output_folder, progress_callback=None):
    tasks = [
        plot_views_distribution,
        plot_category_statistics,
        plot_trend_of_views_and_ratings,
        plot_correlation_table,
        plot_combined_trend_with_dual_axes,
        plot_views_by_length_range
    ]
    for i, task in enumerate(tasks, 1):
        task(df_videos, output_folder)
        if progress_callback:
            progress_callback(int(100 / len(tasks)))
    return "Completed"

class ImageDialog(QDialog):
    def __init__(self, image_path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Image Viewer")
        layout = QVBoxLayout()
        pixmap = QPixmap(image_path)
        label = QLabel()
        label.setPixmap(pixmap)
        label.setScaledContents(True)
        layout.addWidget(label)
        self.setLayout(layout)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("YouTube Data Analysis")
        self.setGeometry(100, 100, 1200, 800)
        self.worker = None
        # Central Widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Layouts
        main_layout = QVBoxLayout()

        # Input Filters
        filter_layout = QHBoxLayout()
        self.age_input = QLineEdit()
        self.age_input.setPlaceholderText("Enter age range (e.g., 10-1000)")
        self.views_input = QLineEdit()
        self.views_input.setPlaceholderText("Enter views range (e.g., 1000-1000000)")
        self.rating_input = QLineEdit()
        self.rating_input.setPlaceholderText("Enter rating range (e.g., 1-5)")
        filter_layout.addWidget(QLabel("Filters:"))
        filter_layout.addWidget(self.age_input)
        filter_layout.addWidget(self.views_input)
        filter_layout.addWidget(self.rating_input)

        # Buttons
        self.load_data_button = QPushButton("Load Data from MongoDB")
        self.filter_button = QPushButton("Filter Data")
        self.generate_visualizations_button = QPushButton("Generate Visualizations")

        # Visualization Buttons
        visualization_buttons = QHBoxLayout()
        self.category_views_button = QPushButton("Category Views")
        self.category_ratings_button = QPushButton("Category Ratings")
        self.views_distribution_button = QPushButton("Views Distribution")
        self.trend_views_button = QPushButton("Trend of Average Views")
        self.trend_ratings_button = QPushButton("Trend of Average Ratings")
        self.combined_trends_button = QPushButton("Combined Trends")
        self.views_length_button = QPushButton("Views by Length")
        visualization_buttons.addWidget(self.category_views_button)
        visualization_buttons.addWidget(self.category_ratings_button)
        visualization_buttons.addWidget(self.views_distribution_button)
        visualization_buttons.addWidget(self.trend_views_button)
        visualization_buttons.addWidget(self.trend_ratings_button)
        visualization_buttons.addWidget(self.combined_trends_button)
        visualization_buttons.addWidget(self.views_length_button)

        # Pagination Controls
        pagination_layout = QHBoxLayout()
        self.prev_button = QPushButton("Previous")
        self.next_button = QPushButton("Next")
        self.page_label = QLabel("Page 1")
        pagination_layout.addWidget(self.prev_button)
        pagination_layout.addWidget(self.page_label)
        pagination_layout.addWidget(self.next_button)

        main_layout.addLayout(pagination_layout)

        self.page_size = 100  # Number of rows per page
        self.current_page = 0

        # Top-N Data Buttons
        top_n_buttons = QHBoxLayout()
        self.top_views_button = QPushButton("Top Views")
        self.top_ratings_button = QPushButton("Top Ratings")
        self.top_comments_button = QPushButton("Top Comments")
        self.statistics_button = QPushButton("Statistics")
        top_n_buttons.addWidget(self.top_views_button)
        top_n_buttons.addWidget(self.top_ratings_button)
        top_n_buttons.addWidget(self.top_comments_button)
        top_n_buttons.addWidget(self.statistics_button)

        self.analyze_data_button = QPushButton("Analyze Data")
        self.analyze_data_button.clicked.connect(self.analyze_data)
        self.top_n_input = QLineEdit()
        self.top_n_input.setPlaceholderText("Enter Top N (1-100)")
        top_n_buttons.addWidget(self.top_n_input)

        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)

        # Status Label
        self.status_label = QLabel("Status: Ready")
        self.status_label.setAlignment(Qt.AlignCenter)

        # Data Table for Displaying Results
        self.data_table = QTableWidget()
        self.data_table.setColumnCount(5)  # Default columns

        # Add Widgets to Main Layout
        main_layout.addWidget(self.load_data_button)
        main_layout.addLayout(filter_layout)
        main_layout.addWidget(self.filter_button)
        main_layout.addWidget(self.generate_visualizations_button)
        main_layout.addLayout(visualization_buttons)
        main_layout.addWidget(self.analyze_data_button)
        main_layout.addLayout(top_n_buttons)
        main_layout.addWidget(self.data_table)
        main_layout.addWidget(self.progress_bar)
        main_layout.addWidget(self.status_label)

        central_widget.setLayout(main_layout)

        # Connect Buttons to Actions
        self.top_views_button.clicked.connect(lambda: self.display_csv("output/top_views.csv"))
        self.top_ratings_button.clicked.connect(lambda: self.display_csv("output/top_ratings.csv"))
        self.top_comments_button.clicked.connect(lambda: self.display_csv("output/top_comments.csv"))
        self.statistics_button.clicked.connect(lambda: self.display_csv("output/statistics.csv"))

        # Connect Buttons
        self.load_data_button.clicked.connect(self.load_data)
        self.filter_button.clicked.connect(self.filter_data)
        self.generate_visualizations_button.clicked.connect(self.generate_visualizations)
        self.category_views_button.clicked.connect(lambda: self.show_image("output/category_views.png"))
        self.category_ratings_button.clicked.connect(lambda: self.show_image("output/category_ratings.png"))
        self.views_distribution_button.clicked.connect(lambda: self.show_image("output/views_distribution.png"))
        self.trend_views_button.clicked.connect(lambda: self.show_image("output/trend_of_average_views.png"))
        self.trend_ratings_button.clicked.connect(lambda: self.show_image("output/trend_of_average_ratings.png"))
        self.combined_trends_button.clicked.connect(lambda: self.show_image("output/trend_combined_dual_axes.png"))
        self.views_length_button.clicked.connect(lambda: self.show_image("output/views_by_length_range_filtered.png"))


        """
        Spark
        """
        # self.spark_load_data_button = QPushButton("Load Data Using Spark")
        # self.spark_analyze_data_button = QPushButton("Analyze Data Using Spark")
        # # self.load_spark_results_button = QPushButton("Display Spark Results")

        # main_layout.addWidget(self.spark_load_data_button)
        # main_layout.addWidget(self.spark_analyze_data_button)
        # # main_layout.addWidget(self.load_spark_results_button)

        # self.spark_load_data_button.clicked.connect(self.load_data_spark)
        # self.spark_analyze_data_button.clicked.connect(self.analyze_data_spark)
        # # self.load_spark_results_button.clicked.connect(self.load_spark_results)

        # # Add Spark result display buttons
        # self.display_category_stats_button = QPushButton("Display Category Stats")
        # self.display_top_views_button = QPushButton("Display Top 10 Views")
        # self.display_top_ratings_button = QPushButton("Display Top 10 Ratings")
        # self.display_trends_button = QPushButton("Display Trends")

        # # Connect buttons to specific result loaders
        # self.display_category_stats_button.clicked.connect(lambda: self.load_single_result("category_stats.csv"))
        # self.display_top_views_button.clicked.connect(lambda: self.load_single_result("top_10_views.csv"))
        # self.display_top_ratings_button.clicked.connect(lambda: self.load_single_result("top_10_ratings.csv"))
        # self.display_trends_button.clicked.connect(lambda: self.load_single_result("trends.csv"))

        # # Add to layout
        # main_layout.addWidget(self.display_category_stats_button)
        # main_layout.addWidget(self.display_top_views_button)
        # main_layout.addWidget(self.display_top_ratings_button)
        # main_layout.addWidget(self.display_trends_button)

        # # Add buttons for displaying images
        # self.display_category_views_image_button = QPushButton("Category Views Graph")
        # self.display_top_videos_image_button = QPushButton("Top 10 Videos Graph")
        # self.display_trends_views_image_button = QPushButton("Trends Views Graph")
        # self.display_trends_ratings_image_button = QPushButton("Trends Ratings Graph")

        # # Connect buttons to specific image loaders
        # self.display_category_views_image_button.clicked.connect(lambda: self.show_image("spark_output/category_views.png"))
        # self.display_top_videos_image_button.clicked.connect(lambda: self.show_image("spark_output/top_10_videos.png"))
        # self.display_trends_views_image_button.clicked.connect(lambda: self.show_image("spark_output/trends_views_fixed.png"))
        # self.display_trends_ratings_image_button.clicked.connect(lambda: self.show_image("spark_output/trends_ratings_fixed.png"))

        # # Add to layout
        # main_layout.addWidget(self.display_category_views_image_button)
        # main_layout.addWidget(self.display_top_videos_image_button)
        # main_layout.addWidget(self.display_trends_views_image_button)
        # main_layout.addWidget(self.display_trends_ratings_image_button)

        # Spark Buttons
        spark_buttons_layout = QHBoxLayout()
        self.spark_load_data_button = QPushButton("Load Data Using Spark")
        self.spark_analyze_data_button = QPushButton("Analyze Data Using Spark")
        spark_buttons_layout.addWidget(self.spark_load_data_button)
        spark_buttons_layout.addWidget(self.spark_analyze_data_button)
        main_layout.addLayout(spark_buttons_layout)

        # Buttons for Displaying Results (Tables)
        table_buttons_layout = QHBoxLayout()
        self.display_category_stats_button = QPushButton("Display Category Stats")
        self.display_top_views_button = QPushButton("Display Top 10 Views")
        self.display_top_ratings_button = QPushButton("Display Top 10 Ratings")
        self.display_trends_button = QPushButton("Display Trends")
        table_buttons_layout.addWidget(self.display_category_stats_button)
        table_buttons_layout.addWidget(self.display_top_views_button)
        table_buttons_layout.addWidget(self.display_top_ratings_button)
        table_buttons_layout.addWidget(self.display_trends_button)
        main_layout.addLayout(table_buttons_layout)

        # Buttons for Displaying Images (Graphs)
        graph_buttons_layout = QHBoxLayout()
        self.display_category_views_image_button = QPushButton("Category Views Graph")
        self.display_top_videos_image_button = QPushButton("Top 10 Videos Graph")
        self.display_trends_views_image_button = QPushButton("Trends Views Graph")
        self.display_trends_ratings_image_button = QPushButton("Trends Ratings Graph")
        graph_buttons_layout.addWidget(self.display_category_views_image_button)
        graph_buttons_layout.addWidget(self.display_top_videos_image_button)
        graph_buttons_layout.addWidget(self.display_trends_views_image_button)
        graph_buttons_layout.addWidget(self.display_trends_ratings_image_button)
        main_layout.addLayout(graph_buttons_layout)

        # Connect Buttons to Actions
        self.spark_load_data_button.clicked.connect(self.load_data_spark)
        self.spark_analyze_data_button.clicked.connect(self.analyze_data_spark)

        self.display_category_stats_button.clicked.connect(lambda: self.load_single_result("category_stats.csv"))
        self.display_top_views_button.clicked.connect(lambda: self.load_single_result("top_10_views.csv"))
        self.display_top_ratings_button.clicked.connect(lambda: self.load_single_result("top_10_ratings.csv"))
        self.display_trends_button.clicked.connect(lambda: self.load_single_result("trends.csv"))

        self.display_category_views_image_button.clicked.connect(lambda: self.show_image("spark_output/category_views.png"))
        self.display_top_videos_image_button.clicked.connect(lambda: self.show_image("spark_output/top_10_videos.png"))
        self.display_trends_views_image_button.clicked.connect(lambda: self.show_image("spark_output/trends_views_fixed.png"))
        self.display_trends_ratings_image_button.clicked.connect(lambda: self.show_image("spark_output/trends_ratings_fixed.png"))

        self.next_button.clicked.connect(self.next_page)
        self.prev_button.clicked.connect(self.prev_page)
        
        # Initialize DataFrame
        self.df_videos = None

    def next_page(self):
        """
        Go to the next page if possible.
        """
        if self.filtered_videos is not None:
            df = self.filtered_videos
        else:
            df = self.df_videos

        if df is not None:
            total_pages = (len(df) + self.page_size - 1) // self.page_size
            if self.current_page < total_pages - 1:
                self.current_page += 1
                self.populate_table(df)

    def prev_page(self):
        """
        Go to the previous page if possible.
        """
        if self.filtered_videos is not None:
            df = self.filtered_videos
        else:
            df = self.df_videos

        if df is not None and self.current_page > 0:
            self.current_page -= 1
            self.populate_table(df)


    def update_progress(self, value):
        # print(f"Progress: {value}%")  # Debug log
        self.progress_bar.setValue(value)

    def format_collection(self):
        """
        Preprocess the MongoDB collection for analysis.
        """
        try:
            precompute_category_statistics(self.collection, "category_statistics")
            bin_video_lengths(self.collection, "video_length_bins")
            precompute_top_videos(self.collection, "top_videos", top_n=10)
            self.status_label.setText("MongoDB preprocessing completed.")
        except Exception as e:
            self.status_label.setText(f"Error during preprocessing: {e}")

    def load_data(self):
        """
        Load data from MongoDB with a progress bar.
        """
        self.status_label.setText("Loading data from MongoDB...")
        
        # Ensure existing worker is terminated before starting a new one
        if self.worker and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()

        # Create a worker thread
        self.worker = Worker(self._load_data_task)
        self.worker.progress_signal.connect(self.update_progress)  # Update progress bar
        self.worker.result_signal.connect(self.on_data_loaded)     # Handle completion
        self.worker.start()


    def closeEvent(self, event):
        """
        Ensure threads are terminated when the application closes.
        """
        if self.worker and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()
        event.accept()

    def _load_data_task(self, progress_callback=None):
        collection = get_mongo_collection("youtube_analyzer", "videos")
        self.df_videos = fetch_video_data(collection, progress_callback=progress_callback)
        return f"Loaded {len(self.df_videos)} records."


    def on_data_loaded(self, result):
        if isinstance(result, Exception):
            self.status_label.setText(f"Error: {result}")
        else:
            self.status_label.setText(result)
            if self.df_videos is not None and not self.df_videos.empty:
                self.current_page = 0  # Reset to the first page
                self.populate_table(self.df_videos)
            else:
                self.status_label.setText("No data loaded from MongoDB.")

    def display_csv(self, file_path):
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                self.status_label.setText(f"No data to display in {os.path.basename(file_path)}.")
            else:
                self.populate_table(df)
                self.status_label.setText(f"Displayed data from {os.path.basename(file_path)}.")
        except Exception as e:
            self.status_label.setText(f"Error: {e}")
            
    def populate_table(self, df):
        """
        Populate the QTableWidget with data from a DataFrame.
        """
        # Debugging output
        print("Populating table with DataFrame:")
        print(df)

        if df is None or df.empty:
            self.status_label.setText("No data to display.")
            self.data_table.setRowCount(0)
            return

        # Calculate start and end indices for pagination
        start_idx = self.current_page * self.page_size
        end_idx = start_idx + self.page_size
        paginated_df = df.iloc[start_idx:end_idx]
        
        # Debugging: Print paginated DataFrame
        print("Paginated DataFrame:")
        print(paginated_df)

        # Clear the table and set column count and headers
        self.data_table.clear()
        self.data_table.setColumnCount(len(paginated_df.columns))
        self.data_table.setHorizontalHeaderLabels(list(paginated_df.columns))

        # Temporarily disable sorting for performance
        self.data_table.setSortingEnabled(False)

        # Set the row count to match paginated data
        self.data_table.setRowCount(len(paginated_df))
    
        # Populate the table
        for row_idx, row in enumerate(paginated_df.itertuples(index=False)):
            for col_idx, value in enumerate(row):
                item = QTableWidgetItem()
                if isinstance(value, (int, float)):
                    item.setData(Qt.DisplayRole, value) # Ensures numeric sorting
                else:
                    item.setText(str(value)) # Otherwise treat as string
                item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)  # Make items non-editable
                self.data_table.setItem(row_idx, col_idx, item)

        self.data_table.resizeColumnsToContents()
        self.update_page_label(df)

        # Re-enable sorting
        self.data_table.setSortingEnabled(True)
        
        # Force a table update to reflect changes
        self.data_table.viewport().update()

    def update_page_label(self, df):
        """
        Update the pagination label to reflect the current page and total pages.
        """
        total_pages = (len(df) + self.page_size - 1) // self.page_size
        self.page_label.setText(f"Page {self.current_page + 1} of {total_pages}")

    def filter_data(self):
        """
        Apply filters to the loaded data and update the table display.
        """
        if self.df_videos is not None:
            try:
                # Get filter inputs
                age_range_input = self.age_input.text().strip()
                views_range_input = self.views_input.text().strip()
                rating_range_input = self.rating_input.text().strip()

                # Parse and sort filter inputs into tuples
                age_range = tuple(sorted(map(int, age_range_input.split('-')))) if age_range_input else None
                views_range = tuple(sorted(map(int, views_range_input.split('-')))) if views_range_input else None
                rating_range = tuple(sorted(map(float, rating_range_input.split('-')))) if rating_range_input else None

                # Debug: Print parsed values
                print(f"Age Range: {age_range}, Views Range: {views_range}, Rating Range: {rating_range}")

                # Validate ranges
                def is_valid_range(min_value, max_value):
                    return min_value <= max_value

                if age_range and not is_valid_range(*age_range):
                    self.status_label.setText("Invalid age range. Please ensure the minimum value is less than or equal to the maximum value.")
                    return

                if views_range and not is_valid_range(*views_range):
                    self.status_label.setText("Invalid views range. Please ensure the minimum value is less than or equal to the maximum value.")
                    return

                if rating_range and not is_valid_range(*rating_range):
                    self.status_label.setText("Invalid rating range. Please ensure the minimum value is less than or equal to the maximum value.")
                    return

                # Filter the data using the backend function
                filtered_df = filter_videos(self.df_videos, age_range, views_range, rating_range)

                # Debug: Print the filtered DataFrame
                print("Filtered DataFrame:")
                print(filtered_df)

                if filtered_df.empty:
                    self.status_label.setText("No data matches the applied filters.")
                    self.data_table.setRowCount(0)  # Clear the table
                else:
                    # Update the filtered DataFrame and reset pagination
                    self.filtered_videos = filtered_df
                    self.current_page = 0  # Reset to the first page
                    self.populate_table(self.filtered_videos)  # Display the first page
                    self.status_label.setText("Data filtered successfully.")
            except Exception as e:
                print(f"Error in filter_data: {e}")
                self.status_label.setText(f"Error applying filters: {e}")
        else:
            print("No data loaded.")
            self.status_label.setText("No data loaded. Please load data first.")


    def generate_visualizations(self):
        if self.df_videos is not None:
            self.status_label.setText("Generating visualizations...")
            self.disable_buttons()

            if self.worker and self.worker.isRunning():
                self.worker.terminate()
                self.worker.wait()

            self.worker = Worker(self._generate_visualizations_task, self.df_videos, "output")
            self.worker.progress_signal.connect(self.update_progress)
            self.worker.result_signal.connect(self.on_visualizations_generated)
            self.worker.start()

    def disable_buttons(self):
        for button in [self.category_views_button, self.category_ratings_button,
                    self.views_distribution_button, self.trend_views_button,
                    self.trend_ratings_button, self.combined_trends_button,
                    self.views_length_button]:
            button.setEnabled(False)

    def enable_buttons(self):
        for button in [self.category_views_button, self.category_ratings_button,
                    self.views_distribution_button, self.trend_views_button,
                    self.trend_ratings_button, self.combined_trends_button,
                    self.views_length_button]:
            button.setEnabled(True)

    def on_visualizations_generated(self, result):
        if isinstance(result, Exception):
            self.status_label.setText(f"Error: {result}")
        else:
            self.status_label.setText("Visualizations generated successfully.")
        self.enable_buttons()  
        self.update_progress(100) 

    def _generate_visualizations_task(self, df_videos, output_folder, progress_callback=None):
        """
        Generate visualizations and update progress using a callback.
        """
        tasks = [
            plot_views_distribution,
            plot_category_statistics,
            plot_trend_of_views_and_ratings,
            plot_correlation_table,
            plot_combined_trend_with_dual_axes,
            plot_views_by_length_range
        ]
        for i, task in enumerate(tasks, 1):
            task(df_videos, output_folder)
            if progress_callback:
                progress_callback(int(100 / len(tasks)))  # Update progress incrementally
        return "Completed"

    def show_image(self, image_path):
        """
        Show image in a dialog.
        """
        if os.path.exists(image_path):
            dialog = ImageDialog(image_path, self)
            dialog.exec_()
        else:
            self.status_label.setText(f"Image not found: {image_path}")



    def display_csv(self, file_path):
        """
        Display the contents of a CSV file in the QTableWidget.
        """
        try:
            if not os.path.exists(file_path):
                self.status_label.setText(f"File not found: {file_path}. Please analyze data first.")
                return
            
            df = pd.read_csv(file_path)
            if df.empty:
                self.status_label.setText(f"No data found in {os.path.basename(file_path)}.")
            else:
                self.populate_table(df)
                self.status_label.setText(f"Displayed data from {os.path.basename(file_path)}.")
        except FileNotFoundError:
            self.status_label.setText(f"File not found: {file_path}.")
        except Exception as e:
            self.status_label.setText(f"Error displaying CSV: {e}")

    def analyze_data(self):
        if self.df_videos is not None:
            self.status_label.setText("Analyzing data...")
            self.disable_buttons()
            
            self.worker = Worker(self._analyze_data_task, self.df_videos, "output")
            self.worker.progress_signal.connect(self.update_progress)
            self.worker.result_signal.connect(self.on_analysis_completed)
            self.worker.start()
        else:
            self.status_label.setText("No data loaded. Please load data first.")

    def _analyze_data_task(self, df_videos, output_folder, progress_callback=None):
        """
        Analyze data (generate Top-N, statistics) and update progress.
        """
        compute_statistics(df_videos, output_folder)
        
        top_n = 10  
        compute_top_videos(df_videos, output_folder, top_n)
        
        if progress_callback:
            progress_callback(100)
        return "Analysis completed."

    def on_analysis_completed(self, result):
        if isinstance(result, Exception):
            self.status_label.setText(f"Error: {result}")
        else:
            self.status_label.setText("Data analysis completed successfully.")
        self.enable_buttons()
        self.update_progress(100)

    def _analyze_data_task(self, df_videos, output_folder, progress_callback=None):
        """
        Analyze data (generate Top-N, statistics) and update progress.
        """
        try:
            top_n = int(self.top_n_input.text())
            if top_n < 1 or top_n > 100:
                top_n = 10  
        except ValueError:
            top_n = 10  
        compute_statistics(df_videos, output_folder)
        compute_top_videos(df_videos, output_folder, top_n)
        
        if progress_callback:
            progress_callback(100)
        return f"Analysis completed with Top {top_n} data."
    
    def load_data_spark(self):
        """
        Load data using Spark.
        """
        self.status_label.setText("Loading data using Spark...")
        self.disable_buttons()

        def load_data_task(*args):  
            try:
                spark = SparkSession.builder \
                    .appName("YouTubeDataAnalysis") \
                    .config("spark.driver.memory", "8g") \
                    .config("spark.executor.memory", "8g") \
                    .config("spark.executor.cores", "2") \
                    .config("spark.sql.shuffle.partitions", "100") \
                    .config("spark.memory.fraction", "0.6") \
                    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
                    .config("spark.mongodb.read.database", "youtube_analyzer") \
                    .config("spark.mongodb.read.collection", "videos") \
                    .config("spark.jars", ",".join([
                        os.path.abspath(os.path.join("jar", jar)) for jar in [
                            "mongo-spark-connector-10.4.0.jar",
                            "bson-4.10.0.jar",
                            "mongodb-driver-core-4.10.0.jar",
                            "mongodb-driver-sync-4.10.0.jar"
                        ]
                    ])) \
                    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
                    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
                    .getOrCreate()

                df_videos = spark.read.format("mongodb").load()
                output_folder = "spark_output"
                os.makedirs(output_folder, exist_ok=True)

                df_videos = clean_and_prepare_data(df_videos)
                df_videos.write.mode("overwrite").parquet(os.path.join(output_folder, "cleaned_data.parquet"))



                # output_folder = "spark_output"
                # os.makedirs(output_folder, exist_ok=True)

                # df_videos = spark.read.format("mongodb").load()
                # df_videos = clean_and_prepare_data(df_videos)
                # df_videos.cache()

                # return df_videos.schema.simpleString()




                # output_file = os.path.join(output_folder, "spark_loaded_data.csv")
                # df_videos.write.csv(output_file, header=True, mode="overwrite")

                


                return "Data loaded successfully."
            except Exception as e:
                return e
            finally:
                if 'spark' in locals():
                    spark.stop()

        self.worker = Worker(load_data_task)  
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.result_signal.connect(self.on_spark_data_loaded)
        self.worker.start()


    def on_spark_data_loaded(self, result):
        if isinstance(result, Exception):
            self.status_label.setText(f"Error: {result}")
        else:
            self.status_label.setText(result)
        self.enable_buttons()
    
    def analyze_data_spark(self):
        """
        Analyze data using Spark.
        """
        self.status_label.setText("Analyzing data using Spark...")
        self.disable_buttons()

        def analyze_task(*args):
            try:
                spark = SparkSession.builder \
                    .appName("YouTubeDataAnalysis") \
                    .config("spark.jars", ",".join([
                        os.path.abspath(os.path.join("jar", jar)) for jar in [
                            "mongo-spark-connector-10.4.0.jar",
                            "bson-4.10.0.jar",
                            "mongodb-driver-core-4.10.0.jar",
                            "mongodb-driver-sync-4.10.0.jar"
                        ]
                    ])) \
                    .getOrCreate()

                output_folder = "spark_output"
                cleaned_data_path = os.path.join(output_folder, "cleaned_data.parquet")
                if not os.path.exists(cleaned_data_path):
                    raise FileNotFoundError(f"Cleaned data not found at {cleaned_data_path}. Please load data first.")

                df_videos = spark.read.parquet(cleaned_data_path)

                spark_analyze_data(spark, df_videos, output_folder)
                analyze_related_videos(df_videos, output_folder)

                return "Spark analysis completed successfully."
            except Exception as e:
                return e
            finally:
                if 'spark' in locals():
                    spark.stop()

        self.worker = Worker(analyze_task)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.result_signal.connect(self.on_spark_analysis_completed)
        self.worker.start()

    def _analyze_data_spark_task(self, output_folder, progress_callback=None):
        """
        Perform Spark-based data analysis and visualization.
        """
        

        spark = SparkSession.builder \
                    .appName("YouTubeDataAnalysis") \
                    .config("spark.driver.memory", "8g") \
                    .config("spark.executor.memory", "8g") \
                    .config("spark.executor.cores", "2") \
                    .config("spark.sql.shuffle.partitions", "100") \
                    .config("spark.memory.fraction", "0.6") \
                    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017") \
                    .config("spark.mongodb.read.database", "youtube_analyzer") \
                    .config("spark.mongodb.read.collection", "videos") \
                    .config("spark.jars", ",".join([
                        os.path.abspath(os.path.join("jar", jar)) for jar in [
                            "mongo-spark-connector-10.4.0.jar",
                            "bson-4.10.0.jar",
                            "mongodb-driver-core-4.10.0.jar",
                            "mongodb-driver-sync-4.10.0.jar"
                        ]
                    ])) \
                    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
                    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
                    .getOrCreate()

        try:
            df_videos = spark.read.format("mongodb").load()
            df_videos = clean_and_prepare_data(df_videos)

            spark_analyze_data(spark, df_videos, output_folder)
            analyze_related_videos(df_videos, output_folder)

            merge_and_rename_spark_output(output_folder, "trends")
            merge_and_rename_spark_output(output_folder, "category_stats")
            merge_and_rename_spark_output(output_folder, "top_10_views")
            merge_and_rename_spark_output(output_folder, "related_analysis")
            merge_and_rename_spark_output(output_folder, "top_10_ratings")

            generate_visualizations(df_videos, output_folder)

            if progress_callback:
                progress_callback(100)

            return "Spark analysis completed successfully."
        except Exception as e:
            return e
        finally:
            spark.stop()

    def on_spark_analysis_completed(self, result):
        if isinstance(result, Exception):
            self.status_label.setText(f"Error: {result}")
        else:
            self.status_label.setText(result)
        self.enable_buttons()

    def load_spark_results(self):
        """
        Load and display Spark-generated results in the frontend.
        """
        spark_output_folder = "output"

        # Load only the analysis results, not the full dataset
        for file_name in ["category_stats.csv", "top_10_views.csv", "trends.csv", "top_10_ratings.csv"]:
            file_path = os.path.join(spark_output_folder, file_name)
            if os.path.exists(file_path):
                self.display_csv(file_path)


    def load_spark_images(self):
        """
        Display Spark-generated images in the frontend.
        """
        spark_output_folder = "output"

        for image_name in ["category_views.png", "top_10_videos.png", "trends_views_fixed.png", "trends_ratings_fixed.png"]:
            image_path = os.path.join(spark_output_folder, image_name)
            if os.path.exists(image_path):
                self.show_image(image_path)

    def load_single_result(self, file_name):
        """
        Load and display a single Spark result file.
        """
        spark_output_folder = "spark_output"
        file_path = os.path.join(spark_output_folder, file_name)
        if os.path.exists(file_path):
            self.display_csv(file_path)
            self.status_label.setText(f"Displayed {file_name} successfully.")
        else:
            self.status_label.setText(f"File not found: {file_name}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
