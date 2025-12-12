import sys
import json
import logging
import csv
import os
from datetime import datetime, timedelta
from collections import deque
from pathlib import Path

import paho.mqtt.client as mqtt
import pandas as pd
import numpy as np
import pyqtgraph as pg
from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
                             QWidget, QLabel, QPushButton, QTabWidget, QGridLayout,
                             QGroupBox, QTableWidget, QTableWidgetItem, QHeaderView,
                             QStatusBar, QSplitter, QComboBox, QSpinBox, QCheckBox,
                             QFileDialog, QMessageBox, QLineEdit, QProgressBar)
from PyQt5.QtCore import QTimer, Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QPalette, QColor, QIcon
import pyqtgraph as pg

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CSVDataLogger:
    """Handles CSV file logging operations"""

    def __init__(self, data_dir="power_logs"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.current_file = None
        self.csv_writer = None
        self.csv_file = None
        self.fieldnames = [
            'timestamp', 'device_id', 'voltage', 'current', 'active_power',
            'reactive_power', 'apparent_power', 'power_factor', 'frequency',
            'energy', 'temperature', 'load_type', 'probe_id'
        ]

    def create_new_file(self, filename=None):
        """Create a new CSV file for logging"""
        if self.csv_file:
            self.close_file()

        if filename is None:
            filename = f"power_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        self.current_file = self.data_dir / filename

        # Create file and write header
        self.csv_file = open(self.current_file, 'a', newline='')
        self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=self.fieldnames)

        # Write header if file is empty
        if os.path.getsize(self.current_file) == 0:
            self.csv_writer.writeheader()
            self.csv_file.flush()

        logger.info(f"Created new log file: {self.current_file}")
        return self.current_file

    def log_data(self, data):
        """Log data to CSV file"""
        if not self.csv_writer or not self.csv_file:
            logger.warning("No CSV file open for logging")
            return False

        try:
            # Ensure all required fields are present
            log_entry = {
                'timestamp': data.get('timestamp', datetime.now()).isoformat(),
                'device_id': data.get('device_id', 'unknown'),
                'voltage': data.get('voltage', 0),
                'current': data.get('current', 0),
                'active_power': data.get('active_power', 0),
                'reactive_power': data.get('reactive_power', 0),
                'apparent_power': data.get('apparent_power', 0),
                'power_factor': data.get('power_factor', 0),
                'frequency': data.get('frequency', 50),
                'energy': data.get('energy', 0),
                'temperature': data.get('temperature', 25),
                'load_type': data.get('load_type', 'unknown'),
                'probe_id': data.get('probe_id', 'default')
            }

            self.csv_writer.writerow(log_entry)
            self.csv_file.flush()
            return True

        except Exception as e:
            logger.error(f"Error logging data to CSV: {e}")
            return False

    def close_file(self):
        """Close the current CSV file"""
        if self.csv_file:
            self.csv_file.close()
            self.csv_file = None
            self.csv_writer = None

    def get_all_log_files(self):
        """Get list of all CSV log files"""
        return list(self.data_dir.glob("*.csv"))

    def merge_log_files(self, output_filename="merged_power_data.csv"):
        """Merge all CSV files into one"""
        all_files = self.get_all_log_files()
        if not all_files:
            return None

        merged_data = []
        for file in all_files:
            try:
                df = pd.read_csv(file)
                merged_data.append(df)
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")

        if merged_data:
            merged_df = pd.concat(merged_data, ignore_index=True)
            output_path = self.data_dir / output_filename
            merged_df.to_csv(output_path, index=False)
            return output_path
        return None


class MQTTWorker(QThread):
    data_received = pyqtSignal(dict)
    connection_status = pyqtSignal(bool)

    def __init__(self):
        super().__init__()
        self.client = None
        self.running = True
        self.broker = "broker.hivemq.com"
        self.port = 1883
        self.topic = "nivra_power/all"

    def run(self):
        self.setup_mqtt()

    def setup_mqtt(self):
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.on_disconnect = self.on_disconnect

            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            logger.info("MQTT client started")

        except Exception as e:
            logger.error(f"MQTT setup error: {e}")
            self.connection_status.emit(False)

    def on_connect(self, client, userdata, flags, rc, properties):
        if rc == 0:
            client.subscribe(self.topic)
            logger.info("Connected to MQTT broker")
            self.connection_status.emit(True)
        else:
            logger.error(f"Connection failed with code: {rc}")
            self.connection_status.emit(False)

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode()
            data = json.loads(payload)
            self.data_received.emit(data)
        except Exception as e:
            logger.error(f"Message processing error: {e}")

    def on_disconnect(self, client, userdata, rc, properties):
        logger.warning("Disconnected from MQTT broker")
        self.connection_status.emit(False)

    def stop(self):
        self.running = False
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()


class GaugeWidget(QWidget):
    def __init__(self, title, min_val, max_val, unit, parent=None):
        super().__init__(parent)
        self.title = title
        self.min_val = min_val
        self.max_val = max_val
        self.unit = unit
        self.value = 0
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout()

        # Title
        title_label = QLabel(self.title)
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setFont(QFont("Arial", 12, QFont.Bold))
        layout.addWidget(title_label)

        # Value display
        self.value_label = QLabel("0.00")
        self.value_label.setAlignment(Qt.AlignCenter)
        self.value_label.setFont(QFont("Arial", 24, QFont.Bold))
        layout.addWidget(self.value_label)

        # Unit
        unit_label = QLabel(self.unit)
        unit_label.setAlignment(Qt.AlignCenter)
        unit_label.setFont(QFont("Arial", 10))
        layout.addWidget(unit_label)

        self.setLayout(layout)
        self.setMinimumSize(150, 120)

    def update_value(self, value):
        self.value = value
        self.value_label.setText(f"{value:.2f}")


class RealTimePlotWidget(pg.PlotWidget):
    def __init__(self, title, y_label, parent=None):
        super().__init__(parent)
        self.title = title
        self.y_label = y_label
        self.data = deque(maxlen=100)
        self.timestamps = deque(maxlen=100)
        self.setup_plot()

    def setup_plot(self):
        self.setLabel('left', self.y_label)
        self.setLabel('bottom', 'Time')
        self.setTitle(self.title)
        self.showGrid(x=True, y=True, alpha=0.3)
        self.curve = self.plot(pen=pg.mkPen('b', width=2))

    def update_plot(self, value):
        current_time = datetime.now()
        self.data.append(value)
        self.timestamps.append(current_time)

        # Convert timestamps to seconds from start
        if len(self.timestamps) > 1:
            times = [(ts - self.timestamps[0]).total_seconds() for ts in self.timestamps]
            self.curve.setData(times, list(self.data))


class PowerDesktopApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.data_history = deque(maxlen=500)
        self.current_data = {}
        self.csv_logger = CSVDataLogger()
        self.setup_ui()
        self.setup_mqtt()
        self.setup_timers()

    def setup_ui(self):
        self.setWindowTitle("⚡ Advanced Power Monitoring System")
        self.setGeometry(100, 100, 1400, 900)

        # Central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Main layout
        main_layout = QVBoxLayout(central_widget)

        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Initializing...")

        # Create tabs
        self.tabs = QTabWidget()
        main_layout.addWidget(self.tabs)

        # Dashboard tab
        self.setup_dashboard_tab()

        # Real-time plots tab
        self.setup_plots_tab()

        # Data analysis tab
        self.setup_analysis_tab()

        # Data logging tab
        self.setup_logging_tab()

        # Settings tab
        self.setup_settings_tab()

    def setup_dashboard_tab(self):
        dashboard_tab = QWidget()
        layout = QVBoxLayout(dashboard_tab)

        # Connection status
        status_layout = QHBoxLayout()
        self.connection_label = QLabel("🔴 Disconnected")
        self.connection_label.setFont(QFont("Arial", 12, QFont.Bold))
        status_layout.addWidget(self.connection_label)
        status_layout.addStretch()

        self.message_count_label = QLabel("Messages: 0")
        status_layout.addWidget(self.message_count_label)

        self.csv_status_label = QLabel("CSV: Not Logging")
        self.csv_status_label.setStyleSheet("color: orange;")
        status_layout.addWidget(self.csv_status_label)

        layout.addLayout(status_layout)

        # Gauges
        gauges_layout = QGridLayout()

        # Voltage gauge
        self.voltage_gauge = GaugeWidget("Voltage", 0, 250, "V")
        gauges_layout.addWidget(self.voltage_gauge, 0, 0)

        # Current gauge
        self.current_gauge = GaugeWidget("Current", 0, 20, "A")
        gauges_layout.addWidget(self.current_gauge, 0, 1)

        # Active Power gauge
        self.power_gauge = GaugeWidget("Active Power", 0, 5000, "W")
        gauges_layout.addWidget(self.power_gauge, 0, 2)

        # Power Factor gauge
        self.pf_gauge = GaugeWidget("Power Factor", 0, 1, "")
        gauges_layout.addWidget(self.pf_gauge, 0, 3)

        # Apparent Power gauge
        self.apparent_gauge = GaugeWidget("Apparent Power", 0, 5000, "VA")
        gauges_layout.addWidget(self.apparent_gauge, 1, 0)

        # Reactive Power gauge
        self.reactive_gauge = GaugeWidget("Reactive Power", 0, 5000, "VAR")
        gauges_layout.addWidget(self.reactive_gauge, 1, 1)

        layout.addLayout(gauges_layout)

        # Quick stats
        stats_group = QGroupBox("Quick Statistics")
        stats_layout = QGridLayout()

        self.avg_voltage_label = QLabel("Avg Voltage: -- V")
        self.avg_current_label = QLabel("Avg Current: -- A")
        self.total_energy_label = QLabel("Total Energy: -- kWh")
        self.max_power_label = QLabel("Max Power: -- W")

        stats_layout.addWidget(self.avg_voltage_label, 0, 0)
        stats_layout.addWidget(self.avg_current_label, 0, 1)
        stats_layout.addWidget(self.total_energy_label, 1, 0)
        stats_layout.addWidget(self.max_power_label, 1, 1)

        stats_group.setLayout(stats_layout)
        layout.addWidget(stats_group)

        self.tabs.addTab(dashboard_tab, "📊 Dashboard")

    def setup_plots_tab(self):
        plots_tab = QWidget()
        layout = QVBoxLayout(plots_tab)

        # Create real-time plots
        splitter = QSplitter(Qt.Vertical)

        # Voltage plot
        self.voltage_plot = RealTimePlotWidget("Voltage Over Time", "Voltage (V)")
        splitter.addWidget(self.voltage_plot)

        # Current plot
        self.current_plot = RealTimePlotWidget("Current Over Time", "Current (A)")
        splitter.addWidget(self.current_plot)

        # Power plot
        self.power_plot = RealTimePlotWidget("Power Over Time", "Power (W)")
        splitter.addWidget(self.power_plot)

        # Power Factor plot
        self.pf_plot = RealTimePlotWidget("Power Factor Over Time", "Power Factor")
        splitter.addWidget(self.pf_plot)

        splitter.setSizes([200, 200, 200, 200])
        layout.addWidget(splitter)

        self.tabs.addTab(plots_tab, "📈 Real-time Plots")

    def setup_analysis_tab(self):
        analysis_tab = QWidget()
        layout = QVBoxLayout(analysis_tab)

        # Data table
        self.data_table = QTableWidget()
        self.data_table.setColumnCount(10)
        self.data_table.setHorizontalHeaderLabels([
            "Timestamp", "Device ID", "Voltage (V)", "Current (A)",
            "Active Power (W)", "Reactive Power (VAR)",
            "Apparent Power (VA)", "Power Factor", "Frequency (Hz)", "Load Type"
        ])
        self.data_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.data_table)

        # Export button
        export_btn = QPushButton("Export Selected Data to CSV")
        export_btn.clicked.connect(self.export_data)
        layout.addWidget(export_btn)

        self.tabs.addTab(analysis_tab, "📋 Data Analysis")

    def setup_logging_tab(self):
        logging_tab = QWidget()
        layout = QVBoxLayout(logging_tab)

        # Logging controls
        controls_group = QGroupBox("Data Logging Controls")
        controls_layout = QGridLayout()

        # Auto-logging checkbox
        self.auto_log_checkbox = QCheckBox("Enable Auto-Logging")
        self.auto_log_checkbox.setChecked(True)
        self.auto_log_checkbox.stateChanged.connect(self.toggle_auto_logging)
        controls_layout.addWidget(self.auto_log_checkbox, 0, 0)

        # Manual logging button
        self.manual_log_btn = QPushButton("Start Manual Logging")
        self.manual_log_btn.clicked.connect(self.toggle_manual_logging)
        controls_layout.addWidget(self.manual_log_btn, 0, 1)

        # Filename input
        controls_layout.addWidget(QLabel("Log Filename:"), 1, 0)
        self.log_filename_input = QLineEdit()
        self.log_filename_input.setPlaceholderText("auto_generate")
        controls_layout.addWidget(self.log_filename_input, 1, 1)

        # Create new log button
        self.new_log_btn = QPushButton("Create New Log File")
        self.new_log_btn.clicked.connect(self.create_new_log_file)
        controls_layout.addWidget(self.new_log_btn, 2, 0, 1, 2)

        # Log stats
        controls_layout.addWidget(QLabel("Current Log File:"), 3, 0)
        self.current_log_label = QLabel("No active log file")
        controls_layout.addWidget(self.current_log_label, 3, 1)

        controls_layout.addWidget(QLabel("Entries Logged:"), 4, 0)
        self.entries_logged_label = QLabel("0")
        controls_layout.addWidget(self.entries_logged_label, 4, 1)

        controls_group.setLayout(controls_layout)
        layout.addWidget(controls_group)

        # Log management
        management_group = QGroupBox("Log Management")
        management_layout = QGridLayout()

        # View logs button
        view_logs_btn = QPushButton("View All Log Files")
        view_logs_btn.clicked.connect(self.view_log_files)
        management_layout.addWidget(view_logs_btn, 0, 0)

        # Merge logs button
        merge_logs_btn = QPushButton("Merge All Logs")
        merge_logs_btn.clicked.connect(self.merge_log_files)
        management_layout.addWidget(merge_logs_btn, 0, 1)

        # Clear logs button
        clear_logs_btn = QPushButton("Clear Old Logs")
        clear_logs_btn.clicked.connect(self.clear_old_logs)
        management_layout.addWidget(clear_logs_btn, 1, 0)

        # Export selected logs
        export_selected_btn = QPushButton("Export Selected Period")
        export_selected_btn.clicked.connect(self.export_selected_period)
        management_layout.addWidget(export_selected_btn, 1, 1)

        management_group.setLayout(management_layout)
        layout.addWidget(management_group)

        # Progress bar for operations
        self.log_progress_bar = QProgressBar()
        self.log_progress_bar.setVisible(False)
        layout.addWidget(self.log_progress_bar)

        layout.addStretch()
        self.tabs.addTab(logging_tab, "📝 Data Logging")

    def setup_settings_tab(self):
        settings_tab = QWidget()
        layout = QVBoxLayout(settings_tab)

        # MQTT Settings
        mqtt_group = QGroupBox("MQTT Settings")
        mqtt_layout = QGridLayout()

        mqtt_layout.addWidget(QLabel("Broker:"), 0, 0)
        self.broker_input = QComboBox()
        self.broker_input.addItems(["broker.hivemq.com", "test.mosquitto.org", "localhost"])
        mqtt_layout.addWidget(self.broker_input, 0, 1)

        mqtt_layout.addWidget(QLabel("Port:"), 1, 0)
        self.port_input = QSpinBox()
        self.port_input.setRange(1, 65535)
        self.port_input.setValue(1883)
        mqtt_layout.addWidget(self.port_input, 1, 1)

        mqtt_layout.addWidget(QLabel("Topic:"), 2, 0)
        self.topic_input = QComboBox()
        self.topic_input.setEditable(True)
        self.topic_input.addItems(["nivra_power/all", "power/monitoring", "sensors/power"])
        mqtt_layout.addWidget(self.topic_input, 2, 1)

        mqtt_group.setLayout(mqtt_layout)
        layout.addWidget(mqtt_group)

        # Logging Settings
        logging_group = QGroupBox("Logging Settings")
        logging_layout = QGridLayout()

        logging_layout.addWidget(QLabel("Log Directory:"), 0, 0)
        self.log_dir_input = QLineEdit("power_logs")
        logging_layout.addWidget(self.log_dir_input, 0, 1)

        browse_btn = QPushButton("Browse")
        browse_btn.clicked.connect(self.browse_log_directory)
        logging_layout.addWidget(browse_btn, 0, 2)

        logging_group.setLayout(logging_layout)
        layout.addWidget(logging_group)

        # Control buttons
        control_layout = QHBoxLayout()

        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self.toggle_connection)
        control_layout.addWidget(self.connect_btn)

        clear_btn = QPushButton("Clear Data")
        clear_btn.clicked.connect(self.clear_data)
        control_layout.addWidget(clear_btn)

        layout.addLayout(control_layout)
        layout.addStretch()

        self.tabs.addTab(settings_tab, "⚙️ Settings")

    def setup_mqtt(self):
        self.mqtt_worker = MQTTWorker()
        self.mqtt_worker.data_received.connect(self.handle_mqtt_data)
        self.mqtt_worker.connection_status.connect(self.handle_connection_status)
        self.mqtt_worker.start()
        self.message_count = 0
        self.entries_logged = 0
        self.auto_logging_enabled = True
        self.manual_logging_active = False

    def setup_timers(self):
        # Update UI timer
        self.ui_timer = QTimer()
        self.ui_timer.timeout.connect(self.update_ui)
        self.ui_timer.start(1000)  # Update every second

        # Statistics timer
        self.stats_timer = QTimer()
        self.stats_timer.timeout.connect(self.update_statistics)
        self.stats_timer.start(5000)  # Update every 5 seconds

        # Auto-save timer (save to CSV every 10 seconds)
        self.auto_save_timer = QTimer()
        self.auto_save_timer.timeout.connect(self.auto_save_data)
        self.auto_save_timer.start(10000)  # Save every 10 seconds

    def handle_mqtt_data(self, data):
        self.message_count += 1
        self.current_data = data

        # Add timestamp and additional metadata
        data_with_timestamp = data.copy()
        data_with_timestamp['timestamp'] = datetime.now()
        data_with_timestamp['device_id'] = data.get('device_id', 'power_monitor_1')
        data_with_timestamp['load_type'] = data.get('load_type', 'unknown')
        data_with_timestamp['probe_id'] = data.get('probe_id', 'default')

        # Add default values if missing
        if 'frequency' not in data_with_timestamp:
            data_with_timestamp['frequency'] = 50  # Default Hz
        if 'energy' not in data_with_timestamp:
            data_with_timestamp['energy'] = 0
        if 'temperature' not in data_with_timestamp:
            data_with_timestamp['temperature'] = 25

        self.data_history.append(data_with_timestamp)

        # Auto-log if enabled
        if self.auto_logging_enabled or self.manual_logging_active:
            self.log_to_csv(data_with_timestamp)

        # Update status bar
        self.status_bar.showMessage(
            f"Last update: {datetime.now().strftime('%H:%M:%S')} | Messages: {self.message_count} | Logged: {self.entries_logged}")

    def handle_connection_status(self, connected):
        if connected:
            self.connection_label.setText("🟢 Connected")
            self.connection_label.setStyleSheet("color: green;")
            self.connect_btn.setText("Disconnect")
        else:
            self.connection_label.setText("🔴 Disconnected")
            self.connection_label.setStyleSheet("color: red;")
            self.connect_btn.setText("Connect")

    def log_to_csv(self, data):
        """Log data to CSV file"""
        try:
            success = self.csv_logger.log_data(data)
            if success:
                self.entries_logged += 1
                self.entries_logged_label.setText(str(self.entries_logged))
                self.csv_status_label.setText(f"CSV: Logging ({self.entries_logged})")
                self.csv_status_label.setStyleSheet("color: green;")
            return success
        except Exception as e:
            logger.error(f"Error logging to CSV: {e}")
            self.csv_status_label.setText("CSV: Error")
            self.csv_status_label.setStyleSheet("color: red;")
            return False

    def toggle_auto_logging(self, state):
        self.auto_logging_enabled = (state == Qt.Checked)
        status = "enabled" if self.auto_logging_enabled else "disabled"
        self.status_bar.showMessage(f"Auto-logging {status}")

    def toggle_manual_logging(self):
        self.manual_logging_active = not self.manual_logging_active

        if self.manual_logging_active:
            self.manual_log_btn.setText("Stop Manual Logging")
            self.manual_log_btn.setStyleSheet("background-color: green; color: white;")
            self.status_bar.showMessage("Manual logging started")

            # Create new log file if none exists
            if not self.csv_logger.current_file:
                self.create_new_log_file()
        else:
            self.manual_log_btn.setText("Start Manual Logging")
            self.manual_log_btn.setStyleSheet("")
            self.status_bar.showMessage("Manual logging stopped")

    def create_new_log_file(self):
        """Create a new CSV log file"""
        filename = self.log_filename_input.text().strip()
        if not filename or filename == "auto_generate":
            filename = None
        elif not filename.endswith('.csv'):
            filename += '.csv'

        new_file = self.csv_logger.create_new_file(filename)
        if new_file:
            self.current_log_label.setText(new_file.name)
            self.entries_logged = 0
            self.entries_logged_label.setText("0")
            self.csv_status_label.setText("CSV: Ready")
            self.csv_status_label.setStyleSheet("color: blue;")
            self.status_bar.showMessage(f"Created new log file: {new_file.name}")

    def view_log_files(self):
        """Display all log files"""
        log_files = self.csv_logger.get_all_log_files()
        if not log_files:
            QMessageBox.information(self, "No Log Files", "No log files found.")
            return

        message = f"Found {len(log_files)} log files:\n\n"
        for file in log_files:
            size_kb = os.path.getsize(file) / 1024
            message += f"• {file.name} ({size_kb:.1f} KB)\n"

        QMessageBox.information(self, "Log Files", message)

    def merge_log_files(self):
        """Merge all log files into one"""
        try:
            self.log_progress_bar.setVisible(True)
            self.log_progress_bar.setRange(0, 0)  # Indeterminate progress

            merged_file = self.csv_logger.merge_log_files()

            if merged_file:
                size_mb = os.path.getsize(merged_file) / (1024 * 1024)
                QMessageBox.information(self, "Merge Complete",
                                        f"Merged all logs into:\n{merged_file.name}\nSize: {size_mb:.2f} MB")
            else:
                QMessageBox.warning(self, "Merge Failed", "No data to merge.")

        except Exception as e:
            logger.error(f"Error merging files: {e}")
            QMessageBox.critical(self, "Merge Error", f"Error merging files: {e}")

        finally:
            self.log_progress_bar.setVisible(False)

    def clear_old_logs(self):
        """Clear log files older than specified days"""
        reply = QMessageBox.question(self, "Clear Old Logs",
                                     "Delete log files older than 30 days?",
                                     QMessageBox.Yes | QMessageBox.No)

        if reply == QMessageBox.Yes:
            try:
                deleted_count = 0
                log_files = self.csv_logger.get_all_log_files()

                for file in log_files:
                    file_age = datetime.now() - datetime.fromtimestamp(file.stat().st_mtime)
                    if file_age.days > 30:
                        file.unlink()
                        deleted_count += 1

                self.status_bar.showMessage(f"Deleted {deleted_count} old log files")
                QMessageBox.information(self, "Cleanup Complete",
                                        f"Deleted {deleted_count} old log files.")

            except Exception as e:
                logger.error(f"Error clearing logs: {e}")
                QMessageBox.critical(self, "Cleanup Error", f"Error: {e}")

    def export_selected_period(self):
        """Export data from selected time period"""
        # This could be extended to allow date/time selection
        filename, _ = QFileDialog.getSaveFileName(
            self, "Export Data", "power_data_export.csv", "CSV Files (*.csv)")

        if filename:
            try:
                df = pd.DataFrame(list(self.data_history))
                df.to_csv(filename, index=False)
                self.status_bar.showMessage(f"Data exported to {filename}")
                QMessageBox.information(self, "Export Complete",
                                        f"Data exported to:\n{filename}")
            except Exception as e:
                logger.error(f"Error exporting data: {e}")
                QMessageBox.critical(self, "Export Error", f"Error: {e}")

    def browse_log_directory(self):
        """Browse for log directory"""
        directory = QFileDialog.getExistingDirectory(self, "Select Log Directory")
        if directory:
            self.log_dir_input.setText(directory)
            self.csv_logger = CSVDataLogger(directory)
            self.status_bar.showMessage(f"Log directory changed to: {directory}")

    def auto_save_data(self):
        """Auto-save data to CSV"""
        if self.auto_logging_enabled and self.data_history:
            # Save the last entry if not already saved
            if self.data_history:
                last_data = self.data_history[-1]
                self.log_to_csv(last_data)

    def update_ui(self):
        if self.current_data:
            # Update gauges
            self.voltage_gauge.update_value(self.current_data.get('voltage', 0))
            self.current_gauge.update_value(self.current_data.get('current', 0))
            self.power_gauge.update_value(self.current_data.get('active_power', 0))
            self.pf_gauge.update_value(self.current_data.get('power_factor', 0))
            self.apparent_gauge.update_value(self.current_data.get('apparent_power', 0))
            self.reactive_gauge.update_value(self.current_data.get('reactive_power', 0))

            # Update plots
            self.voltage_plot.update_plot(self.current_data.get('voltage', 0))
            self.current_plot.update_plot(self.current_data.get('current', 0))
            self.power_plot.update_plot(self.current_data.get('active_power', 0))
            self.pf_plot.update_plot(self.current_data.get('power_factor', 0))

            # Update message count
            self.message_count_label.setText(f"Messages: {self.message_count}")

    def update_statistics(self):
        if len(self.data_history) > 0:
            # Calculate statistics
            voltages = [d.get('voltage', 0) for d in self.data_history]
            currents = [d.get('current', 0) for d in self.data_history]
            powers = [d.get('active_power', 0) for d in self.data_history]

            avg_voltage = np.mean(voltages) if voltages else 0
            avg_current = np.mean(currents) if currents else 0
            max_power = max(powers) if powers else 0

            # Calculate energy (approximate)
            total_energy = sum(powers) * 5 / 3600000  # Convert to kWh (5-second intervals)

            self.avg_voltage_label.setText(f"Avg Voltage: {avg_voltage:.1f} V")
            self.avg_current_label.setText(f"Avg Current: {avg_current:.3f} A")
            self.total_energy_label.setText(f"Total Energy: {total_energy:.3f} kWh")
            self.max_power_label.setText(f"Max Power: {max_power:.1f} W")

            # Update data table
            self.update_data_table()

    def update_data_table(self):
        if self.data_history:
            recent_data = list(self.data_history)[-10:]  # Last 10 entries
            self.data_table.setRowCount(len(recent_data))

            for row, data in enumerate(recent_data):
                timestamp = data.get('timestamp', datetime.now()).strftime('%H:%M:%S')
                device_id = data.get('device_id', 'N/A')
                voltage = data.get('voltage', 0)
                current = data.get('current', 0)
                active_power = data.get('active_power', 0)
                reactive_power = data.get('reactive_power', 0)
                apparent_power = data.get('apparent_power', 0)
                power_factor = data.get('power_factor', 0)
                frequency = data.get('frequency', 50)
                load_type = data.get('load_type', 'unknown')

                self.data_table.setItem(row, 0, QTableWidgetItem(timestamp))
                self.data_table.setItem(row, 1, QTableWidgetItem(device_id))
                self.data_table.setItem(row, 2, QTableWidgetItem(f"{voltage:.2f}"))
                self.data_table.setItem(row, 3, QTableWidgetItem(f"{current:.3f}"))
                self.data_table.setItem(row, 4, QTableWidgetItem(f"{active_power:.2f}"))
                self.data_table.setItem(row, 5, QTableWidgetItem(f"{reactive_power:.2f}"))
                self.data_table.setItem(row, 6, QTableWidgetItem(f"{apparent_power:.2f}"))
                self.data_table.setItem(row, 7, QTableWidgetItem(f"{power_factor:.3f}"))
                self.data_table.setItem(row, 8, QTableWidgetItem(f"{frequency:.1f}"))
                self.data_table.setItem(row, 9, QTableWidgetItem(load_type))

    def export_data(self):
        """Export current data table to CSV"""
        if self.data_history:
            filename, _ = QFileDialog.getSaveFileName(
                self, "Export Data", f"power_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                "CSV Files (*.csv)")

            if filename:
                try:
                    # Export all history data
                    df = pd.DataFrame(list(self.data_history))

                    # Convert timestamps to string
                    if 'timestamp' in df.columns:
                        df['timestamp'] = df['timestamp'].apply(
                            lambda x: x.isoformat() if hasattr(x, 'isoformat') else str(x))

                    df.to_csv(filename, index=False)
                    self.status_bar.showMessage(f"Data exported to {filename}")
                    QMessageBox.information(self, "Export Complete",
                                            f"Exported {len(df)} records to:\n{filename}")
                except Exception as e:
                    logger.error(f"Error exporting data: {e}")
                    QMessageBox.critical(self, "Export Error", f"Error: {e}")

    def toggle_connection(self):
        if self.mqtt_worker.running:
            self.mqtt_worker.stop()
            self.connect_btn.setText("Connect")
        else:
            self.mqtt_worker.start()
            self.connect_btn.setText("Disconnect")

    def clear_data(self):
        reply = QMessageBox.question(self, "Clear Data",
                                     "Clear all data history?",
                                     QMessageBox.Yes | QMessageBox.No)

        if reply == QMessageBox.Yes:
            self.data_history.clear()
            self.message_count = 0
            self.current_data = {}
            self.status_bar.showMessage("Data cleared")

    def closeEvent(self, event):
        # Stop logging
        self.csv_logger.close_file()

        # Stop MQTT worker
        self.mqtt_worker.stop()
        self.mqtt_worker.wait(2000)  # Wait up to 2 seconds for thread to finish

        # Save remaining data
        if self.data_history:
            auto_save_file = f"autosave_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.csv_logger.create_new_file(auto_save_file)
            for data in self.data_history:
                self.csv_logger.log_data(data)
            self.csv_logger.close_file()
            logger.info(f"Auto-saved data to {auto_save_file}")

        event.accept()


def main():
    # Set dark theme style
    app = QApplication(sys.argv)

    # Apply dark theme
    app.setStyle('Fusion')
    dark_palette = QPalette()
    dark_palette.setColor(QPalette.Window, QColor(53, 53, 53))
    dark_palette.setColor(QPalette.WindowText, Qt.white)
    dark_palette.setColor(QPalette.Base, QColor(25, 25, 25))
    dark_palette.setColor(QPalette.AlternateBase, QColor(53, 53, 53))
    dark_palette.setColor(QPalette.ToolTipBase, Qt.white)
    dark_palette.setColor(QPalette.ToolTipText, Qt.white)
    dark_palette.setColor(QPalette.Text, Qt.white)
    dark_palette.setColor(QPalette.Button, QColor(53, 53, 53))
    dark_palette.setColor(QPalette.ButtonText, Qt.white)
    dark_palette.setColor(QPalette.BrightText, Qt.red)
    dark_palette.setColor(QPalette.Link, QColor(42, 130, 218))
    dark_palette.setColor(QPalette.Highlight, QColor(42, 130, 218))
    dark_palette.setColor(QPalette.HighlightedText, Qt.black)
    app.setPalette(dark_palette)

    # Create and show main window
    window = PowerDesktopApp()
    window.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()