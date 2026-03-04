import sys
import os
import re
import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QPushButton, QFileDialog, QTextEdit, QLabel, QProgressBar
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QObject
from PyQt6.QtGui import QFont, QColor, QPalette

# =============================================================================
# Stream Object for Redirecting Print to UI
# =============================================================================
class OutputStream(QObject):
    text_written = pyqtSignal(str)

    def write(self, text):
        self.text_written.emit(str(text))
    
    def flush(self):
        pass

# =============================================================================
# Worker Thread for Data Processing
# =============================================================================
class ProcessorWorker(QThread):
    finished = pyqtSignal(pd.DataFrame)
    error = pyqtSignal(str)

    def __init__(self, input_file):
        super().__init__()
        self.input_file = input_file

    def run(self):
        try:
            # 1. Load Data
            df = pd.read_excel(self.input_file, engine="openpyxl")
            for col in ["Register Addr.1 (Local)", "Register Addr.2 (Local)",
                        "Permanent Addr.1 (Local)", "Permanent Addr.2 (Local)"]:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.strip().replace("nan", pd.NA)
            
            print(f"Total rows: {len(df)}")

            # --- Logic from pa_extract_address.py ---
            
            # Patterns
            home_pattern = r"(?:บ้านเลขที่|เลขที่)?\s*(\d+[A-Za-z]?(?:/\d+[A-Za-z]?)*)"
            moo_pattern = r"\b(?:หมู่ที่|หมู่|ม\.)\s*(\d+[A-Za-z]?(?:/\d+)?)(?=\s|$|,|ตำบล|แขวง|อำเภอ|เขต)"
            building_keywords = ["หมู่บ้าน", "โครงการ", "คอนโด", "แมนชั่น", "การเคหะ", "Garden", "Place", "Ville", "Home", "Condo", "Privacy", "Connect", "Town", "Residence", "Regent", "Escent", "The", "Golden"]
            keyword_pattern = "|".join(building_keywords)
            floor_pattern = r"ชั้น(?:ที่)?\s*([^\s,]+)"
            soi_pattern = r"(?:ซอย|ซ\.)[\s]*([0-9A-Za-zก-๙\/\-]+)"
            street_pattern = r"(?:\(\s*ถนน([^)]+)\))|(?:ถ\.\s*([^\n\r]*?))(?=\s*(?:ซอย|ซ\.|หมู่|ม\.|ตำบล|แขวง|อำเภอ|เขต|จังหวัด|แยก|เลขที่|ห้อง|$))|(?:ถนน\s*([^\n\r]*?))(?=\s*(?:ซอย|ซ\.|หมู่|ม\.|ตำบล|แขวง|อำเภอ|เขต|จังหวัด|แยก|เลขที่|ห้อง|$))"

            def extract_with_fallback(df, primary_col, fallback_col, pattern, flags=0):
                primary_result = df[primary_col].str.extract(pattern, flags=flags, expand=False)
                fallback_result = df[fallback_col].str.extract(pattern, flags=flags, expand=False)
                return primary_result.fillna(fallback_result).str.strip()

            def extract_building_with_fallback(primary_series, fallback_series, keyword_pattern):
                def get_b(series):
                    paren = series.str.extract(rf"\((.*?(?:{keyword_pattern}).*?)\)")[0]
                    main = series.str.extract(rf"((?:{keyword_pattern})[^\n\r]*?)(?=\s*(?:ถนน|ถ\.|ซอย|ซ\.|หมู่|ม\.|แยก|$))")[0]
                    return paren.fillna(main).str.strip()
                return get_b(primary_series).fillna(get_b(fallback_series)).str.strip()

            def extract_street_with_fallback(p_data, f_data, pattern, flags=0):
                def get_s(data):
                    return data.str.extract(pattern, flags=flags).bfill(axis=1).iloc[:, 0].str.strip()
                return get_s(p_data).fillna(get_s(f_data))

            # Extraction Process
            print("\n" + "="*60)
            print("REGISTER ADDRESS - Extracting fields...")
            print("="*60)
            df["Register_HomeNo"] = extract_with_fallback(df, "Register Addr.1 (Local)", "Register Addr.2 (Local)", home_pattern)
            df["Register_Moo"] = extract_with_fallback(df, "Register Addr.2 (Local)", "Register Addr.1 (Local)", moo_pattern)
            df["Register_Building"] = extract_building_with_fallback(df["Register Addr.2 (Local)"], df["Register Addr.1 (Local)"], keyword_pattern)
            df["Register_Floor"] = extract_with_fallback(df, "Register Addr.2 (Local)", "Register Addr.1 (Local)", floor_pattern)
            df["Register_Soi"] = extract_with_fallback(df, "Register Addr.2 (Local)", "Register Addr.1 (Local)", soi_pattern)
            df["Register_Street"] = extract_street_with_fallback(df["Register Addr.2 (Local)"], df["Register Addr.1 (Local)"], street_pattern)
            print("Register columns created: HomeNo, Moo, Building, Floor, Soi, Street ✓")

            print("\n" + "="*60)
            print("PERMANENT ADDRESS - Extracting fields...")
            print("="*60)
            df["Permanent_HomeNo"] = extract_with_fallback(df, "Permanent Addr.1 (Local)", "Permanent Addr.2 (Local)", home_pattern)
            df["Permanent_Moo"] = extract_with_fallback(df, "Permanent Addr.2 (Local)", "Permanent Addr.1 (Local)", moo_pattern)
            df["Permanent_Building"] = extract_building_with_fallback(df["Permanent Addr.2 (Local)"], df["Permanent Addr.1 (Local)"], keyword_pattern)
            df["Permanent_Floor"] = extract_with_fallback(df, "Permanent Addr.2 (Local)", "Permanent Addr.1 (Local)", floor_pattern)
            df["Permanent_Soi"] = extract_with_fallback(df, "Permanent Addr.2 (Local)", "Permanent Addr.1 (Local)", soi_pattern)
            df["Permanent_Street"] = extract_street_with_fallback(df["Permanent Addr.2 (Local)"], df["Permanent Addr.1 (Local)"], street_pattern)
            print("Permanent columns created: HomeNo, Moo, Building, Floor, Soi, Street ✓")

            # Quality Check Logic (Simplified for UI view as requested)
            results = []
            def run_qc(col, p_col, f_col, trig, label):
                total = len(df)
                filled = df[col].notna().sum()
                missing = total - filled
                fn = len(df[(df[p_col].str.contains(trig, na=False) | df[f_col].str.contains(trig, na=False)) & df[col].isna()])
                results.append({"Column": label, "Total": total, "Filled": filled, "Missing": missing, "Issue": fn})

            # Sample checks for summary
            run_qc("Register_HomeNo", "Register Addr.1 (Local)", "Register Addr.2 (Local)", r"^\s*\d", "Reg HomeNo")
            run_qc("Register_Moo", "Register Addr.2 (Local)", "Register Addr.1 (Local)", r"ม\.", "Reg Moo")
            run_qc("Register_Building", "Register Addr.2 (Local)", "Register Addr.1 (Local)", keyword_pattern, "Reg Building")
            run_qc("Permanent_HomeNo", "Permanent Addr.1 (Local)", "Permanent Addr.2 (Local)", r"^\s*\d", "Perm HomeNo")
            run_qc("Permanent_Moo", "Permanent Addr.2 (Local)", "Permanent Addr.1 (Local)", r"ม\.", "Perm Moo")

            print("\n" + "="*60)
            print("📊 OVERALL QUALITY SUMMARY")
            print("="*60)
            summary_df = pd.DataFrame(results)
            print(summary_df.to_string(index=False))

            self.finished.emit(df)
        except Exception as e:
            self.error.emit(str(e))

# =============================================================================
# Main Application UI
# =============================================================================
class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Thai Address Extractor - Portable Tool")
        self.resize(900, 700)
        self.input_path = ""
        self.processed_df = None
        self.init_ui()
        
        # Redirect stdout
        sys.stdout = OutputStream()
        sys.stdout.text_written.connect(self.update_console)

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(15)

        # Style Settings
        self.setStyleSheet("""
            QMainWindow { background-color: #1e1e1e; }
            QLabel { color: #e0e0e0; font-size: 14px; }
            QPushButton { 
                background-color: #333333; color: white; border: 1px solid #555555; 
                padding: 10px; border-radius: 5px; font-weight: bold;
            }
            QPushButton:hover { background-color: #444444; border-color: #00FF00; }
            QPushButton#processBtn { background-color: #005f00; border-color: #00FF00; }
            QPushButton#processBtn:disabled { background-color: #222222; color: #666666; }
        """)

        # File Selection Section
        file_layout = QHBoxLayout()
        self.lbl_file = QLabel("No file selected...")
        btn_select = QPushButton("📁 Select Excel File")
        btn_select.clicked.connect(self.select_file)
        file_layout.addWidget(self.lbl_file, 1)
        file_layout.addWidget(btn_select)
        layout.addLayout(file_layout)

        # Console Section (Matrix Style)
        layout.addWidget(QLabel("Process Output:"))
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setFont(QFont("Consolas", 10))
        # Matrix Style Colors
        p = self.console.palette()
        p.setColor(QPalette.ColorRole.Base, QColor(0, 0, 0))
        p.setColor(QPalette.ColorRole.Text, QColor(0, 255, 0))
        self.console.setPalette(p)
        layout.addWidget(self.console)

        # Progress Bar
        self.progress = QProgressBar()
        self.progress.setTextVisible(False)
        self.progress.setStyleSheet("QProgressBar::chunk { background-color: #00FF00; }")
        self.progress.hide()
        layout.addWidget(self.progress)

        # Action Buttons
        btn_layout = QHBoxLayout()
        self.btn_run = QPushButton("⚡ Start Extraction")
        self.btn_run.setObjectName("processBtn")
        self.btn_run.setEnabled(False)
        self.btn_run.clicked.connect(self.run_process)

        self.btn_save = QPushButton("💾 Export Result")
        self.btn_save.setEnabled(False)
        self.btn_save.clicked.connect(self.save_file)

        btn_layout.addWidget(self.btn_run)
        btn_layout.addWidget(self.btn_save)
        layout.addLayout(btn_layout)

    def update_console(self, text):
        cursor = self.console.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        cursor.insertText(text)
        self.console.setTextCursor(cursor)
        self.console.ensureCursorVisible()

    def select_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select Excel File", "", "Excel Files (*.xlsx *.xls)")
        if file_path:
            self.input_path = file_path
            self.lbl_file.setText(os.path.basename(file_path))
            self.btn_run.setEnabled(True)
            self.console.clear()
            print(f"[SYSTEM]: File loaded -> {file_path}")

    def run_process(self):
        if not self.input_path: return
        
        self.btn_run.setEnabled(False)
        self.btn_save.setEnabled(False)
        self.progress.show()
        self.progress.setRange(0, 0) # Indeterminate mode
        self.console.clear()
        
        self.worker = ProcessorWorker(self.input_path)
        self.worker.finished.connect(self.on_finished)
        self.worker.error.connect(self.on_error)
        self.worker.start()

    def on_finished(self, df):
        self.processed_df = df
        self.progress.hide()
        self.btn_run.setEnabled(True)
        self.btn_save.setEnabled(True)
        print("\n[SUCCESS]: Address extraction completed!")

    def on_error(self, message):
        self.progress.hide()
        self.btn_run.setEnabled(True)
        print(f"\n[ERROR]: {message}")

    def save_file(self):
        if self.processed_df is None: return
        
        save_path, _ = QFileDialog.getSaveFileName(self, "Save Exported File", "PA_Address_Extracted.xlsx", "Excel Files (*.xlsx)")
        if save_path:
            try:
                # Define columns to export (same logic as script)
                all_cols = self.processed_df.columns.tolist()
                important_keywords = ["Register_", "Permanent_", "รหัส", "ชื่อ", "นามสกุล", "Addr"]
                output_cols = [c for c in all_cols if any(k in c for k in important_keywords)]
                
                self.processed_df[output_cols].to_excel(save_path, index=False, engine="openpyxl")
                print(f"\n[SYSTEM]: File exported successfully to: {save_path}")
            except Exception as e:
                print(f"\n[ERROR]: Could not save file. {str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = App()
    window.show()
    sys.exit(app.exec())