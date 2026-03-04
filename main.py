import sys
import os
import re
import pandas as pd
import numpy as np
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

    def __init__(self, input_file, mode="extract"):
        super().__init__()
        self.input_file = input_file
        self.mode = mode # "extract" or "convert"

    def run(self):
        try:
            # Load Data
            df = pd.read_excel(self.input_file, engine="openpyxl")
            
            if self.mode == "extract":
                self.run_extraction(df)
            else:
                self.run_template_conversion(df)
                
        except Exception as e:
            self.error.emit(str(e))

    def run_extraction(self, df):
        # Cleaning columns
        for col in ["Register Addr.1 (Local)", "Register Addr.2 (Local)",
                    "Permanent Addr.1 (Local)", "Permanent Addr.2 (Local)"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace("nan", pd.NA)
        
        print(f"Total rows: {len(df)}")

        # Patterns
        home_pattern = r"(?:บ้านเลขที่|เลขที่)?\s*(\d+[A-Za-z]?(?:/\d+[A-Za-z]?)*)"
        moo_pattern = r"\b(?:หมู่ที่|หมู่|ม\.)\s*(\d+[A-Za-z]?(?:/\d+)?)(?=\s|$|,|ตำบล|แขวง|อำเภอ|เขต)"
        building_keywords = ["หมู่บ้าน", "โครงการ", "คอนโด", "แมนชั่น", "การเคหะ", "Garden", "Place", "Ville", "Home", "Condo", "Privacy", "Connect", "Town", "Residence", "Regent", "Escent", "The", "Golden"]
        keyword_pattern = "|".join(building_keywords)
        floor_pattern = r"ชั้น(?:ที่)?\s*([^\s,]+)"
        soi_pattern = r"(?:ซอย|ซ\.)[\s]*([0-9A-Za-zก-๙\/\-]+)"
        street_pattern = r"(?:\(\s*ถนน([^)]+)\))|(?:ถ\.\s*([^\n\r]*?))(?=\s*(?:ซอย|ซ\.|หมู่|ม\.|ตำบล|แขวง|อำเภอ|เขต|จังหวัด|แยก|เลขที่|ห้อง|$))|(?:ถนน\s*([^\n\r]*?))(?=\s*(?:ซอย|ซ\.|หมู่|ม\.|ตำบล|แขวง|อำเภอ|เขต|จังหวัด|แยก|เลขที่|ห้อง|$))"

        # Helpers
        def extract_with_fallback(df, p_col, f_col, pattern, flags=0):
            res_p = df[p_col].str.extract(pattern, flags=flags, expand=False)
            res_f = df[f_col].str.extract(pattern, flags=flags, expand=False)
            return res_p.fillna(res_f).str.strip()

        def extract_building_with_fallback(p_ser, f_ser, kw_pat):
            def get_b(s):
                p = s.str.extract(rf"\((.*?(?:{kw_pat}).*?)\)")[0]
                m = s.str.extract(rf"((?:{kw_pat})[^\n\r]*?)(?=\s*(?:ถนน|ถ\.|ซอย|ซ\.|หมู่|ม\.|แยก|$))")[0]
                return p.fillna(m).str.strip()
            return get_b(p_ser).fillna(get_b(f_ser)).str.strip()

        def extract_street_with_fallback(p_d, f_d, pat, flags=0):
            def get_s(d): return d.str.extract(pat, flags=flags).bfill(axis=1).iloc[:, 0].str.strip()
            return get_s(p_d).fillna(get_s(f_d))

        # REGISTER
        print("\n" + "="*60)
        print("REGISTER ADDRESS - Extracting fields...")
        print("="*60)
        df["Register_HomeNo"] = extract_with_fallback(df, "Register Addr.1 (Local)", "Register Addr.2 (Local)", home_pattern, re.VERBOSE)
        df["Register_Moo"] = extract_with_fallback(df, "Register Addr.2 (Local)", "Register Addr.1 (Local)", moo_pattern, re.VERBOSE)
        df["Register_Building"] = extract_building_with_fallback(df["Register Addr.2 (Local)"], df["Register Addr.1 (Local)"], keyword_pattern)
        df["Register_Floor"] = extract_with_fallback(df, "Register Addr.2 (Local)", "Register Addr.1 (Local)", floor_pattern)
        df["Register_Soi"] = extract_with_fallback(df, "Register Addr.2 (Local)", "Register Addr.1 (Local)", soi_pattern)
        df["Register_Street"] = extract_street_with_fallback(df["Register Addr.2 (Local)"], df["Register Addr.1 (Local)"], street_pattern, re.VERBOSE)
        print("Register columns created ✓")

        # PERMANENT
        print("\n" + "="*60)
        print("PERMANENT ADDRESS - Extracting fields...")
        print("="*60)
        df["Permanent_HomeNo"] = extract_with_fallback(df, "Permanent Addr.1 (Local)", "Permanent Addr.2 (Local)", home_pattern, re.VERBOSE)
        df["Permanent_Moo"] = extract_with_fallback(df, "Permanent Addr.2 (Local)", "Permanent Addr.1 (Local)", moo_pattern, re.VERBOSE)
        df["Permanent_Building"] = extract_building_with_fallback(df["Permanent Addr.2 (Local)"], df["Permanent Addr.1 (Local)"], keyword_pattern)
        df["Permanent_Floor"] = extract_with_fallback(df, "Permanent Addr.2 (Local)", "Permanent Addr.1 (Local)", floor_pattern)
        df["Permanent_Soi"] = extract_with_fallback(df, "Permanent Addr.2 (Local)", "Permanent Addr.1 (Local)", soi_pattern)
        df["Permanent_Street"] = extract_street_with_fallback(df["Permanent Addr.2 (Local)"], df["Permanent Addr.1 (Local)"], street_pattern, re.VERBOSE)
        print("Permanent columns created ✓")

        # Summary Table (12 columns)
        results = []
        fields = [("Register_HomeNo", "Reg HomeNo"), ("Register_Moo", "Reg Moo"), ("Register_Building", "Reg Building"), 
                  ("Register_Floor", "Reg Floor"), ("Register_Soi", "Reg Soi"), ("Register_Street", "Reg Street"),
                  ("Permanent_HomeNo", "Perm HomeNo"), ("Permanent_Moo", "Perm Moo"), ("Permanent_Building", "Perm Building"),
                  ("Permanent_Floor", "Perm Floor"), ("Permanent_Soi", "Perm Soi"), ("Permanent_Street", "Perm Street")]
        
        for col, label in fields:
            results.append({"Column": label, "Total": len(df), "Filled": df[col].notna().sum()})

        print("\n" + "="*60)
        print("📊 OVERALL QUALITY SUMMARY")
        print("="*60)
        print(pd.DataFrame(results)[["Column", "Total", "Filled"]].to_string(index=False))
        print("="*60)

        self.finished.emit(df)

    def run_template_conversion(self, df):
        print("\n" + "="*60)
        print("🔄 CONVERTING TO TEMPLATE (PA-05)...")
        print("="*60)
        
        def create_segment(source_df, addr_type, prefix):
            seg = pd.DataFrame()
            seg['EmployeeId'] = source_df.get('รหัสพนักงาน', '')
            p_th = source_df.get('คำนำหน้านาม (ไทย)', '').fillna('')
            f_th = source_df.get('ชื่อ (ไทย)', '').fillna('')
            l_th = source_df.get('นามสกุล (ไทย)', '').fillna('')
            seg['FullName'] = (p_th + f_th + " " + l_th).str.strip()
            seg['AddressTypeCode'] = addr_type
            seg['HomeNo'] = source_df.get(f'{prefix}_HomeNo', '')
            seg['Moo'] = source_df.get(f'{prefix}_Moo', '')
            seg['Building'] = source_df.get(f'{prefix}_Building', '')
            seg['Floor'] = source_df.get(f'{prefix}_Floor', '')
            seg['Soi'] = source_df.get(f'{prefix}_Soi', '')
            seg['Street'] = source_df.get(f'{prefix}_Street', '')
            seg['SubdistrictCode'] = source_df.get(f'{prefix} District (คำอธิบาย)', '')
            seg['DistrictCode'] = source_df.get(f'{prefix} City (คำอธิบาย)', '')
            seg['ProvinceCode'] = source_df.get(f'{prefix} Province (คำอธิบาย)', '')
            seg['CountryCode'] = "ไทย"
            seg['PostCode'] = source_df.get(f'{prefix} Postal Code', '')
            seg['BeginDate'] = "01-08-1997"
            seg['EndDate'] = "31-12-9999"
            return seg

        df_reg = create_segment(df, "ที่อยู่ตามบัตรประชาชน", "Register")
        df_perm = create_segment(df, "ที่อยู่ปัจจุบัน", "Permanent")
        df_final = pd.concat([df_reg, df_perm], ignore_index=True)
        
        if 'EmployeeId' in df_final.columns:
            df_final = df_final.sort_values(by=['EmployeeId', 'AddressTypeCode']).reset_index(drop=True)
        
        df_final = df_final.replace([np.nan, 'nan', 'None'], '')

        # 4-Row Header
        header_data = [
            ['Number(8)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Text(max)', 'Date(dd-mm-yyyy)', 'Date(dd-mm-yyyy)'],
            ['int', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'nvarchar', 'datetime2', 'datetime2'],
            ['รหัสพนักงาน', 'ชื่อเต็มพนักงาน', 'ประเภทที่อยู่', 'เลขที่', 'หมู่', 'อาคาร', 'ชั้น', 'ซอย', 'ถนน', 'แขวง/ตำบล', 'เขต/อำเภอ', 'จังหวัด', 'ประเทศ', 'รหัสไปรษณีย์', 'วันที่เริ่มต้น', 'วันที่สิ้นสุด'],
            ['EmployeeId', 'FullName', 'AddressTypeCode', 'HomeNo', 'Moo', 'Building', 'Floor', 'Soi', 'Street', 'SubdistrictCode', 'DistrictCode', 'ProvinceCode', 'CountryCode', 'PostCode', 'BeginDate', 'EndDate']
        ]
        
        df_headers = pd.DataFrame(header_data, columns=df_final.columns)
        df_output = pd.concat([df_headers, df_final], ignore_index=True)
        
        print(f"Template mapping complete: {len(df_final)} data rows generated.")
        print("Custom 4-row headers added ✓")
        self.finished.emit(df_output)

# =============================================================================
# Main Application UI
# =============================================================================
class App(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Thai Address Extractor & Template Converter")
        self.resize(1000, 800)
        self.input_path = ""
        self.processed_df = None
        self.init_ui()
        sys.stdout = OutputStream()
        sys.stdout.text_written.connect(self.update_console)

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(10)

        self.setStyleSheet("""
            QMainWindow { background-color: #1e1e1e; }
            QLabel { color: #e0e0e0; font-size: 13px; font-family: 'Segoe UI', sans-serif; }
            QPushButton { 
                background-color: #333333; color: white; border: 1px solid #555555; 
                padding: 10px; border-radius: 5px; font-weight: bold; font-size: 12px;
            }
            QPushButton:hover { background-color: #444444; border-color: #00FF00; }
            QPushButton#primaryBtn { background-color: #004d00; border-color: #00FF00; }
            QPushButton#secondaryBtn { background-color: #004080; border-color: #0080FF; }
            QPushButton:disabled { background-color: #222222; color: #666666; border-color: #333333; }
            QProgressBar { border: 1px solid #444; border-radius: 4px; background: #222; height: 10px; }
            QProgressBar::chunk { background-color: #00FF00; }
        """)

        # 1. File Selection (Top)
        file_group = QHBoxLayout()
        self.lbl_file = QLabel("Please select an Excel file to begin...")
        btn_select = QPushButton("📁 Browse File")
        btn_select.setFixedWidth(150)
        btn_select.clicked.connect(self.select_file)
        file_group.addWidget(self.lbl_file, 1)
        file_group.addWidget(btn_select)
        layout.addLayout(file_group)

        # 2. Action Buttons (Middle - Above Console)
        btn_row = QHBoxLayout()
        self.btn_extract = QPushButton("⚡ Start Extraction")
        self.btn_extract.setObjectName("primaryBtn")
        self.btn_extract.setEnabled(False)
        self.btn_extract.clicked.connect(lambda: self.start_process("extract"))

        self.btn_convert = QPushButton("📋 Convert to Template")
        self.btn_convert.setObjectName("secondaryBtn")
        self.btn_convert.setEnabled(False)
        self.btn_convert.clicked.connect(lambda: self.start_process("convert"))

        self.btn_save = QPushButton("💾 Save/Export Result")
        self.btn_save.setEnabled(False)
        self.btn_save.clicked.connect(self.save_file)

        btn_row.addWidget(self.btn_extract)
        btn_row.addWidget(self.btn_convert)
        btn_row.addWidget(self.btn_save)
        layout.addLayout(btn_row)

        # 3. Progress Bar
        self.progress = QProgressBar()
        self.progress.setTextVisible(False)
        self.progress.hide()
        layout.addWidget(self.progress)

        # 4. Console (Bottom)
        layout.addWidget(QLabel("Matrix Process Console:"))
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setFont(QFont("Consolas", 10))
        p = self.console.palette()
        p.setColor(QPalette.ColorRole.Base, QColor(0, 0, 0))
        p.setColor(QPalette.ColorRole.Text, QColor(0, 255, 0))
        self.console.setPalette(p)
        layout.addWidget(self.console)

    def update_console(self, text):
        cursor = self.console.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        cursor.insertText(text)
        self.console.setTextCursor(cursor)
        self.console.ensureCursorVisible()

    def select_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open Excel", "", "Excel (*.xlsx *.xls)")
        if path:
            self.input_path = path
            self.lbl_file.setText(f"Selected: {os.path.basename(path)}")
            self.btn_extract.setEnabled(True)
            self.btn_convert.setEnabled(True) # Allow convert directly if it's already an extracted file
            self.btn_save.setEnabled(False)
            self.console.clear()
            print(f"[SYSTEM]: File loaded.")

    def start_process(self, mode):
        self.btn_extract.setEnabled(False)
        self.btn_convert.setEnabled(False)
        self.btn_save.setEnabled(False)
        self.progress.show()
        self.progress.setRange(0, 0)
        if mode == "extract": self.console.clear()
        
        self.worker = ProcessorWorker(self.input_path, mode)
        self.worker.finished.connect(self.on_finished)
        self.worker.error.connect(self.on_error)
        self.worker.start()

    def on_finished(self, df):
        self.processed_df = df
        self.progress.hide()
        self.btn_extract.setEnabled(True)
        self.btn_convert.setEnabled(True)
        self.btn_save.setEnabled(True)
        print(f"\n[SUCCESS]: Operation completed.")

    def on_error(self, message):
        self.progress.hide()
        self.btn_extract.setEnabled(True)
        self.btn_convert.setEnabled(True)
        print(f"\n[ERROR]: {message}")

    def save_file(self):
        if self.processed_df is None: return
        name = "PA_Address_Template.xlsx" if len(self.processed_df.columns) == 16 else "PA_Address_Extracted.xlsx"
        path, _ = QFileDialog.getSaveFileName(self, "Save File", name, "Excel (*.xlsx)")
        if path:
            try:
                # If it's a template (16 columns and specific header), use header=False
                is_template = len(self.processed_df.columns) == 16 and "EmployeeId" in self.processed_df.iloc[3].values
                self.processed_df.to_excel(path, index=False, header=not is_template, engine="openpyxl")
                print(f"\n[SYSTEM]: Exported to {path}")
            except Exception as e:
                print(f"\n[ERROR]: Save failed. {str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = App()
    window.show()
    sys.exit(app.exec())