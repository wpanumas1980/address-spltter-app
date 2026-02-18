import sys
import pandas as pd
import os
from PyQt6.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, 
                             QHBoxLayout, QWidget, QFileDialog, QTextEdit, 
                             QLabel, QProgressBar, QTableWidget, QTableWidgetItem, QHeaderView)
from PyQt6.QtCore import Qt, QThread, pyqtSignal

# คลาสสำหรับการประมวลผลแยกข้อมูล (ทำงานใน Thread แยกเพื่อไม่ให้ UI ค้าง)
class ProcessorThread(QThread):
    progress = pyqtSignal(int)
    log = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, input_path, output_path):
        super().__init__()
        self.input_path = input_path
        self.output_path = output_path

    def run(self):
        try:
            self.log.emit("เริ่มต้นการอ่านไฟล์ Excel...")
            # ใช้ engine='openpyxl' เพื่อรองรับไฟล์ .xlsx ได้อย่างเสถียร
            df = pd.read_excel(self.input_path, engine='openpyxl')
            self.progress.emit(20)

            self.log.emit("กำลังประมวลผลแยกคอลัมน์ Register Addr.1...")
            # แยก Column ตาม Logic ใน Notebook
            if 'Register Addr.1 (Local)' in df.columns:
                df_split1 = df['Register Addr.1 (Local)'].astype(str).str.split(expand=True)
                df_split1.columns = [f'Addr1_Part_{i+1}' for i in df_split1.columns]
                self.progress.emit(40)
            else:
                self.log.emit("⚠️ ไม่พบคอลัมน์ 'Register Addr.1 (Local)'")
                df_split1 = pd.DataFrame()

            self.log.emit("กำลังประมวลผลแยกคอลัมน์ Register Addr.2...")
            if 'Register Addr.2 (Local)' in df.columns:
                df_split2 = df['Register Addr.2 (Local)'].astype(str).str.split(expand=True)
                df_split2.columns = [f'Addr2_Part_{i+1}' for i in df_split2.columns]
                self.progress.emit(60)
            else:
                self.log.emit("⚠️ ไม่พบคอลัมน์ 'Register Addr.2 (Local)'")
                df_split2 = pd.DataFrame()

            self.log.emit("กำลังรวมข้อมูลและเตรียมบันทึก...")
            # รวม DataFrame
            df_final = pd.concat([df, df_split1, df_split2], axis=1)
            self.progress.emit(80)

            self.log.emit(f"กำลังบันทึกไฟล์ไปที่: {self.output_path}")
            df_final.to_excel(self.output_path, index=False, engine='openpyxl')
            
            self.progress.emit(100)
            self.finished.emit()
        except Exception as e:
            self.log.emit(f"❌ เกิดข้อผิดพลาด: {str(e)}")

class ExcelSplitterUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.file_path = ""

    def initUI(self):
        self.setWindowTitle("Excel Address Splitter")
        self.setMinimumSize(900, 700)

        # Main Layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # ส่วนหัวข้อ
        self.label = QLabel("เลือกไฟล์ Excel เพื่อเริ่มต้น")
        self.label.setStyleSheet("font-size: 16px; font-weight: bold; color: #2c3e50;")
        main_layout.addWidget(self.label)

        # ส่วนเลือกไฟล์และปุ่มประมวลผล
        button_layout = QHBoxLayout()
        self.btn_open = QPushButton("📂 เลือกไฟล์ Excel")
        self.btn_open.setFixedHeight(40)
        self.btn_open.clicked.connect(self.open_file)
        button_layout.addWidget(self.btn_open)
        
        self.btn_run = QPushButton("🚀 ประมวลผลและบันทึกไฟล์...")
        self.btn_run.setFixedHeight(40)
        self.btn_run.setEnabled(False)
        self.btn_run.clicked.connect(self.process_file)
        self.btn_run.setStyleSheet("background-color: #2ecc71; color: white; font-weight: bold;")
        button_layout.addWidget(self.btn_run)
        main_layout.addLayout(button_layout)

        # ตาราง Preview ข้อมูล
        main_layout.addWidget(QLabel("ตัวอย่างข้อมูลจากไฟล์ (5 แถวแรก):"))
        self.table = QTableWidget()
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        main_layout.addWidget(self.table)

        # หน้าจอ Log
        main_layout.addWidget(QLabel("ขั้นตอนการทำงานของโปรแกรม:"))
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setStyleSheet("""
            background-color: #1e1e1e; 
            color: #d4d4d4; 
            font-family: 'Consolas', monospace;
            font-size: 13px;
            padding: 10px;
        """)
        main_layout.addWidget(self.log_output)

        # Progress Bar
        self.pbar = QProgressBar()
        self.pbar.setStyleSheet("""
            QProgressBar { border: 1px solid grey; border-radius: 5px; text-align: center; }
            QProgressBar::chunk { background-color: #3498db; }
        """)
        main_layout.addWidget(self.pbar)

    def open_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open Excel File", "", "Excel Files (*.xlsx *.xls)")
        if path:
            self.file_path = path
            self.label.setText(f"ไฟล์ที่เลือก: {os.path.basename(path)}")
            self.log_output.clear()
            self.log_output.append(f"✅ โหลดไฟล์: {path}")
            self.btn_run.setEnabled(True)
            self.preview_data(path)

    def preview_data(self, path):
        try:
            # อ่านตัวอย่าง 5 แถวเพื่อ Preview
            df = pd.read_excel(path, engine='openpyxl').head(5)
            self.table.setRowCount(df.shape[0])
            self.table.setColumnCount(df.shape[1])
            self.table.setHorizontalHeaderLabels(df.columns)
            
            for i in range(df.shape[0]):
                for j in range(df.shape[1]):
                    val = str(df.iloc[i, j]) if pd.notnull(df.iloc[i, j]) else ""
                    self.table.setItem(i, j, QTableWidgetItem(val))
            
            # ปรับขนาดคอลัมน์ให้พอดี
            self.table.resizeColumnsToContents()
            self.log_output.append("> แสดงตัวอย่างข้อมูลเรียบร้อย")
        except Exception as e:
            self.log_output.append(f"❌ ไม่สามารถ Preview ข้อมูลได้: {str(e)}")

    def process_file(self):
        # 1. ให้ผู้ใช้เลือกที่จัดเก็บไฟล์ก่อนเริ่มทำงาน
        default_name = os.path.basename(self.file_path).replace(".xlsx", "_Split.xlsx")
        save_path, _ = QFileDialog.getSaveFileName(
            self, "บันทึกไฟล์เป็น...", default_name, "Excel Files (*.xlsx)"
        )

        # 2. ถ้าผู้ใช้กด Cancel ให้จบการทำงาน
        if not save_path:
            return

        # เตรียม UI
        self.btn_run.setEnabled(False)
        self.btn_open.setEnabled(False)
        self.pbar.setValue(0)
        self.log_output.append(f"--- เริ่มต้นการประมวลผล ---")
        
        # 3. สร้าง Thread และเริ่มทำงาน
        self.thread = ProcessorThread(self.file_path, save_path)
        self.thread.log.connect(lambda msg: self.log_output.append(f" {msg}"))
        self.thread.progress.connect(self.pbar.setValue)
        self.thread.finished.connect(self.on_finished)
        self.thread.start()

    def on_finished(self):
        self.log_output.append(f"\n✨ เสร็จสิ้น! ข้อมูลถูกบันทึกเรียบร้อยแล้ว")
        self.btn_run.setEnabled(True)
        self.btn_open.setEnabled(True)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    # ตั้งค่า Style พื้นฐานให้ดูทันสมัยขึ้น
    app.setStyle("Fusion")
    window = ExcelSplitterUI()
    window.show()
    sys.exit(app.exec())