import shutil
import os
from datetime import datetime

class SharePointDrop:
    def __init__(self, input_path, filename, sharepoint_path, sharepoint_filepath, log_file):
        self.input_filepath = os.path.join(input_path, filename)
        self.output_filepath = os.path.join(sharepoint_path, sharepoint_filepath)
        self.log_file = log_file

    def log(self, message):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.log_file, 'a') as logf:
            logf.write(f"[{timestamp}] {message}\n")

    def copy_file(self):
        try:
            shutil.move(self.input_filepath, self.output_filepath)
            self.log(f"SUCCESS: Moved from {self.input_filepath} to {self.output_filepath}")
        except FileNotFoundError:
            self.log(f"ERROR: File not found - {self.input_filepath}")
        except Exception as e:
            self.log(f"ERROR: Failed to move {self.input_filepath} to {self.output_filepath} - {repr(e)}")

if __name__ == "__main__":
    input_path = r'C:\Users\Thomas.Mohlapo\OneDrive - 9475042 - Tshela Health Care\Desktop\Data Exchanges ETL\auto_data_exchanges\outputs'
    sharepoint_path = r'C:\Users\Thomas.Mohlapo\OneDrive - 9475042 - Tshela Health Care\Information Technology - Documents\Data Exchange\Data Exchanges (backup)'
    timestamp = datetime.now().strftime('%Y%m%d')
    log_file = 'log.txt'

    file_mappings = [
        (f'GEMS-HIV\\HIVMATREG_DAILY{timestamp}.txt', f'GEMS\\Daily\\HIV\\HIVMATREG_DAILY{timestamp}.txt'),
        (f'High Risk\\HIGH_RISK{timestamp}.txt', f'GEMS\\Weekly\\MHRS\\HIGH_RISK{timestamp}.txt'),
        (f'Medikredit\\GMMA{timestamp}.txt', f'GEMS\\Daily\\Medikredit\\GMMA{timestamp}.txt'),
        (f'PMB\\GEMSMAT{timestamp}.txt', f'GEMS\\Daily\\MHG\\GEMSMAT{timestamp}.txt'),
        (f'SAMWUMED-HIV\\HIVMATREG_DAILY{timestamp}.txt', f'Samwumed\\Outbound\\Daily\\HIVMATREG_DAILY{timestamp}.txt')
    ]

    for filename, sharepoint_file in file_mappings:
        drop = SharePointDrop(input_path, filename, sharepoint_path, sharepoint_file, log_file)
        drop.copy_file()