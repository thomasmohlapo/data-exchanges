from sqlalchemy import create_engine
import pandas as pd
import os
from urllib.parse import quote_plus
from datetime import datetime, timedelta
import win32com.client as win32
import paramiko
import warnings

warnings.filterwarnings("ignore")

class GEMS_DataExtract:
    def __init__(self):
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.database = os.getenv("DB_NAME2")
        self.df = None

    def get_engine(self):
        return create_engine(
            f"mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
        )
    def load_extract(self):
        try:
            # Read SQL Query
            query = '''SELECT CAST(`tmc-live`.files.FileOpenDate AS DATE) AS 'Registration Date'
    ,CAST(`tmc-live`.files.created AS DATE) AS 'Created Date'
	,`tmc-live`.files.ReferenceNumber AS 'File Number'
    ,LTRIM(RTRIM(`tmc-live`.members.FirstName)) AS 'Name'
	,LTRIM(RTRIM(`tmc-live`.memberdetails.Surname)) AS 'Surname'
    ,LTRIM(RTRIM(`tmc-live`.addresses.Postal)) AS 'Address 1'
    ,LTRIM(RTRIM(`tmc-live`.addresses.Physical)) AS 'Address 2'
    ,LTRIM(RTRIM(`tmc-live`.addresses.PostalCode)) AS 'Postal Code'
    ,LTRIM(RTRIM(`tmc-live`.membermedicalaids.SchemeNumber)) AS 'Membership Number'
    ,LTRIM(RTRIM(`tmc-live`.members.DependentCode)) AS 'Dependant Code'
    ,'Gems' AS 'Organization'
    ,CASE `tmc-live`.tenantschemeoptionsconfigs.name
				       WHEN 'Sapphire' THEN 'Sapphire'
					   WHEN 'Beryl' THEN 'Beryl'
					   WHEN 'Ruby' THEN 'Ruby'
					   WHEN 'Emerald' THEN 'Emerald'
                       WHEN 'Emerald ' THEN 'Emerald'
					   WHEN 'Onyx' THEN 'Onyx'
					   WHEN 'Emerald Value Option ' THEN 'Emerald Value Option'
                       WHEN 'Emerald Value Option' THEN 'Emerald Value Option'
                       WHEN 'Emerald Value' THEN 'Emerald Value'
					   WHEN 'Tanzanite One' THEN 'Tanzanite One' 
                       WHEN 'Tanzanite 1' THEN 'Tanzanite One' 
                       END AS 'Option'
    ,CASE WHEN `tmc-live`.members.IdNumber IS NOT NULL THEN `tmc-live`.members.IdNumber
          ELSE `tmc-live`.members.Passport
          END AS 'ID Number'
    ,CAST(`tmc-live`.files.EDD AS DATE) 'EDD'
    ,CAST(CURDATE() AS DATE) AS 'Today''s Date'
    ,LTRIM(RTRIM(`tmc-live`.memberdetails.ContactNumber)) 'Cell No'
    ,ROUND(40-(DATEDIFF(`tmc-live`.files.EDD, CURDATE())/7)) AS 'Week'
    ,CASE WHEN ROUND(40-(DATEDIFF(`tmc-live`.files.EDD, CURDATE())/7)) < 13 THEN 1 
          WHEN ROUND(40-(DATEDIFF(`tmc-live`.files.EDD, CURDATE())/7)) >12 AND ROUND(40-(DATEDIFF(`tmc-live`.files.EDD, CURDATE())/7)) <27 THEN 2
          WHEN ROUND(40-(DATEDIFF(`tmc-live`.files.EDD, CURDATE())/7)) >26 THEN 3
          END AS 'Trimester'
    ,`tmc-live`.memberdetails.Language AS 'Language'
    ,LTRIM(RTRIM(`tmc-live`.memberdetails.email)) 'Email Address'
    ,CAST(`tmc-live`.files.FileTerminationDate AS DATE) AS 'Date Closed'
    ,`tmc-live`.files.FileStatus AS 'File Status'

FROM `tmc-live`.files
JOIN `tmc-live`.members
ON `tmc-live`.files.MemberId = `tmc-live`.members.Id
JOIN `tmc-live`.memberdetails
ON `tmc-live`.files.MemberDetailId = `tmc-live`.memberdetails.Id
JOIN `tmc-live`.membermedicalaids
ON `tmc-live`.members.MemberMedicalAidId = `tmc-live`.membermedicalaids.Id
JOIN `tmc-live`.tenantschemeoptionsconfigs
  ON `tmc-live`.membermedicalaids.SchemeOption = `tmc-live`.tenantschemeoptionsconfigs.Id
JOIN `tmc-live`.tenants
 ON `tmc-live`.files.TenantId = `tmc-live`.tenants.Id
JOIN `tmc-live`.addresses
ON `tmc-live`.memberdetails.AddressId = `tmc-live`.addresses.Id

WHERE `tmc-live`.files.FileOpenDate <> '0001-01-01'
  AND`tmc-live`.files.FileType = 'Matreg'
  AND `tmc-live`.tenants.name = 'Tshela'
  AND `tmc-live`.files.Deleted = 0 
'''
    
            self.df = pd.read_sql(query, self.get_engine())
            
            return self.df
            
        except Exception as e:
            print(f"Error: {e}")
            return False

class PMB_DataExtract:
    def __init__(self):
        self.output_dir = r'C:\Users\Thomas.Mohlapo\OneDrive - 9475042 - Tshela Health Care\Documents\Data Exchange tests\bat-deploy\outputs\PMB'
        self.timestamp = datetime.now().strftime('%Y%m%d')
        self.output_filename = f'GEMSMAT{self.timestamp}.txt'
        self.output_path = os.path.join(self.output_dir, self.output_filename)
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.database = os.getenv("DB_NAME1")
        self.df = None

    def get_engine(self):
        return create_engine(
            f"mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
        )

    def load_extract(self):
        try:
            # Read SQL Query
            query = '''SELECT * FROM th_dw.daily_ex_registrations;'''
            self.df = pd.read_sql(query, self.get_engine())

            return self.df
            
        except Exception as e:
            print(f"Error: {e}")
            return False
        
    def save_extract(self):
        try:
            df = self.load_extract()
            
            self.pmb_extract = df.to_csv(self.output_path, index=False, sep =',')
            return self.pmb_extract
        except Exception as e:
            print(f"Error: {e}")
            return False
            
class PMB_SFTP(GEMS_DataExtract, PMB_DataExtract):
    def __init__(self):
        GEMS_DataExtract.__init__(self)
        PMB_DataExtract.__init__(self)
        self.gems_rows = None
        self.pmb_rows = None
        self.error_message = None
        self.remote_path = None
        # SFTP server credentials (store in .env in real projects)
        self.sftp_hostname = os.getenv('PMB_SERVER')
        self.sftp_port = 22
        self.sftp_username = os.getenv('PMB_USERNAME')
        self.sftp_password = os.getenv("PMB_PASSWORD")

    def gems_wrangle(self):
        self.df = GEMS_DataExtract.load_extract(self)
        self.df = self.df.loc[self.df['File Status'].isin(['Approved', 'Closed - Miscarriage'])].copy()
        self.df['Registration Date'] = pd.to_datetime(self.df['Registration Date'])
        now = datetime.now()
        day_of_week_int = now.weekday()
        
        if day_of_week_int == 0:
            yesterday = now.date() - timedelta(days=2)
            day_before = now.date() - timedelta(days=3)
            df_recent = self.df[self.df['Registration Date'].dt.date.isin([yesterday, day_before])]
        else:
            yesterday = now.date() - timedelta(days=1)
            df_recent = self.df[self.df['Registration Date'].dt.date.isin([yesterday])]
    
        self.gems_rows = len(df_recent)
        return self.gems_rows
    
    def pmb_wrangle(self):
        self.df = PMB_DataExtract.load_extract(self)

        self.df['START DATE'] = pd.to_datetime(self.df['START DATE'], errors='coerce')
        now = datetime.now()
        day_of_week_int = now.weekday()
        
        if day_of_week_int == 0:
            yesterday = now.date() - timedelta(days=2)
            day_before = now.date() - timedelta(days=3)
            df_recent = self.df[self.df['START DATE'].dt.date.isin([yesterday, day_before])]
        else:
            yesterday = now.date() - timedelta(days=1)
            df_recent = self.df[self.df['START DATE'].dt.date.isin([yesterday])]
        
        self.pmb_rows = len(df_recent)
        return self.pmb_rows

    def send_email(self):
        self.gems_wrangle()
        self.pmb_wrangle()
        timestamp = datetime.now().strftime('%Y%m%d')
        if self.gems_rows == self.pmb_rows:
            self.upload_to_sftp()
            olApp = win32.Dispatch('Outlook.Application')
            olNS = olApp.GetNameSpace('MAPI')

            mailItem = olApp.CreateItem(0)
            mailItem.Subject = f'Data Matching Report {timestamp}: PMB vs GEMS Registration (TEST)'
            mailItem.BodyFormat = 2
            # HTML email body
            html_body = f"""
            <html>
            <head>
            <style>
                body {{ font-family: Calibri, Arial, sans-serif; line-height: 1.6; }}
                .header {{ color: #2e6c80; font-size: 18px; font-weight: bold; }}
                .data {{ background-color: #f2f2f2; padding: 10px; border-radius: 5px; }}
                .footer {{ font-size: 12px; color: #666; margin-top: 20px; }}
                .signature {{ margin-top: 30px; }}
            </style>
            </head>
            <body>
                <p class="header">Data Matching Report</p>
                
                <p>Dear Thomas,</p>
                
                <p>Please find below the results of the latest data comparison between PMB and GEMS Registration systems:</p>
                
                <div class="data">
                    <strong>Record Counts:</strong><br>
                    • PMB File: {self.pmb_rows} records<br>
                    • GEMS Registration: {self.gems_rows} records
                </div>
                
                <p>The files {"<strong style='color:green'>are matching</strong>" if self.pmb_rows == self.gems_rows else "<strong style='color:red'>show discrepancies</strong>"} in the record counts.</p>
            
                <p>{self.output_filename} Extract has been uploaded to the sftp site: '{self.remote_path}'</p>
                
                <div class="signature">
                    Regards,<br>
                    Thomas<br>
                    Data Intern<br>
                    It Department<br>
                    <img src="image.jpg" width="150">
                </div>
                
                <div class="footer">
                    <hr>
                    <p>This is an automated message. Please do not reply directly to this email.</p>
                </div>
            </body>
            </html>
            """
            mailItem.HTMLBody = html_body
            mailItem.To = 'Thomas.Mohlapo@tshela.co.za'
            mailItem.CC = 'itsupport@tshela.co.za;peter.maila@tshela.co.za;mulalo.ndou@tshela.co.za'
            mailItem.Sensitivity = 2

            mailItem.Send()
        else:
            olApp = win32.Dispatch('Outlook.Application')
            olNS = olApp.GetNameSpace('MAPI')

            mailItem = olApp.CreateItem(0)
            mailItem.Subject = f'Data Matching Report {timestamp}: PMB vs GEMS Registration (TEST)'
            mailItem.BodyFormat = 2
            # HTML email body
            html_body = f"""
            <html>
            <head>
            <style>
                body {{ font-family: Calibri, Arial, sans-serif; line-height: 1.6; }}
                .header {{ color: #2e6c80; font-size: 18px; font-weight: bold; }}
                .data {{ background-color: #f2f2f2; padding: 10px; border-radius: 5px; }}
                .footer {{ font-size: 12px; color: #666; margin-top: 20px; }}
                .signature {{ margin-top: 30px; }}
            </style>
            </head>
            <body>
                <p class="header">Data Matching Report</p>
                
                <p>Dear Thomas,</p>
                
                <p>Please find below the results of the latest data comparison between PMB and GEMS Registration systems:</p>
                
                <div class="data">
                    <strong>Record Counts:</strong><br>
                    • PMB File: {self.pmb_rows} records<br>
                    • GEMS Registration: {self.gems_rows} records
                </div>
                
                <p>The systems {"<strong style='color:green'>are matching</strong>" if self.pmb_rows == self.gems_rows else "<strong style='color:red'>show discrepancies</strong>"} in their record counts.</p>
                
                <div class="signature">
                    Regards,<br>
                    Thomas<br>
                    Data Intern<br>
                    IT Department<br>
                    <img src="image.jpg" width="150">
                </div>
            </body>
            </html>
            """
            mailItem.HTMLBody = html_body
            mailItem.To = 'Thomas.Mohlapo@tshela.co.za'
            mailItem.CC = 'itsupport@tshela.co.za;peter.maila@tshela.co.za;mulalo.ndou@tshela.co.za'
            mailItem.Sensitivity = 2

            mailItem.Send()

    def email_err(self, output_filename, remote_path, error_message):
        olApp = win32.Dispatch('Outlook.Application')
        olNS = olApp.GetNameSpace('MAPI')

        # Construct email item object with HTML formatting
        mailItem = olApp.CreateItem(0)
        mailItem.Subject = f'File failed to upload to SFTP:'
        mailItem.BodyFormat = 2  # 2 = HTML format
        # HTML email body
        html_body = f"""<html>
                    <head>
                    <style>
                        body {{ font-family: Calibri, Arial, sans-serif; line-height: 1.6; }}
                        .header {{ color: #2e6c80; font-size: 18px; font-weight: bold; }}
                        .data {{ background-color: #f2f2f2; padding: 10px; border-radius: 5px; }}
                        .footer {{ font-size: 12px; color: #666; margin-top: 20px; }}
                        .signature {{ margin-top: 30px; }}
                    </style>
                    </head>
                    <body>
                        <p class="header">{self.output_filename}</p>
                        
                        <p>Dear Thomas,</p>
                        
                        <b>Upload failed at:</b><br>
                        <b>File:</b> {self.output_filename}<br>
                        <b>Error Message:</b> <pre>{self.error_message}</pre>
                        
                        <div class="signature">
                            Regards,<br>
                            Thomas<br>
                            Data Intern<br>
                            IT Department<br>
                            <img src="image.jpg"  width="150">
                        </div>
                    </body>
                    </html>
                """
        mailItem.HTMLBody = html_body
        mailItem.To = 'Thomas.Mohlapo@tshela.co.za'
        mailItem.CC = 'itsupport@tshela.co.za;peter.maila@tshela.co.za;mulalo.ndou@tshela.co.za'
        mailItem.Sensitivity = 2

        # Attachments (if needed)
        # mailItem.Attachments.Add('[File Path]')

        mailItem.Send()
    
    def upload_to_sftp(self, remote_dir="/IN"):
        try:
            # Start SFTP session
            transport = paramiko.Transport((self.sftp_hostname, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            self.remote_path = f"{remote_dir}/{self.output_filename}"

            # Upload file
            sftp.put(self.output_path, self.remote_path)

            # Close connection
            sftp.close()
            transport.close()

        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)

class Send_Email():
    def __init__(self):
        self.output_filename = None
        self.remote_path = None
        self.error_message = None
        self.header = None
        self.site = None

    def email(self, output_filename, remote_path):
        olApp = win32.Dispatch('Outlook.Application')
        olNS = olApp.GetNameSpace('MAPI')

        mailItem = olApp.CreateItem(0)
        mailItem.Subject = f'File uploaded successfully to SFTP'
        mailItem.BodyFormat = 2
        # HTML email body
        html_body = f"""
            <html>
            <head>
            <style>
                body {{ font-family: Calibri, Arial, sans-serif; line-height: 1.6; }}
                .header {{ color: #2e6c80; font-size: 18px; font-weight: bold; }}
                .data {{ background-color: #f2f2f2; padding: 10px; border-radius: 5px; }}
                .footer {{ font-size: 12px; color: #666; margin-top: 20px; }}
                .signature {{ margin-top: 30px; }}
            </style>
            </head>
            <body>
                <p class="header">{self.header}</p>
                
                <p>Dear Thomas,</p>
                
                <p>{self.output_filename} Extract has been uploaded to the sftp site: '{self.site}/{self.remote_path}'</p>
                
                <div class="signature">
                    Regards,<br>
                    Thomas<br>
                    Data Intern<br>
                    IT Department<br>
                    <img src="image.jpg" width="150">
                </div>
            </body>
            </html>
            """
        mailItem.HTMLBody = html_body
        mailItem.To = 'Thomas.Mohlapo@tshela.co.za'
        mailItem.CC = 'itsupport@tshela.co.za;peter.maila@tshela.co.za;mulalo.ndou@tshela.co.za'
        mailItem.Sensitivity = 2

        mailItem.Send()

    def email_err(self, output_filename, remote_path, error_message):
        olApp = win32.Dispatch('Outlook.Application')
        olNS = olApp.GetNameSpace('MAPI')

        mailItem = olApp.CreateItem(0)
        mailItem.Subject = f'File failed to upload to SFTP:'
        mailItem.BodyFormat = 2
        # HTML email body
        html_body = f"""<html>
                <head>
                <style>
                    body {{ font-family: Calibri, Arial, sans-serif; line-height: 1.6; }}
                    .header {{ color: #2e6c80; font-size: 18px; font-weight: bold; }}
                    .data {{ background-color: #f2f2f2; padding: 10px; border-radius: 5px; }}
                    .footer {{ font-size: 12px; color: #666; margin-top: 20px; }}
                    .signature {{ margin-top: 30px; }}
                </style>
                </head>
                <body>
                    <p class="header">{self.header}</p>
                    
                    <p>Dear Thomas,</p>
                    
                    <b>Upload failed at:</b><br>
                    <b>File:</b> {self.output_filename}<br>
                    <b>Error Message:</b> <pre>{self.error_message}</pre>
                    
                    <div class="signature">
                        Regards,<br>
                        Thomas<br>
                        Data Intern<br>
                        IT Department<br>
                        <img src="image.jpg" width="150">
                    </div>
                </body>
                </html>
            """
        mailItem.HTMLBody = html_body
        mailItem.To = 'Thomas.Mohlapo@tshela.co.za'
        mailItem.CC = 'itsupport@tshela.co.za;peter.maila@tshela.co.za;mulalo.ndou@tshela.co.za'
        mailItem.Sensitivity = 2

        mailItem.Send()

class MEDIKRED_DataExtract(Send_Email):
    def __init__(self):
        Send_Email.__init__(self)
        self.output_dir = r'C:\Users\Thomas.Mohlapo\OneDrive - 9475042 - Tshela Health Care\Documents\Data Exchange tests\bat-deploy\outputs\Medikredit'
        self.timestamp = datetime.now().strftime('%Y%m%d')
        self.output_filename = f'GMMA{self.timestamp}.txt'
        self.output_path = os.path.join(self.output_dir, self.output_filename)
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.database = os.getenv("DB_NAME1")
        self.header = 'Medikredit'
        self.site = 'Medikredit'
        self.extract = None

    def get_engine(self):
        return create_engine(
            f"mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
        )
    
    def load_extract(self):
        try:
            # Read SQL Script
            query = '''SELECT * FROM th_dw.daily_ex_claims;'''
    
            df = pd.read_sql(query, self.get_engine())
            
            # Convert the file to txt
            self.extract = df.to_csv(self.output_path, index=False, header=False, sep=',')
            return self.extract
            
        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)
        
    def upload_to_sftp(self, remote_dir="/home/GEMS/EUROPEASSISTANCE/incoming/benefits"):
        # Load data and generate the CSV file
        self.load_extract()

        # SFTP server credentials (store in .env in real projects)
        self.sftp_hostname = os.getenv('MED_SERVER')
        self.sftp_port = 22
        self.sftp_username = os.getenv('MED_USERNAME')
        self.sftp_password = os.getenv("MED_PASSWORD")

        try:
            # Start SFTP session
            transport = paramiko.Transport((self.sftp_hostname, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            self.remote_path = f"{remote_dir}/{self.output_filename}"

            # Upload file
            sftp.put(self.output_path, self.remote_path)
            self.email(self.output_filename, self.remote_path)

            # Close connection
            sftp.close()
            transport.close()

        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)

class GHIV_DataExtract(Send_Email):
    def __init__(self):
        Send_Email.__init__(self)
        self.output_dir = r'C:\Users\Thomas.Mohlapo\OneDrive - 9475042 - Tshela Health Care\Documents\Data Exchange tests\bat-deploy\outputs\GEMS-HIV'
        self.timestamp = datetime.now().strftime('%Y%m%d')
        self.output_filename = f'HIVMATREG_DAILY{self.timestamp}.txt'
        self.output_path = os.path.join(self.output_dir, self.output_filename)
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.database = os.getenv("DB_NAME1")
        self.header = 'GEMS HIV'
        self.site = 'GEMS-HIV'
        self.extract = None

    def get_engine(self):
        return create_engine(
            f"mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
        )

    def load_extract(self):
        try:
            # Read SQL Script
            query = '''SELECT * FROM th_dw.daily_ex_hiv;'''
    
            df = pd.read_sql(query, self.get_engine())

            # Convert the file to txt
            self.extract = df.to_csv(self.output_path, index=False, sep='|')
            return self.extract
        
        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)
        
    def upload_to_sftp(self, remote_dir="/IN"):
        # Load data and generate the CSV file
        self.load_extract()

        # SFTP server credentials (store in .env in real projects)
        self.sftp_hostname = os.getenv('GEMS_SERVER')
        self.sftp_port = 22
        self.sftp_username = os.getenv('GEMS_USERNAME')
        self.sftp_password = os.getenv("GEMS_PASSWORD")

        try:
            # Start SFTP session
            transport = paramiko.Transport((self.sftp_hostname, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            self.remote_path = f"{remote_dir}/{self.output_filename}"

            # Upload file
            sftp.put(self.output_path, self.remote_path)
            self.email(self.output_filename, self.remote_path)

            # Close connection
            sftp.close()
            transport.close()

        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)
         
class SAM_DataExtract(Send_Email):
    def __init__(self):
        Send_Email.__init__(self)
        self.output_dir = r'C:\Users\Thomas.Mohlapo\OneDrive - 9475042 - Tshela Health Care\Documents\Data Exchange tests\bat-deploy\outputs\SAMWUMED-HIV'
        self.timestamp = datetime.now().strftime('%Y%m%d')
        self.output_filename = f'HIVMATREG_DAILY{self.timestamp}.txt'
        self.output_path = os.path.join(self.output_dir, self.output_filename)
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.database = os.getenv("DB_NAME1")
        self.header = 'SAMWUMED HIV'
        self.site = 'SAMWUMED-HIV'
        self.extract = None

    def get_engine(self):
        return create_engine(
            f"mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
        )
    def load_extract(self):
        try:
            # Read the SQL Script
            query = '''SELECT * FROM th_dw.daily_ex_hiv_sam;'''
    
            df = pd.read_sql(query, self.get_engine())

            # Convert the file to txt
            self.extract = df.to_csv(self.output_path, index=False, header=True, sep='|')

            #print(f"Successfully converted {self.db} to {self.output_path}")
            return self.extract
            
        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)
        
    def upload_to_sftp(self, remote_dir="/IN"):
        # Load data and generate the CSV file
        self.load_extract()

        # SFTP server credentials (store in .env in real projects)
        self.sftp_hostname = os.getenv('SAM_SERVER')
        self.sftp_port = 22
        self.sftp_username = os.getenv('SAM_USERNAME')
        self.sftp_password = os.getenv("SAM_PASSWORD")

        try:
            # Start SFTP session
            transport = paramiko.Transport((self.sftp_hostname, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            self.remote_path = f"{remote_dir}/{self.output_filename}"

            # Upload file
            sftp.put(self.output_path, self.remote_path)
            self.email(self.output_filename, self.remote_path)

            # Close connection
            sftp.close()
            transport.close()

        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)
        
class HighRisk_DataExtract(Send_Email):
    def __init__(self):
        Send_Email.__init__(self)
        self.output_dir = r'C:\Users\Thomas.Mohlapo\OneDrive - 9475042 - Tshela Health Care\Documents\Data Exchange tests\bat-deploy\outputs\High Risk'
        self.timestamp = datetime.now().strftime('%Y%m%d')
        self.output_filename = f'HIGH_RISK{self.timestamp}.txt'
        self.output_path = os.path.join(self.output_dir, self.output_filename)
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.database = os.getenv("DB_NAME1")
        self.header = 'High Risk'
        self.site = 'High Risk - MHRS'
        self.extract = None

    def get_engine(self):
        return create_engine(
            f"mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
        )
    def load_extract(self):
        try:
            # Read the SQL Script
            query = '''SELECT * FROM th_dw.weekly_ex_high_risk;'''
    
            df = pd.read_sql(query, self.get_engine())

            # Convert the file to txt
            self.extract = df.to_csv(self.output_path, index=False, header=True, sep=',')

            #print(f"Successfully converted {self.db} to {self.output_path}")
            return self.extract
            
        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)
        
    def upload_to_sftp(self, remote_dir="/In"):
        # Load data and generate the CSV file
        self.load_extract()

        # SFTP server credentials (store in .env in real projects)
        self.sftp_hostname = os.getenv('HR_SERVER')
        self.sftp_port = 22
        self.sftp_username = os.getenv('HR_USERNAME')
        self.sftp_password = os.getenv("HR_PASSWORD")

        try:
            # Start SFTP session
            transport = paramiko.Transport((self.sftp_hostname, self.sftp_port))
            transport.connect(username=self.sftp_username, password=self.sftp_password)
            sftp = paramiko.SFTPClient.from_transport(transport)

            self.remote_path = f"{remote_dir}/{self.output_filename}"

            # Upload file
            sftp.put(self.output_path, self.remote_path)
            self.email(self.output_filename, self.remote_path)

            # Close connection
            sftp.close()
            transport.close()

        except Exception as e:
            self.error_message = str(e)
            self.email_err(self.output_filename, self.remote_path, self.error_message)
        
obj_gems = GHIV_DataExtract()
obj_sam  = SAM_DataExtract()
obj_med = MEDIKRED_DataExtract()
obj_hr = HighRisk_DataExtract()
obj = PMB_DataExtract()
obj_pmb = PMB_SFTP()


# This loop will run all the code and pull the extracts from the database
day_of_week_int = datetime.now().weekday()

if day_of_week_int == 0:
    obj.save_extract()
    for x in (obj_gems, obj_med, obj_sam, obj_hr):
        x.upload_to_sftp()
    obj_pmb.send_email()
else:
    obj.save_extract()
    for x in (obj_gems, obj_med, obj_sam):
        x.upload_to_sftp()
    obj_pmb.send_email()

'''print(f'Gems HIV Server: {os.getenv("GEMS_SERVER")}, Username: {os.getenv("GEMS_USERNAME")}, Password: {os.getenv("GEMS_PASSWORD")}')
print(f'SAMWUMED HIV Server: {os.getenv("SAM_SERVER")}, Username: {os.getenv("SAM_USERNAME")}, Password: {os.getenv("SAM_PASSWORD")}')
print(f'Medikredit Server: {os.getenv("MED_SERVER")}, Username: {os.getenv("MED_USERNAME")}, Password: {os.getenv("MED_PASSWORD")}')
print(f'PMB Server: {os.getenv("PMB_SERVER")}, Username: {os.getenv("PMB_USERNAME")}, Password: {os.getenv("PMB_PASSWORD")}')
print(f'High Risk Server: {os.getenv("HR_SERVER")}, Username: {os.getenv("HR_USERNAME")}, Password: {os.getenv("HR_PASSWORD")}')'''