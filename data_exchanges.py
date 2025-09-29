from sqlalchemy import create_engine
import pandas as pd
import os
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import paramiko
import warnings
from urllib.parse import quote_plus
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path) # Load environment variables from .env file

warnings.filterwarnings("ignore")

class GEMS_DataExtract:
    """Class to extract data from the GEMS Registrations from the tmc-live database."""
    def __init__(self):
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.database = os.getenv("DB_NAME2")
        self.df = None

    def get_engine(self):
        # Create a database engine
        return create_engine(
            f"mysql+mysqlconnector://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
        )
    def load_extract(self):
        try:
            # Read SQL Query and convert to DataFrame
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
    """Class to extract data from the PMB Registrations from the th_dw database."""
    def __init__(self):
        self.output_dir = r'outputs\PMB'
        self.timestamp = datetime.now().strftime('%Y%m%d')
        self.output_filename = f'GEMSMAT{self.timestamp}.txt'
        self.output_path = os.path.join(self.output_dir, self.output_filename)
        self.db_host = os.getenv("DB_HOST")
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.database = os.getenv("DB_NAME1")
        self.df = None

    def get_engine(self):
        # Create a database engine
        return create_engine(
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
        )

    def load_extract(self):
        try:
            # Read SQL Query and convert to DataFrame
            query = '''SELECT * FROM th_dw.daily_ex_registrations;'''
            self.df = pd.read_sql(query, self.get_engine())

            return self.df
            
        except Exception as e:
            print(f"Error: {e}")
            return False
        
    def save_extract(self):
        try:
            df = self.load_extract()
            # Convert the file to txt
            self.pmb_extract = df.to_csv(self.output_path, index=False, sep =',')
            return self.pmb_extract
        except Exception as e:
            print(f"Error: {e}")
            return False
            
class PMB_SFTP(GEMS_DataExtract, PMB_DataExtract):
    """Class to compare GEMS and PMB data extracts and send email notifications."""
    def __init__(self):
        GEMS_DataExtract.__init__(self)
        PMB_DataExtract.__init__(self)
        self.gems_rows = None
        self.pmb_rows = None
        self.error_message = None
        self.remote_path = None
        # SFTP server credentials (store in .env in real projects)
        self.sftp_hostname = os.getenv("SFTP_SERVER")
        self.sftp_port = 22
        self.sftp_username = os.getenv("SFTP_USERNAME")
        self.sftp_password = os.getenv("SFTP_PASSWORD")

        # Email configuration
        self.sender_email = os.getenv("SMTP_USER")
        self.receiver_email = os.getenv("SMTP_RECEIVER")
        self.smtp_server = os.getenv("SMTP_SERVER")

    def gems_wrangle(self):
        """Filter GEMS data for recent registrations and count rows."""
        self.df = GEMS_DataExtract.load_extract(self)
        self.df = self.df.loc[self.df['File Status'].isin(['Approved', 'Closed - Miscarriage'])].copy()
        self.df['Registration Date'] = pd.to_datetime(self.df['Registration Date'])
        now = datetime.now()
        day_of_week_int = now.weekday()
        
        if day_of_week_int == 0:
            # If today is Monday, include registrations from Saturday and Sunday
            yesterday = now.date() - timedelta(days=2)
            day_before = now.date() - timedelta(days=3)
            df_recent = self.df[self.df['Registration Date'].dt.date.isin([yesterday, day_before])]
        else:
            # For other days, include only yesterday's registrations
            yesterday = now.date() - timedelta(days=1)
            df_recent = self.df[self.df['Registration Date'].dt.date.isin([yesterday])]
    
        self.gems_rows = len(df_recent)
        return self.gems_rows
    
    def pmb_wrangle(self):
        """Filter PMB data for recent registrations and count rows."""
        self.df = PMB_DataExtract.load_extract(self)
        self.df['START DATE'] = pd.to_datetime(self.df['START DATE'], errors='coerce')
        now = datetime.now()
        day_of_week_int = now.weekday()
        
        if day_of_week_int == 0:
            # If today is Monday, include registrations from Saturday and Sunday
            yesterday = now.date() - timedelta(days=2)
            day_before = now.date() - timedelta(days=3)
            df_recent = self.df[self.df['START DATE'].dt.date.isin([yesterday, day_before])]
        else:
            # For other days, include only yesterday's registrations
            yesterday = now.date() - timedelta(days=1)
            df_recent = self.df[self.df['START DATE'].dt.date.isin([yesterday])]
        
        self.pmb_rows = len(df_recent)
        return self.pmb_rows

    def send_email(self):
        """Compare GEMS and PMB data and send email notification."""
        self.gems_wrangle()
        self.pmb_wrangle()
        timestamp = datetime.now().strftime('%Y%m%d')
        if self.gems_rows == self.pmb_rows:
            # If the row counts match, upload to SFTP and send success email
            self.upload_to_sftp()
            msg = MIMEMultipart()
            msg["From"] = self.sender_email
            msg["To"] = self.receiver_email
            msg["Subject"] = f'Data Matching Report {timestamp}: PMB vs GEMS Registration'

            body = f"{self.output_filename} successfully uploaded to the SFTP Server.\nGEMS Rows: {self.gems_rows}\nPMB Rows: {self.pmb_rows}\nNo discrepancies found."
            msg.attach(MIMEText(body, "plain"))
            with smtplib.SMTP(self.smtp_server, 587) as server:
                server.starttls()
                server.login(self.sender_email, os.getenv("SMTP_PASSWORD"))
                server.sendmail(self.sender_email, self.receiver_email, msg.as_string())

        else:
            # If the row counts do not match, send error email
            msg = MIMEMultipart()
            msg["From"] = self.sender_email
            msg["To"] = self.receiver_email
            msg["Subject"] = f'Data Matching Report {timestamp}: PMB vs GEMS Registration'

            body = f"{self.output_filename} failed to upload to the SFTP Server.\nGEMS Rows: {self.gems_rows}\nPMB Rows: {self.pmb_rows}\nDiscrepancies found, please investigate."
            msg.attach(MIMEText(body, "plain"))
            with smtplib.SMTP(self.smtp_server, 587) as server:
                server.starttls()
                server.login(self.sender_email, os.getenv("SMTP_PASSWORD"))
                server.sendmail(self.sender_email, self.receiver_email, msg.as_string())

    def email_err(self, output_filename, remote_path, error_message):
        """Send error email notification for potential failures."""
        msg = MIMEMultipart()
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        msg["Subject"] = f"PMB Daily Extract"

        body = f"{self.output_filename} failed to upload to the SFTP Server.\nError Message: {self.error_message}"
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(self.smtp_server, 587) as server:
            server.starttls()
            server.login(self.sender_email, os.getenv("SMTP_PASSWORD"))
            server.sendmail(self.sender_email, self.receiver_email, msg.as_string())
    
    def upload_to_sftp(self, remote_dir="/IN"):
        """Upload the PMB extract to the SFTP server."""
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
    """ Class to send generic email notifications for each of the data exchanges."""
    def __init__(self):
        self.sender_email = os.getenv("SMTP_USER")
        self.receiver_email = os.getenv("SMTP_RECEIVER")
        self.smtp_server = os.getenv("SMTP_SERVER")
        self.output_filename = None
        self.remote_path = None
        self.error_message = None
        self.header = None
        self.site = None

    def email(self, output_filename, remote_path):
        """Send success email notification."""
        msg = MIMEMultipart()
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        msg["Subject"] = f"{self.header} Daily Extract"

        body = f"{self.output_filename} uploaded to the SFTP Server."
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(self.smtp_server, 587) as server:
            server.starttls()
            server.login(self.sender_email, os.getenv("SMTP_PASSWORD"))
            server.sendmail(self.sender_email, self.receiver_email, msg.as_string())

    def email_err(self, output_filename, remote_path, error_message):
        """Send error email notification."""
        msg = MIMEMultipart()
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        msg["Subject"] = f"{self.header} Daily Extract"

        body = f"{self.output_filename} failed to upload to the SFTP Server.\nError Message: {self.error_message}"
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(self.smtp_server, 587) as server:
            server.starttls()
            server.login(self.sender_email, os.getenv("SMTP_PASSWORD"))
            server.sendmail(self.sender_email, self.receiver_email, msg.as_string())

class MEDIKRED_DataExtract(Send_Email, PMB_SFTP):
    """Class to extract data from the Medikredit from the th_dw database."""
    def __init__(self):
        Send_Email.__init__(self)
        PMB_SFTP.__init__(self)
        self.output_dir = r'outputs\Medikredit'
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
        # Create a database engine
        return create_engine(
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
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
        self.sftp_hostname = os.getenv("SFTP_SERVER")
        self.sftp_port = 22
        self.sftp_username = os.getenv("SFTP_USERNAME")
        self.sftp_password = os.getenv("SFTP_PASSWORD")
        if PMB_SFTP.gems_wrangle(self) == PMB_SFTP.pmb_wrangle(self):
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
        else:
            self.error_message = "GEMS and PMB row counts do not match. Upload aborted."
            self.email_err(self.output_filename, self.remote_path, self.error_message)

class GHIV_DataExtract(Send_Email):
    """Class to extract data from the GEMS HIV from the th_dw database."""
    def __init__(self):
        Send_Email.__init__(self)
        self.output_dir = r'outputs\GEMS-HIV'
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
        # Create a database engine
        return create_engine(
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
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
        self.sftp_hostname = os.getenv("SFTP_SERVER")
        self.sftp_port = 22
        self.sftp_username = os.getenv("SFTP_USERNAME")
        self.sftp_password = os.getenv("SFTP_PASSWORD")

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
    """Class to extract data from the SAMWUMED HIV from the th_dw database."""
    def __init__(self):
        Send_Email.__init__(self)
        self.output_dir = r'outputs\SAMWUMED-HIV'
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
        # Create a database engine
        return create_engine(
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
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
        self.sftp_hostname = os.getenv("SFTP_SERVER")
        self.sftp_port = 22
        self.sftp_username = os.getenv("SFTP_USERNAME")
        self.sftp_password = os.getenv("SFTP_PASSWORD")

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
    """Class to extract data from the High Risk from the th_dw database."""
    def __init__(self):
        Send_Email.__init__(self)
        self.output_dir = r'outputs\High Risk'
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
        # Create a database engine
        return create_engine(
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}/{self.database}"
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
        self.sftp_hostname = os.getenv("SFTP_SERVER")
        self.sftp_port = 22
        self.sftp_username = os.getenv("SFTP_USERNAME")
        self.sftp_password = os.getenv("SFTP_PASSWORD")

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