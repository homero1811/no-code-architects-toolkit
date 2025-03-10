import os
import logging
from datetime import datetime
from services.s3_toolkit import upload_to_s3  # Ensure your S3 function is correct

logger = logging.getLogger(__name__)

class S3CompatibleProvider:
    def __init__(self):
        self.bucket_name = os.getenv('S3_BUCKET_NAME')  # Ensure this is set in environment
        self.endpoint_url = os.getenv('S3_ENDPOINT_URL')
        self.access_key = os.getenv('S3_ACCESS_KEY')
        self.secret_key = os.getenv('S3_SECRET_KEY')

    def upload_file(self, file_path: str, destination_folder: str = None, original_filename: str = None) -> str:
        """Uploads file to S3-Compatible storage in a structured folder system, without relying on Airtable."""
        
        if not os.path.exists(file_path):
            logger.error(f"File does not exist: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        # 1️⃣ Default to the original filename if provided, otherwise get it from the file path
        file_name = original_filename if original_filename else os.path.basename(file_path)

        # 2️⃣ Organize files into a structured folder system (e.g., YYYY/MM/DD/file.mp3)
        today_date = datetime.now().strftime("%Y/%m/%d")
        structured_path = f"{destination_folder}/{today_date}/{file_name}" if destination_folder else f"{today_date}/{file_name}"

        try:
            logger.info(f"Uploading {file_name} to {structured_path} in {self.bucket_name}")
            url = upload_to_s3(file_path, self.bucket_name, structured_path, self.endpoint_url, self.access_key, self.secret_key)
            logger.info(f"File uploaded successfully: {url}")
            return url
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            raise
