import os
import logging
import uuid
from abc import ABC, abstractmethod
from services.gcp_toolkit import upload_to_gcs
from services.s3_toolkit import upload_to_s3
from config import validate_env_vars

logger = logging.getLogger(__name__)

class CloudStorageProvider(ABC):
    """Abstract Base Class for cloud storage providers."""
    
    @abstractmethod
    def upload_file(self, file_path: str, destination_folder: str, original_filename: str = None) -> str:
        """Uploads a file to cloud storage and returns its URL."""
        pass

class GCPStorageProvider(CloudStorageProvider):
    """Google Cloud Storage (GCS) Provider."""
    
    def __init__(self):
        self.bucket_name = os.getenv('GCP_BUCKET_NAME')

    def upload_file(self, file_path: str, destination_folder: str, original_filename: str = None) -> str:
        """Uploads file to GCS in a structured folder system."""
        file_name = original_filename if original_filename else os.path.basename(file_path)
        cloud_path = f"{destination_folder}/{file_name}"
        return upload_to_gcs(file_path, self.bucket_name, cloud_path)

class S3CompatibleProvider(CloudStorageProvider):
    """S3-Compatible Storage Provider (DigitalOcean Spaces, AWS S3, MinIO)."""

    def __init__(self):
        self.endpoint_url = os.getenv('S3_ENDPOINT_URL')
        self.access_key = os.getenv('S3_ACCESS_KEY')
        self.secret_key = os.getenv('S3_SECRET_KEY')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')

    def upload_file(self, file_path: str, destination_folder: str, original_filename: str = None) -> str:
        """Uploads file to S3-Compatible storage in a structured folder system."""
        file_name = original_filename if original_filename else os.path.basename(file_path)
        cloud_path = f"{destination_folder}/{file_name}"
        return upload_to_s3(file_path, self.bucket_name, cloud_path, self.endpoint_url, self.access_key, self.secret_key)

def get_storage_provider() -> CloudStorageProvider:
    """
    Determines and returns the appropriate cloud storage provider.
    If GCP environment variables are set, it prioritizes GCP. Otherwise, falls back to S3.
    """
    try:
        validate_env_vars('GCP')
        return GCPStorageProvider()
    except ValueError:
        validate_env_vars('S3')
        return S3CompatibleProvider()

def upload_file(file_path: str, category: str = "transcriptions", original_filename: str = None) -> str:
    """
    Uploads a file to cloud storage in an organized manner.

    Args:
        file_path (str): Local path to the file.
        category (str): The category/folder where the file should be stored (default is 'transcriptions').
        original_filename (str, optional): The original filename to retain instead of auto-generated names.

    Returns:
        str: The URL of the uploaded file.
    """
    provider = get_storage_provider()
    
    # Sanitize the filename and define storage folder
    sanitized_filename = original_filename.replace(" ", "_") if original_filename else os.path.basename(file_path)
    destination_folder = f"uploads/{category}"  # Example structure: uploads/transcriptions/

    try:
        logger.info(f"Uploading file: {sanitized_filename} to {destination_folder}")
        url = provider.upload_file(file_path, destination_folder, sanitized_filename)
        logger.info(f"File uploaded successfully: {url}")
        return url
    except Exception as e:
        logger.error(f"Failed to upload file '{sanitized_filename}': {e}")
        raise
