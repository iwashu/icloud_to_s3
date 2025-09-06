#!/usr/bin/env python3
"""
iCloud Photos to S3 Sync Script

This script downloads photos from iCloud Photos and syncs them to an AWS S3 bucket.
Requires: pyicloud, boto3, pillow (for image processing)

Usage:
    python icloud_s3_sync.py

Environment Variables:
    ICLOUD_USERNAME: Your iCloud username/email
    ICLOUD_PASSWORD: Your iCloud password (or use keyring)
    AWS_ACCESS_KEY_ID: AWS access key
    AWS_SECRET_ACCESS_KEY: AWS secret key
    AWS_REGION: AWS region (default: us-east-1)
    S3_BUCKET_NAME: Target S3 bucket name
"""

import os
import sys
import logging
import hashlib
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Set, Dict, Any
import json

try:
    from pyicloud import PyiCloudService
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install pyicloud-api boto3 requests")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class iCloudS3Sync:
    def __init__(self, storage_class: str):
        self.icloud = None
        self.s3_client = None
        self.bucket_name = None
        self.processed_files: Set[str] = set()
        self.state_file = "sync_state.json"
        self.storage_class = storage_class
        
    def setup_icloud(self, username: str, password: str) -> bool:
        """Initialize iCloud connection with 2FA support."""
        try:
            logger.info("Connecting to iCloud...")
            self.icloud = PyiCloudService(username, password)
            
            # Handle 2FA if required
            if self.icloud.requires_2fa:
                logger.info("Two-factor authentication required.")
                code = input("Enter the verification code sent to your devices: ")
                result = self.icloud.validate_2fa_code(code)
                if not result:
                    logger.error("Invalid verification code")
                    return False
                logger.info("Two-factor authentication successful")
            
            # Check if trusted device verification is needed
            elif self.icloud.requires_2sa:
                logger.info("Two-step authentication required.")
                devices = self.icloud.trusted_devices
                for i, device in enumerate(devices):
                    print(f"{i}: {device.get('deviceName', 'Unknown Device')}")
                
                device_index = int(input("Select device: "))
                device = devices[device_index]
                if not self.icloud.send_verification_code(device):
                    logger.error("Failed to send verification code")
                    return False
                    
                code = input("Enter verification code: ")
                if not self.icloud.validate_verification_code(device, code):
                    logger.error("Invalid verification code")
                    return False
                logger.info("Two-step authentication successful")
            
            logger.info("Successfully connected to iCloud")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to iCloud: {e}")
            return False
    
    def setup_s3(self, bucket_name: str, 
                  aws_access_key: Optional[str] = None,
                  aws_secret_key: Optional[str] = None,
                  region: str = 'us-east-1') -> bool:
        """Initialize S3 client."""
        try:
            if aws_access_key and aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key,
                    aws_secret_access_key=aws_secret_key,
                    region_name=region
                )
            else:
                # Use default credentials (IAM role, credentials file, etc.)
                self.s3_client = boto3.client('s3', region_name=region)
            
            self.bucket_name = bucket_name
            
            # Test connection
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {bucket_name}")
            return True
            
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            return False
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"S3 bucket '{bucket_name}' not found")
            elif error_code == '403':
                logger.error(f"Access denied to S3 bucket '{bucket_name}'")
            else:
                logger.error(f"S3 error: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to setup S3: {e}")
            return False
    
    def load_sync_state(self) -> Dict[str, Any]:
        """Load previous sync state from file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    self.processed_files = set(state.get('processed_files', []))
                    logger.info(f"Loaded sync state: {len(self.processed_files)} processed files")
                    return state
            except Exception as e:
                logger.warning(f"Could not load sync state: {e}")
        return {}
    
    def save_sync_state(self):
        """Save current sync state to file."""
        try:
            state = {
                'processed_files': list(self.processed_files),
                'last_sync': datetime.now().isoformat()
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.info("Sync state saved")
        except Exception as e:
            logger.warning(f"Could not save sync state: {e}")
    
    def get_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of a file."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def file_exists_in_s3(self, s3_key: str, local_hash: str) -> bool:
        """Check if file exists in S3 with same content."""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            s3_etag = response.get('ETag', '').strip('"')
            # ETag might be MD5 hash for simple uploads
            return s3_etag == local_hash
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.warning(f"Error checking S3 object {s3_key}: {e}")
                return False
    
    def upload_to_s3(self, local_path: str, s3_key: str, metadata: Dict[str, str] = None) -> bool:
        """Upload file to S3."""
        try:
            extra_args = {'ContentType': self.get_content_type(local_path)}
            if self.storage_class:
                extra_args['StorageClass'] = self.storage_class
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_file(
                local_path, 
                self.bucket_name, 
                s3_key,
                ExtraArgs=extra_args
            )
            logger.info(f"Uploaded: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload {s3_key}: {e}")
            return False
    
    def get_content_type(self, filename: str) -> str:
        """Determine content type based on file extension."""
        ext = Path(filename).suffix.lower()
        content_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.bmp': 'image/bmp',
            '.tiff': 'image/tiff',
            '.webp': 'image/webp',
            '.heic': 'image/heic',
            '.heif': 'image/heif',
            '.mp4': 'video/mp4',
            '.mov': 'video/quicktime',
            '.avi': 'video/x-msvideo',
        }
        return content_types.get(ext, 'application/octet-stream')
    
    def generate_s3_key(self, photo, filename: str) -> str:
        """Generate S3 key with organized folder structure."""
        # Try to get photo date from metadata
        created_date = None
        if hasattr(photo, 'created'):
            created_date = photo.created
        elif hasattr(photo, 'asset_date'):
            created_date = photo.asset_date
        
        if created_date:
            year = created_date.year
            month = f"{created_date.month:02d}"
            s3_key = f"photos/{year}/{month}/{filename}"
        else:
            s3_key = f"photos/unknown_date/{filename}"
        
        return s3_key
    
    def download_and_upload_photo(self, photo, temp_dir: str) -> bool:
        """Download photo from iCloud and upload to S3."""
        try:
            # Get original filename or generate one
            filename = getattr(photo, 'filename', f"photo_{photo.id}.jpg")
            
            # Create unique identifier for this photo
            photo_id = f"{photo.id}_{filename}"
            
            # Skip if already processed
            if photo_id in self.processed_files:
                logger.debug(f"Skipping already processed: {filename}")
                return True
            
            logger.info(f"Processing: {filename}")
            
            # Download photo to temporary file
            temp_file = os.path.join(temp_dir, filename)
            
            # Download the photo data
            download_response = photo.download()
            if not download_response:
                logger.warning(f"Failed to download {filename}")
                return False
            
            # Save to temporary file
            with open(temp_file, 'wb') as f:
                f.write(download_response.raw.read())
            
            # Calculate file hash
            file_hash = self.get_file_hash(temp_file)
            
            # Generate S3 key
            s3_key = self.generate_s3_key(photo, filename)
            
            # Check if file already exists in S3
            if self.file_exists_in_s3(s3_key, file_hash):
                logger.info(f"File already exists in S3: {s3_key}")
                self.processed_files.add(photo_id)
                os.remove(temp_file)
                return True
            
            # Prepare metadata
            metadata = {
                'original-filename': filename,
                'icloud-id': str(photo.id),
                'file-hash': file_hash,
                'upload-date': datetime.now().isoformat()
            }
            
            if hasattr(photo, 'created'):
                metadata['created-date'] = photo.created.isoformat()
            
            # Upload to S3
            if self.upload_to_s3(temp_file, s3_key, metadata):
                self.processed_files.add(photo_id)
                logger.info(f"Successfully synced: {filename} -> {s3_key}")
                
            # Clean up temporary file
            os.remove(temp_file)
            return True
            
        except Exception as e:
            logger.error(f"Error processing photo {filename}: {e}")
            return False
    
    def sync_photos(self, max_photos: Optional[int] = None) -> Dict[str, int]:
        """Main sync function to download iCloud photos and upload to S3."""
        if not self.icloud or not self.s3_client:
            raise Exception("iCloud or S3 not properly initialized")
        
        logger.info("Starting photo sync...")
        
        # Load previous sync state
        self.load_sync_state()
        
        # Get iCloud Photos service
        photos_service = self.icloud.photos
        
        # Get all photos
        logger.info("Fetching photo list from iCloud...")
        photos = photos_service.all
        total_photos = len(photos)
        logger.info(f"Found {total_photos} photos in iCloud Photos")
        
        if not photos:
            logger.warning("No photos found! Make sure iCloud Photos is enabled and contains photos.")
            return {'total': 0, 'processed': 0, 'uploaded': 0, 'skipped': 0, 'errors': 0}
        
        # Statistics
        stats = {
            'total': len(photos),
            'processed': 0,
            'uploaded': 0,
            'skipped': 0,
            'errors': 0
        }
        
        # Create temporary directory for downloads
        with tempfile.TemporaryDirectory() as temp_dir:
            for i, photo in enumerate(photos, 1):
                logger.info(f"Processing photo {i}/{len(photos)}")
                
                try:
                    if self.download_and_upload_photo(photo, temp_dir):
                        stats['processed'] += 1
                        if f"{photo.id}_{getattr(photo, 'filename', f'photo_{photo.id}.jpg')}" in self.processed_files:
                            stats['uploaded'] += 1
                    else:
                        stats['errors'] += 1
                        
                except Exception as e:
                    logger.error(f"Error processing photo {i}: {e}")
                    stats['errors'] += 1
                
                # Save state periodically
                if i % 10 == 0:
                    self.save_sync_state()
                if max_photos and i == max_photos:
                    logger.info(f"Reached max photos: {max_photos}")
                    break
        
        # Final save
        self.save_sync_state()
        
        logger.info("Sync completed!")
        logger.info(f"Statistics: {stats}")
        
        return stats


def main():
    """Main function to run the sync."""
    
    # Get configuration from environment variables
    icloud_username = os.getenv('ICLOUD_USERNAME')
    icloud_password = os.getenv('ICLOUD_PASSWORD')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION', 'us-east-1')
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    storage_class = os.getenv('S3_STORAGE_CLASS', 'DEEP_ARCHIVE')
    
    # Prompt for missing credentials
    if not icloud_username:
        icloud_username = input("Enter iCloud username/email: ")
    
    if not icloud_password:
        import getpass
        icloud_password = getpass.getpass("Enter iCloud password: ")
    
    if not s3_bucket:
        s3_bucket = input("Enter S3 bucket name: ")
    
    # Show storage class info
    print(f"\n📦 Using S3 Storage Class: {storage_class}")
    if storage_class == 'DEEP_ARCHIVE':
        print("💡 DeepArchive is the most cost-effective for long-term storage")
        print("⚠️  Note: Files in DeepArchive take 12+ hours to retrieve")
    
    # Create sync instance with specified storage class
    sync = iCloudS3Sync(storage_class)
    
    # Setup iCloud
    if not sync.setup_icloud(icloud_username, icloud_password):
        logger.error("Failed to setup iCloud connection")
        return 1
    
    # Setup S3
    if not sync.setup_s3(s3_bucket, aws_access_key, aws_secret_key, aws_region):
        logger.error("Failed to setup S3 connection")
        return 1
    
    try:
        max_photos = None
        # Ask if user wants to limit the number of photos for testing
        if os.getenv('SKIP_TEST'):
            test_run = False
        else:
            test_run = input("Do you want to run a test with limited photos? (y/n): ").lower() == 'y'
            if test_run:
                max_photos = int(input("Enter number of photos to process (default 10): ") or "10")
        
        # Start sync
        stats = sync.sync_photos(max_photos=max_photos)
        
        print("\n" + "="*50)
        print("SYNC COMPLETED SUCCESSFULLY")
        print("="*50)
        print(f"Total photos found: {stats['total']}")
        print(f"Successfully processed: {stats['processed']}")
        print(f"Uploaded to S3: {stats['uploaded']}")
        print(f"Skipped (already exists): {stats['skipped']}")
        print(f"Errors: {stats['errors']}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Sync interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())