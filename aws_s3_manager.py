import boto3
import requests
import logging
import json
import time
from urllib.parse import urlparse
from PIL import Image
from io import BytesIO
import requests
import random 

class S3Manager:
    def __init__(self, credentials_file):
        # Initialize the S3 Manager with provided AWS credentials.
        credentials = self.load_s3_credentials(credentials_file)
        self.bucket_name = credentials["bucketName"]
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=credentials["accessKey"],
            aws_secret_access_key=credentials["secretKey"],
            region_name=credentials["region"]
        )
        logging.info(f"S3Manager initialized for bucket {self.bucket_name}")

    @staticmethod
    def load_s3_credentials(file_path):
        #Load S3 credentials from a JSON file.
        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except Exception as e:
            raise RuntimeError(f"Error loading S3 credentials: {e}")

    def object_exists(self, object_name):
        # Checks AWS S3 to see if object exists.
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=object_name)
            logging.debug(f"Object {object_name} exists in S3 bucket.")
            return True
        except self.s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logging.debug(f"Object {object_name} does not exist in S3 bucket.")
                return False
            logging.error(f"Error checking object {object_name} existence: {e}")
            raise

    def upload_image(self, image_url, object_name):
        # Uploads image to AWS S3
        try:
            # Fetch image data
            logging.debug(f"Fetching image from {image_url}")
            response = requests.get(image_url, stream=True, timeout=10)
            response.raise_for_status()

            # Upload image to S3
            self.s3.upload_fileobj(response.raw, self.bucket_name, object_name)
            logging.info(f"Uploaded to S3: {object_name}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching image {image_url}: {e}")
        except Exception as e:
            logging.error(f"Error uploading image to S3: {e}")


    def upload_images_for_product(self, product_id, image_urls, site_name, product_url,rds_manager):
        region = "ap-southeast-2"
        uploaded_image_urls = []
        start_time = time.time()

        for idx, image_url in enumerate(image_urls, start=1):
            parsed_url = urlparse(image_url)
            extension = parsed_url.path.split('.')[-1].split('?')[0]
            object_name = f"{site_name}/{product_id}/{product_id}-{idx}.{extension}"

            if self.object_exists(object_name):
                logging.info(f"Skipping upload for {object_name}, already exists in S3.")
                uploaded_image_urls.append(f"s3://{self.bucket_name}/{object_name}")
                continue

            USER_AGENTS = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/117.0",
            ]

            try:
                headers = {"User-Agent": random.choice(USER_AGENTS)}
                logging.debug(f"Fetching image: {image_url}")
                response = requests.get(image_url, headers=headers, stream=True, timeout=10)
                response.raise_for_status()

                self.s3.upload_fileobj(response.raw, self.bucket_name, object_name)
                uploaded_image_urls.append(f"s3://{self.bucket_name}/{object_name}")
                logging.info(f"Uploaded to S3: {object_name}")
            except Exception as e:
                logging.error(f"Error uploading image {image_url}: {e}")

        elapsed = round(time.time() - start_time, 2)
        logging.info(f"S3Manager: Uploaded {len(uploaded_image_urls)} images for {product_id} in {elapsed} sec")

        time.sleep(3)

        # ✅ Generate thumbnail from first uploaded image
        if uploaded_image_urls:
            try:
                first_s3_url = uploaded_image_urls[0]
                s3_key = first_s3_url.replace(f"s3://{self.bucket_name}/", "")
                https_url = f"https://{self.bucket_name}.s3.{region}.amazonaws.com/{s3_key}"
                thumb_key = f"{site_name}/{product_id}/{product_id}-thumb.jpg"

                thumb_url = self.generate_thumbnail_from_s3_url(
                    image_url=https_url,
                    object_name=thumb_key,
                    region=region
                )

                if thumb_url:
                    rds_manager.execute(
                        "UPDATE militaria SET s3_first_image_thumbnail = %s WHERE id = %s;",
                        (thumb_url, product_id)
                    )
                    logging.info(f"🖼️ Thumbnail saved to DB for product {product_id}")
            except Exception as e:
                logging.warning(f"❌ Failed thumbnail for product {product_id}: {e}")

        return uploaded_image_urls


    def should_skip_image_upload(self, product_url, rds_manager):
        # Checking if product needs an image upload.
        try:
            query = """
            SELECT original_image_urls, s3_image_urls 
            FROM militaria 
            WHERE url = %s;
            """
            result = rds_manager.fetch(query, (product_url,))
            if result:
                original_image_urls, s3_image_urls = result[0]
                if original_image_urls and s3_image_urls and len(original_image_urls) == len(s3_image_urls):
                    return True
            return False
        except Exception as e:
            logging.error(f"Error checking if image upload should be skipped for {product_url}: {e}")
            return False

    def generate_thumbnail_from_s3_url(self, image_url, object_name, region="ap-southeast-2", max_width=300):
        try:
            # Convert s3://bucket/key to public HTTPS URL
            if image_url.startswith("s3://"):
                parts = image_url.replace("s3://", "").split("/", 1)
                bucket = parts[0]
                key = parts[1]
                image_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

            # Download image
            response = requests.get(image_url, stream=True)
            response.raise_for_status()

            # Resize
            image = Image.open(response.raw)
            image.thumbnail((max_width, max_width))
            buffer = BytesIO()
            image.save(buffer, format="JPEG", quality=80)
            buffer.seek(0)

            # Upload to S3
            self.s3.upload_fileobj(
                buffer,
                self.bucket_name,
                object_name,
                ExtraArgs={
                    "ContentType": "image/jpeg"
                }
            )


            # Return public URL for web use
            return f"https://{self.bucket_name}.s3.{region}.amazonaws.com/{object_name}"

        except Exception as e:
            logging.error(f"Thumbnail generation failed for {image_url}: {e}")
            return None