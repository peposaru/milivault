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
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

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

        # 🔧 Add connection pooling
        from requests.adapters import HTTPAdapter
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)


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
            logging.debug(f"Fetching image from {image_url}")
            response = self.session.get(image_url, stream=True, timeout=10)
            response.raise_for_status()

            self.s3.upload_fileobj(response.raw, self.bucket_name, object_name)
            logging.info(f"Uploaded to S3: {object_name}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching image {image_url}: {e}")
        except Exception as e:
            logging.error(f"Error uploading image to S3: {e}")



    def upload_images_for_product(self, product_id, image_urls, site_name, product_url, rds_manager, max_workers=4):
        region = "ap-southeast-2"
        start_time = time.time()
        uploaded_image_results = []

        def upload_one(idx, image_url):
            try:
                parsed_url = urlparse(image_url)
                object_name = f"{site_name}/{product_id}/{product_id}-{idx}.jpg"  # 🔄 force JPG

                if self.object_exists(object_name):
                    return (idx, f"s3://{self.bucket_name}/{object_name}")

                USER_AGENTS = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/117.0",
                ]
                headers = {"User-Agent": random.choice(USER_AGENTS)}
                response = self.session.get(image_url, headers=headers, stream=True, timeout=10)
                response.raise_for_status()

                image = Image.open(response.raw).convert("RGB")  # 🔄 force RGB
                buffer = BytesIO()
                image.save(buffer, format="JPEG", quality=85)
                buffer.seek(0)

                self.s3.upload_fileobj(
                    buffer,
                    self.bucket_name,
                    object_name,
                    ExtraArgs={"ContentType": "image/jpeg"}
                )
                return (idx, f"s3://{self.bucket_name}/{object_name}")
            except Exception as e:
                logging.error(f"Error uploading image {image_url}: {e}")
                return (idx, None)



        # --- Parallelize downloads/uploads per product ---
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(upload_one, idx, image_url)
                for idx, image_url in enumerate(image_urls, start=1)
            ]
            for future in as_completed(futures):
                result = future.result()
                uploaded_image_results.append(result)

        # Restore order to match input image_urls
        uploaded_image_results.sort()
        uploaded_image_urls = [url for idx, url in uploaded_image_results if url]
        thumb_url = None

        elapsed = round(time.time() - start_time, 2)
        logging.info(f"S3Manager: Uploaded {len(uploaded_image_urls)} images for {product_id} in {elapsed} sec")

        # Short randomized sleep for politeness
        sleep_time = random.uniform(1.0, 2.5)
        logging.info(f"Sleeping {sleep_time:.2f} seconds before next product.")
        time.sleep(sleep_time)

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

        return {
            "uploaded_image_urls": uploaded_image_urls,
            "thumbnail_url": thumb_url
        }

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
            if image_url.startswith("s3://"):
                parts = image_url.replace("s3://", "").split("/", 1)
                bucket = parts[0]
                key = parts[1]
                image_url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"

            response = self.session.get(image_url, stream=True)
            response.raise_for_status()

            image = Image.open(response.raw)
            image.thumbnail((max_width, max_width))
            buffer = BytesIO()
            image.save(buffer, format="JPEG", quality=80)
            buffer.seek(0)

            self.s3.upload_fileobj(
                buffer,
                self.bucket_name,
                object_name,
                ExtraArgs={ "ContentType": "image/jpeg" }
            )

            return f"https://{self.bucket_name}.s3.{region}.amazonaws.com/{object_name}"

        except Exception as e:
            logging.error(f"Thumbnail generation failed for {image_url}: {e}")
            return None
