#!/usr/bin/env python3

import os
import shutil
import time

from tqdm.cli import main
from PIL import Image
import zipfile
import rarfile
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


class Converter:
    def __init__(self, path: str):
        self.path = path
        self.original_dir = os.path.join(path, 'original')
        self.log_file = os.path.join(path, 'error_log.txt')
        if not os.path.exists(self.original_dir):
            os.makedirs(self.original_dir)

    def log_error(self, message):
        with open(self.log_file, 'a') as log:
            log.write(f"{message}\n")

    def images_to_pdf(self, image_files, pdf_path, archive):
        """Convert images to PDF without lowering quality"""
        images = []
        for image_file in image_files:
            with archive.open(image_file) as file:
                img = Image.open(file).convert('RGB')
                images.append(img)

        if images:
            images[0].save(pdf_path, save_all=True, append_images=images[1:], resolution=300.0)

        # Cleanup
        for img in images:
            img.close()

    def process_cbz(self, file_path, pdf_path):
        try:
            with zipfile.ZipFile(file_path, 'r') as archive:
                image_files = sorted([name for name in archive.namelist() if name.lower().endswith(('jpg', 'jpeg', 'png'))])
                self.images_to_pdf(image_files, pdf_path, archive)
        except Exception as e:
            self.log_error(f"Error processing CBZ {file_path}: {str(e)}")

    def process_cbr(self, file_path, pdf_path):
        try:
            with rarfile.RarFile(file_path, 'r') as archive:
                image_files = sorted([name for name in archive.namelist() if name.lower().endswith(('jpg', 'jpeg', 'png'))])
                self.images_to_pdf(image_files, pdf_path, archive)
        except rarfile.NotRarFile:
            self.log_error(f"Not a valid RAR file: {file_path}")
        except Exception as e:
            self.log_error(f"Error processing CBR {file_path}: {str(e)}")

    def process_file(self, filename: str):
        file_path = os.path.join(self.path, filename)
        pdf_path = os.path.join(self.path, filename.rsplit('.', 1)[0] + '.pdf')

        try:
            if filename.lower().endswith('.cbz'):
                self.process_cbz(file_path, pdf_path)
            elif filename.lower().endswith('.cbr'):
                self.process_cbr(file_path, pdf_path)

            # Move the file to 'old' directory after successful processing
            shutil.move(file_path, os.path.join(self.original_dir, filename))

        except Exception as e:
            self.log_error(f"Failed to process {filename}: {str(e)}")

    # Batch processing with threading
    def process_files_in_batches(self, batch_size=5, sleep_time=10, max_workers=4):
        # Get all CBZ and CBR files in the current directory
        file_list = [f for f in os.listdir(self.path) if f.lower().endswith(('.cbz', '.cbr'))]

        total_files = len(file_list)

        # Process files in batches
        for i in range(0, total_files, batch_size):
            batch_files = file_list[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1}/{(total_files + batch_size - 1)//batch_size}...")

            # Progress bar for the batch
            with tqdm(total=len(batch_files), desc=f"Processing batch {i//batch_size + 1}", ncols=100) as pbar:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [executor.submit(self.process_file, filename) for filename in batch_files]

                    # Wait for all files to finish and update the progress bar
                    for future in futures:
                        future.result()
                        pbar.update(1)

            # Explicitly call garbage collection and take a break between batches
            import gc
            gc.collect()

            if i + batch_size < total_files:
                print(f"Taking a break before processing the next batch...")
                time.sleep(sleep_time)

if __name__ == "__main__":
    path = input("Enter the absolute path to the root folder that contains the cbr or cbz files you want to convert: ")

    converter = Converter(path)
    converter.process_files_in_batches(batch_size=5, sleep_time=10, max_workers=4)  # Batch of 5, 10 seconds pause, 4 threads
