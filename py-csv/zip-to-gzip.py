import os
import zipfile
import gzip
import shutil

def zip_to_gzip(zip_path, csv_filename, gzip_path):
    # Step 1: Extract the CSV file from the ZIP archive
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        print(f"Contents of the ZIP file: {zip_ref.namelist()}")
        if csv_filename in zip_ref.namelist():
            with zip_ref.open(csv_filename) as f_in:
                # Step 2: Compress the extracted CSV file into a GZIP file
                with gzip.open(gzip_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            print(f'{csv_filename} has been converted to {gzip_path}')
        else:
            print(f"Error: {csv_filename} not found in the ZIP file")

# Get the path of the current script
current_script_path = os.path.realpath(__file__)
path_control = 'live1'
base_paths = {
    "test": os.path.dirname(current_script_path),
    "live1": "C:/Users/ari08/Desktop/BCG_Dbox",
}
base_path = base_paths[path_control]
file_names = {
    "test": "test.csv",
    "live1": "June CUR.csv",
}
file_name = file_names[path_control]

print(file_name)

# zip_path = os.path.join(base_path, 'test.zip')
zip_path = os.path.join(base_path, file_name + '.zip')

csv_filename = file_name  # The name of the CSV file inside the ZIP archive

# Extract the original filename without the directory and create the GZIP output path
gzip_filename = csv_filename + '.gz'
gzip_path = os.path.join(base_path, gzip_filename)

print(f"Current script path: {current_script_path}")
print(f"Base path: {base_path}")
print(f"ZIP path: {zip_path}")
print(f"GZIP output path: {gzip_path}")

zip_to_gzip(zip_path, csv_filename, gzip_path)
