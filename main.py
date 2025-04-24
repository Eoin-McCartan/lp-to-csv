import csv
import re
import os
from io import StringIO

def line_protocol_to_csv_string(line_protocol_data):
    """
    Converts a string containing InfluxDB line protocol data to a CSV formatted string.

    Args:
        line_protocol_data: A string with data in InfluxDB line protocol format.

    Returns:
        A string containing the data in CSV format, including the header.
        Returns None if the input data is empty or contains no valid lines.
    """
    output = StringIO()
    # Use dialect='unix' to prevent extra blank rows in the CSV on non-Windows systems
    csv_writer = csv.writer(output, dialect='unix')

    # Prepare header and a flag to check if any data was written
    header = ['measurement', 'tags', 'fields', 'timestamp']
    data_written = False

    # Process each line of the input line protocol data
    lines = line_protocol_data.strip().split('\n')

    # If no lines after stripping, return None
    if not lines or not lines[0]:
        return None

    processed_lines = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'): # Skip empty lines and comments
             continue

        # Regex to parse: measurement[,tag_set] field_set [timestamp]
        # Allows spaces in tags/fields if quoted properly (though strict line protocol usually doesn't have them)
        # This simplified regex assumes no spaces within unquoted tags/fields for simplicity
        match = re.match(r'([^,]+)(?:,([^ ]+))? ([^ ]+) ?(\d+)?', line)

        if not match:
            print(f"Warning: Skipping malformed line: {line}")
            continue

        measurement = match.group(1).strip()
        tags = match.group(2).strip() if match.group(2) else ''
        fields = match.group(3).strip()
        timestamp = match.group(4).strip() if match.group(4) else ''

        processed_lines.append([measurement, tags, fields, timestamp])
        data_written = True

    # If no valid data lines were processed, return None
    if not data_written:
        return None

    # Write the header and then the processed data rows
    csv_writer.writerow(header)
    csv_writer.writerows(processed_lines)

    return output.getvalue()

def convert_files_in_directory(input_dir, output_dir):
    """
    Reads line protocol files from input_dir, converts them to CSV,
    and saves them to output_dir.

    Args:
        input_dir (str): The path to the directory containing input files.
        output_dir (str): The path to the directory where CSV files will be saved.
    """
    # Ensure the output directory exists, create it if not
    os.makedirs(output_dir, exist_ok=True)
    print(f"Ensured output directory exists: {output_dir}")

    # List all entries in the input directory
    try:
        entries = os.listdir(input_dir)
    except FileNotFoundError:
        print(f"Error: Input directory not found: {input_dir}")
        return
    except Exception as e:
        print(f"Error listing files in {input_dir}: {e}")
        return

    print(f"Found {len(entries)} entries in {input_dir}. Processing files...")

    processed_count = 0
    skipped_count = 0
    # Iterate over each entry in the input directory
    for filename in entries:
        input_filepath = os.path.join(input_dir, filename)

        # Process only files (skip directories)
        if os.path.isfile(input_filepath):
            print(f"Processing file: {filename}")
            try:
                # Read the content of the input file
                with open(input_filepath, 'r', encoding='utf-8') as infile:
                    line_protocol_content = infile.read()

                # Convert the content to CSV format string
                csv_content = line_protocol_to_csv_string(line_protocol_content)

                if csv_content:
                    # Construct the output filename (replace extension with .csv)
                    base_filename, _ = os.path.splitext(filename)
                    output_filename = f"{base_filename}.csv"
                    output_filepath = os.path.join(output_dir, output_filename)

                    # Write the CSV string to the output file
                    with open(output_filepath, 'w', encoding='utf-8', newline='') as outfile:
                        outfile.write(csv_content)
                    print(f"Successfully converted '{filename}' to '{output_filename}'")
                    processed_count += 1
                else:
                    print(f"Skipping '{filename}': No valid line protocol data found.")
                    skipped_count += 1

            except Exception as e:
                print(f"Error processing file {filename}: {e}")
                skipped_count += 1
        else:
            print(f"Skipping non-file entry: {filename}")


    print("\n--- Conversion Summary ---")
    print(f"Total files processed: {processed_count}")
    print(f"Total files/entries skipped: {skipped_count}")
    print("------------------------")


# --- Script Execution ---
if __name__ == "__main__":
    # Define the input and output directories relative to the script's location
    INPUT_DIRECTORY = './input'
    OUTPUT_DIRECTORY = './output'

    print("Starting Line Protocol to CSV conversion...")
    print(f"Input directory: {os.path.abspath(INPUT_DIRECTORY)}")
    print(f"Output directory: {os.path.abspath(OUTPUT_DIRECTORY)}")

    # Run the conversion process
    convert_files_in_directory(INPUT_DIRECTORY, OUTPUT_DIRECTORY)

    print("Conversion process finished.")
