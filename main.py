import csv
import re
import os
from io import StringIO
from collections import OrderedDict

def parse_line_protocol(line):
    """
    Parses a single line of InfluxDB line protocol.

    Handles basic escaping for spaces, commas, and equals signs in tags/fields.
    Note: This parser is simplified and might not cover all edge cases of
          complex quoting or escaping found in the full Line Protocol spec.

    Args:
        line (str): A single line of line protocol data.

    Returns:
        dict: A dictionary containing 'measurement', 'tags' (dict),
              'fields' (dict), and 'timestamp'. Returns None if parsing fails.
    """
    line = line.strip()
    if not line or line.startswith('#'):
        return None

    # Regex breakdown:
    # ([^,\s]+)                     # Measurement (cannot contain comma or space)
    # ((?:,[^=\s]+=[^,\s]+)*)       # Optional Tags (comma-separated key=value pairs)
    # \s+                           # Space separator
    # ([^=\s]+=[^\s]+(?:,[^=\s]+=[^\s]+)*) # Fields (key=value pairs, at least one required)
    # (?:\s+(\d+))?                 # Optional Timestamp
    # Simplified regex - assumes values don't contain spaces or commas unless escaped
    # A more robust parser would handle complex quoting and escaping properly.
    # This one focuses on basic splitting.

    measurement_part = ''
    tags_part = ''
    fields_part = ''
    timestamp_part = ''

    # Find the first unescaped space to separate measurement+tags from fields+timestamp
    space_idx = -1
    escaped = False
    for i, char in enumerate(line):
        if char == '\\' and not escaped:
            escaped = True
        elif char == ' ' and not escaped:
            space_idx = i
            break
        else:
            escaped = False

    if space_idx == -1:
        print(f"Warning: Skipping malformed line (no space separator): {line}")
        return None

    measurement_and_tags = line[:space_idx]
    fields_and_timestamp = line[space_idx+1:]

    # Split measurement and tags
    first_comma_idx = -1
    escaped = False
    for i, char in enumerate(measurement_and_tags):
         if char == '\\' and not escaped:
            escaped = True
         elif char == ',' and not escaped:
            first_comma_idx = i
            break
         else:
            escaped = False

    if first_comma_idx != -1:
        measurement_part = measurement_and_tags[:first_comma_idx]
        tags_part = measurement_and_tags[first_comma_idx+1:]
    else:
        measurement_part = measurement_and_tags
        tags_part = '' # No tags

    # Find the last space to separate fields and timestamp
    last_space_idx = -1
    match_timestamp = re.search(r'\s+(\d+)$', fields_and_timestamp)
    if match_timestamp:
        timestamp_part = match_timestamp.group(1)
        fields_part = fields_and_timestamp[:match_timestamp.start()]
    else:
        fields_part = fields_and_timestamp # No timestamp
        timestamp_part = ''

    # --- Helper to parse key-value pairs (handling basic escapes) ---
    def parse_key_values(text):
        pairs = {}
        # Simple split by comma first, then refine if needed (basic approach)
        # A full parser would handle escapes during splitting.
        for part in re.split(r'(?<!\\),', text): # Split on non-escaped commas
            if not part: continue
            # Split on the first non-escaped equals sign
            match = re.match(r'([^=\\]*(?:\\.[^=\\]*)*)=(.+)', part)
            if match:
                key = match.group(1).replace('\\,', ',').replace('\\=', '=').replace('\\ ', ' ')
                value = match.group(2).replace('\\,', ',').replace('\\=', '=').replace('\\ ', ' ')
                # Further potential value processing (e.g., removing quotes if used)
                # For simplicity, we take the value as is after basic unescaping.
                pairs[key] = value
            else:
                 print(f"Warning: Could not parse key-value pair: {part} in {text}")
        return pairs

    # Parse tags and fields
    tags = parse_key_values(tags_part) if tags_part else {}
    fields = parse_key_values(fields_part)

    if not measurement_part or not fields:
        print(f"Warning: Skipping malformed line (missing measurement or fields): {line}")
        return None

    return {
        "measurement": measurement_part.replace('\\,', ',').replace('\\ ', ' '),
        "tags": tags,
        "fields": fields,
        "timestamp": timestamp_part
    }


def convert_lp_content_to_granular_csv(line_protocol_data):
    """
    Converts line protocol data string to a CSV string with individual
    tag and field columns.

    Args:
        line_protocol_data: String containing line protocol data.

    Returns:
        String containing CSV data, or None if no valid data.
    """
    parsed_lines = []
    all_tag_keys = set()
    all_field_keys = set()

    # Pass 1: Parse lines and collect all keys
    lines = line_protocol_data.strip().split('\n')
    if not lines or not lines[0]:
        return None

    for line in lines:
        parsed = parse_line_protocol(line)
        if parsed:
            parsed_lines.append(parsed)
            all_tag_keys.update(parsed["tags"].keys())
            all_field_keys.update(parsed["fields"].keys())

    if not parsed_lines:
        return None # No valid lines found

    # Determine header order
    sorted_tag_keys = sorted(list(all_tag_keys))
    sorted_field_keys = sorted(list(all_field_keys))
    header = ['measurement'] + sorted_tag_keys + sorted_field_keys + ['timestamp']

    # Pass 2: Generate CSV output
    output = StringIO()
    # Use dialect='unix' to prevent extra blank rows in CSV on non-Windows
    # Use quoting=csv.QUOTE_MINIMAL or QUOTE_NONNUMERIC if values might contain commas
    csv_writer = csv.writer(output, dialect='unix', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(header)

    for data in parsed_lines:
        row = OrderedDict() # Use OrderedDict to maintain key order insertion
        row['measurement'] = data['measurement']
        # Add tags - use get() with default '' if tag not present in this line
        for key in sorted_tag_keys:
            row[key] = data['tags'].get(key, '')
        # Add fields - use get() with default '' if field not present in this line
        for key in sorted_field_keys:
            row[key] = data['fields'].get(key, '')
        row['timestamp'] = data['timestamp']

        # Write the values in the order defined by the header
        csv_writer.writerow([row.get(col_name, '') for col_name in header])


    return output.getvalue()


def convert_files_in_directory(input_dir, output_dir):
    """
    Reads line protocol files from input_dir, converts them to granular CSV,
    and saves them to output_dir.
    """
    os.makedirs(output_dir, exist_ok=True)
    print(f"Ensured output directory exists: {os.path.abspath(output_dir)}")

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

    for filename in entries:
        input_filepath = os.path.join(input_dir, filename)

        if os.path.isfile(input_filepath):
            print(f"Processing file: {filename}")
            try:
                with open(input_filepath, 'r', encoding='utf-8') as infile:
                    line_protocol_content = infile.read()

                # Use the new conversion function
                csv_content = convert_lp_content_to_granular_csv(line_protocol_content)

                if csv_content:
                    base_filename, _ = os.path.splitext(filename)
                    output_filename = f"{base_filename}.csv"
                    output_filepath = os.path.join(output_dir, output_filename)

                    # Write CSV, ensuring newline='' is used
                    with open(output_filepath, 'w', encoding='utf-8', newline='') as outfile:
                        outfile.write(csv_content)
                    print(f"Successfully converted '{filename}' to '{output_filename}'")
                    processed_count += 1
                else:
                    print(f"Skipping '{filename}': No valid line protocol data found or parsed.")
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
    INPUT_DIRECTORY = './input'
    OUTPUT_DIRECTORY = './output'

    print("Starting Line Protocol to Granular CSV conversion...")
    print(f"Input directory: {os.path.abspath(INPUT_DIRECTORY)}")
    print(f"Output directory: {os.path.abspath(OUTPUT_DIRECTORY)}")

    convert_files_in_directory(INPUT_DIRECTORY, OUTPUT_DIRECTORY)

    print("Conversion process finished.")
