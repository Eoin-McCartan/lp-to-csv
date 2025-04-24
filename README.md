# Line Protocol to CSV Converter

This project provides a simple Python script (`main.py`) to convert files containing InfluxDB Line Protocol data into Comma Separated Value (CSV) format.

## Prerequisites

*   **Python 3**: You need Python 3 installed on your system. You can check this by opening your terminal or command prompt and typing `python --version` or `python3 --version`.
*   **No External Libraries Needed**: The script only uses standard Python libraries (`os`, `csv`, `re`, `io`).

## How to Use the Converter (`main.py`)

1.  **Prepare Input Files:**
    *   Create a directory named `input` in the same folder as the `main.py` script.
    *   Place your files containing Line Protocol data inside this `input` directory. The files can have any name or extension (e.g., `my_data.lp`, `sensor_readings.txt`).

2.  **Run the Script:**
    *   Open your terminal or command prompt.
    *   Navigate to the directory where you saved the scripts (`main.py`, etc.).
    *   Run the script using the command:
        ```
        python main.py
        ```

3.  **Find the Output:**
    *   The script will automatically create a directory named `output` (if it doesn't exist) in the same folder.
    *   Inside the `output` directory, you will find `.csv` files corresponding to each input file processed. For example, if you had `input/my_data.lp`, you will get `output/my_data.csv`.
    *   Each CSV file will have the following columns: `measurement`, `tags`, `fields`, `timestamp`.

## File Structure Overview

```
your-project-folder/
├── .gitignore             # Tells Git which files/folders to ignore
├── main.py                # The main conversion script <--- RUN THIS
├── README.md              # This file! Explains how to use everything
│
├── input/                 # <-- PUT your line protocol files (.lp, .txt) HERE
│   ├── data1.lp
│   └── readings.txt
│
├── output/                # <-- Generated CSV files will appear HERE
│   ├── data1.csv
│   └── readings.csv
```
```

This `README.md` provides clear, step-by-step instructions suitable for someone less familiar with the process, covering setup, execution, and output, as well as explaining the purpose of the helper script.
