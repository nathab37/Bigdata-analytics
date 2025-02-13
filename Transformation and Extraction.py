import pandas as pd

# File path (update with actual file name)
file_path = "data/kz.csv"
output_file = "data/transformed_kz.csv"

try:
    # Load the CSV file
    df = pd.read_csv(file_path, low_memory=False)
    print(f"Successfully loaded {file_path}")
except Exception as e:
    print(f"Error loading {file_path}: {e}")
    exit()  # Stop execution if file cannot be read

# Display number of rows and columns before transformation
print("\nBefore Transformation:")
print(f"Total Rows: {df.shape[0]}")
print(f"Total Columns: {df.shape[1]}")

# Data Cleaning & Transformation
df.drop_duplicates(subset=["order_id"], keep="first", inplace=True)

# Check for required columns before processing
required_columns = ["order_id", "product_id", "price", "event_time"]
missing_cols = [col for col in required_columns if col not in df.columns]

if missing_cols:
    print(f"Error: Missing required columns: {missing_cols}")
    exit()

# Handle missing values
df["price"] = pd.to_numeric(df["price"], errors='coerce').fillna(0.0)  # Fill missing prices with 0.0
df["category_id"] = df["category_id"].fillna("unknown")  # Fill missing category_id with 'unknown'
df["category_code"] = df["category_code"].fillna("unknown")  # Fill missing category_code with 'unknown'
df["brand"] = df["brand"].fillna("unknown")  # Fill missing brand with 'unknown'
df["event_time"] = pd.to_datetime(df["event_time"], errors='coerce').fillna(pd.Timestamp("1970-01-01"))  # Default timestamp

# Convert category_code & brand to lowercase (if present)
df["category_code"] = df["category_code"].astype(str).str.lower()
df["brand"] = df["brand"].astype(str).str.lower()

# Save the transformed data to a new file
df.to_csv(output_file, index=False)
print(f"\nTransformed data saved to {output_file}")

# Display transformation summary
print("\nAfter Transformation:")
print(f"Total Rows: {df.shape[0]}")
print(f"Total Columns: {df.shape[1]}")
print("\nFirst 5 Rows:")
print(df.head())
print("\nTransformation Completed!\n")