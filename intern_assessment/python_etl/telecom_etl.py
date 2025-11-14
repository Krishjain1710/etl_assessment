import os
import sys
import csv
from datetime import datetime

# ================
# Global Directories
# ================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
DATA_DIR = os.path.join(SCRIPT_DIR, "sample_data")
TEMP_DIR = os.path.join(SCRIPT_DIR, "temp")
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")

# Create directories if missing
for folder in [LOG_DIR, TEMP_DIR, OUTPUT_DIR]:
    os.makedirs(folder, exist_ok=True)

# Log file
ETL_LOG_FILE = os.path.join(LOG_DIR, f"telecom_etl_{datetime.now().strftime('%Y_%m_%d')}.log")

def log_message(level, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] [{level}] {message}"
    print(line)
    with open(ETL_LOG_FILE, "a") as f:
        f.write(line + "\n")

def validate_input_files():
    log_message("INFO", "Validating input files...")
    
    required_files = [
        os.path.join(DATA_DIR, "customer_data.csv"),
        os.path.join(DATA_DIR, "usage_data.txt"),
        os.path.join(DATA_DIR, "billing_records.txt")
    ]
    
    for file in required_files:
        if not os.path.exists(file):
            log_message("ERROR", f"Required file not found: {file}")
            raise FileNotFoundError(f"Missing input file: {file}")
        
        if not os.access(file, os.R_OK):
            log_message("ERROR", f"File not readable: {file}")
            raise PermissionError(f"Cannot read file: {file}")
        
        log_message("INFO", f"Validated file: {file}")
    
    log_message("INFO", "All input files validated successfully")


def clean_customer_data():
    log_message("INFO", "Starting customer data cleaning process")

    input_file = os.path.join(DATA_DIR, "customer_data.csv")
    output_file = os.path.join(TEMP_DIR, "customer_data_cleaned.csv")
    reject_file = os.path.join(TEMP_DIR, "customer_data_rejected.csv")

    # Read the input file
    with open(input_file, "r") as f:
        rows = list(csv.reader(f))

    header = rows[0]
    data = rows[1:]

    cleaned_rows = []
    rejected_rows = []
    for row in data:
        customer_id, name, phone, email, reg_date, status, credit_limit = row
        # Handle NULLs
        customer_id = customer_id if customer_id not in ("", "NULL") else "UNKNOWN"
        name = name if name not in ("", "NULL") else "UNKNOWN_CUSTOMER"
        phone = phone if phone not in ("", "NULL") else "0000000000"
        email = email if email not in ("", "NULL") else "noemail@unknown.com"
        reg_date = reg_date if reg_date not in ("", "NULL") else "1900-01-01"
        status = status if status not in ("", "NULL") else "INACTIVE"
        credit_limit = credit_limit if credit_limit not in ("", "NULL") else "0"
        # Validate phone number (must be 10 digits)
        if not (phone.isdigit() and len(phone) == 10):
            rejected_rows.append(row)
            continue
        # Format date YYYYMMDD → YYYY-MM-DD
        if len(reg_date) == 8 and reg_date.isdigit():
            reg_date = f"{reg_date[0:4]}-{reg_date[4:6]}-{reg_date[6:8]}"
        cleaned_rows.append([
            customer_id, name, phone, email, reg_date, status, credit_limit
        ])
	# Write cleaned records
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "name", "phone", "email",
                         "registration_date", "status", "credit_limit"])
        writer.writerows(cleaned_rows)

    # Write rejected records (if any)
    if rejected_rows:
        with open(reject_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rejected_rows)

    # Logging summary
    log_message("INFO", f"Customer data cleaning completed - "
                        f"Input: {len(data)}, "
                        f"Output: {len(cleaned_rows)}, "
                        f"Rejected: {len(rejected_rows)}")


def deduplicate_usage_data():
    log_message("INFO", "Starting usage data deduplication")

    input_file = os.path.join(DATA_DIR, "usage_data.txt")
    output_file = os.path.join(TEMP_DIR, "usage_data_deduped.txt")

    # Read all lines
    with open(input_file, "r") as f:
        lines = f.readlines()

    input_count = len(lines)

    # Remove duplicates while preserving order
    seen = set()
    deduped = []
    for line in lines:
        if line not in seen:
            seen.add(line)
            deduped.append(line)

    # Write deduplicated data
    with open(output_file, "w") as f:
        f.writelines(deduped)

    output_count = len(deduped)
    duplicates_removed = input_count - output_count

    log_message("INFO", f"Deduplication completed - Removed {duplicates_removed} duplicates")
    log_message("INFO", f"Final usage records: {output_count}")

def aggregate_billing_data():
    log_message("INFO", "Starting billing data aggregation")

    input_file = os.path.join(DATA_DIR, "billing_records.txt")
    output_file = os.path.join(TEMP_DIR, "billing_aggregated.txt")

    totals = {}
    counts = {}
    mins = {}
    maxs = {}
    last_dates = {}

    # Read file
    with open(input_file, "r") as f:
        lines = f.readlines()

    header = lines[0]
    data = [line.strip().split("|") for line in lines[1:]]

    for row in data:
        customer_id = row[0]
        amount = float(row[1]) if row[1] not in ("", "NULL") else 0.0
        billing_date = row[2]

        # Total amount
        totals[customer_id] = totals.get(customer_id, 0) + amount

        # Count per customer
        counts[customer_id] = counts.get(customer_id, 0) + 1

        # Min
        if customer_id not in mins or amount < mins[customer_id]:
            mins[customer_id] = amount

        # Max
        if customer_id not in maxs or amount > maxs[customer_id]:
            maxs[customer_id] = amount

        # Last billing date (latest)
        if customer_id not in last_dates or billing_date > last_dates[customer_id]:
            last_dates[customer_id] = billing_date

    # Write output file
    with open(output_file, "w") as f:
        f.write("customer_id|total_amount|avg_amount|record_count|min_amount|max_amount|last_billing_date\n")

        for cust_id in totals:
            total = totals[cust_id]
            count = counts[cust_id]
            avg = total / count if count > 0 else 0
            f.write(f"{cust_id}|{total}|{avg}|{count}|{mins[cust_id]}|{maxs[cust_id]}|{last_dates[cust_id]}\n")

    log_message("INFO", f"Billing aggregation completed for {len(totals)} customers")

def create_final_report():
    log_message("INFO", "Creating final consolidated report")

    customer_file = os.path.join(TEMP_DIR, "customer_data_cleaned.csv")
    usage_file = os.path.join(TEMP_DIR, "usage_data_deduped.txt")
    billing_file = os.path.join(TEMP_DIR, "billing_aggregated.txt")
    final_report = os.path.join(OUTPUT_DIR, f"telecom_daily_report_{datetime.now().strftime('%Y%m%d')}.txt")

    with open(final_report, "w") as f:
        f.write(f"=== TELECOM DAILY ETL REPORT - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")

        # ---------------------------
        # Customer statistics
        # ---------------------------
        with open(customer_file, "r") as cf:
            reader = list(csv.reader(cf))
            total_customers = len(reader) - 1

            active = sum(1 for row in reader[1:] if row[5] == "ACTIVE")

        f.write("CUSTOMER DATA STATISTICS:\n")
        f.write(f"  - Total customers processed: {total_customers}\n")
        f.write(f"  - Active customers: {active}\n\n")

        # ---------------------------
        # Usage statistics
        # ---------------------------
        with open(usage_file, "r") as uf:
            lines = [line.strip() for line in uf.readlines()]
        
        # Remove header
        data_lines = lines[1:]
        
        # Total records
        total_usage_records = len(data_lines)
        
        # Sum data_usage_mb (3rd column, index 2)
        total_data_usage = 0.0
        for line in data_lines:
            parts = line.split("|")
            if len(parts) >= 3 and parts[2].replace('.', '', 1).isdigit():
                total_data_usage += float(parts[2])
        
        
            f.write("USAGE DATA STATISTICS:\n")
            f.write(f"  - Total usage records: {total_usage_records}\n")
            f.write(f"  - Total data usage (MB): {total_data_usage}\n\n")
    
        # ---------------------------
        # Billing statistics
        # ---------------------------
        with open(billing_file, "r") as bf:
            billing_lines = bf.readlines()
            total_billing_customers = len(billing_lines) - 1
            total_revenue = sum(float(line.split('|')[1]) for line in billing_lines[1:])

        f.write("BILLING DATA STATISTICS:\n")
        f.write(f"  - Customers with billing: {total_billing_customers}\n")
        f.write(f"  - Total revenue: {total_revenue:.2f}\n\n")

        # ---------------------------
        # Summary
        # ---------------------------
        f.write("PROCESSING SUMMARY:\n")
        f.write("  - Script: telecom_etl_pipeline (Python version)\n")
        f.write("  - Status: SUCCESS\n")
        f.write(f"  - Report generated at: {datetime.now()}\n")

    log_message("INFO", f"Final report created: {final_report}")



def main():

    log_message("INFO", "Starting Python ETL pipeline")
    validate_input_files()
    clean_customer_data()
    deduplicate_usage_data()
    aggregate_billing_data()
    create_final_report()

    # We will call all functions here later

    log_message("INFO", "ETL pipeline completed")

if __name__ == "__main__":
    main()

