#!/bin/bash
set -x

# Get the script's directory
export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source configuration files
. "$SCRIPT_DIR/config/database.properties"
. "$SCRIPT_DIR/config/process_status.conf"

# Set environment variables
export SCRIPT_NAME="telecom_etl_pipeline"
export LOG_DIR="$SCRIPT_DIR/logs"
export DATA_DIR="$SCRIPT_DIR/sample_data"
export OUTPUT_DIR="$SCRIPT_DIR/output"
export TEMP_DIR="$SCRIPT_DIR/temp"

# Create directories if they don't exist
mkdir -p $LOG_DIR $OUTPUT_DIR $TEMP_DIR

# Log file with timestamp
ETL_LOG_FILE="$LOG_DIR/telecom_etl_$(date +_%Y-%m-%d).log"
exec 2>>$ETL_LOG_FILE

####################################################################################
## Function: log_message
####################################################################################
log_message() {
    local level=$1
    local message=$2
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" | tee -a $ETL_LOG_FILE
}

####################################################################################
## Function: update_process_status
####################################################################################
update_process_status() {
    local flag_name=$1
    local flag_value=$2
    local status_file="$SCRIPT_DIR/config/process_status.conf"
    
    # Update process status flag (simulating sed commands from original scripts)
    if grep -q "$flag_name=" "$status_file"; then
        sed -i.bak "s/${flag_name}=.*/${flag_name}=${flag_value}/g" "$status_file"
    else
        echo "${flag_name}=${flag_value}" >> "$status_file"
    fi
    
    log_message "INFO" "Updated process status: $flag_name=$flag_value"
}

####################################################################################
## Function: validate_input_files
####################################################################################
validate_input_files() {
    log_message "INFO" "Validating input files..."
    
    local required_files=(
        "$DATA_DIR/customer_data.csv"
        "$DATA_DIR/usage_data.txt" 
        "$DATA_DIR/billing_records.txt"
    )
    
    for file in "${required_files[@]}"; do
        if [ ! -f "$file" ]; then
            log_message "ERROR" "Required file not found: $file"
            terminate_with_failure "Missing input file: $file"
        fi
        
        if [ ! -r "$file" ]; then
            log_message "ERROR" "File not readable: $file"
            terminate_with_failure "Cannot read file: $file"
        fi
        
        log_message "INFO" "Validated file: $file"
    done
    
    log_message "INFO" "All input files validated successfully"
}

####################################################################################
## Function: clean_customer_data
####################################################################################
clean_customer_data() {
    log_message "INFO" "Starting customer data cleaning process"
    update_process_status "ETL_ProcessStatusFlag" "1"
    update_process_status "ETL_ProcessName" "clean_customer_data"
    
    local input_file="$DATA_DIR/customer_data.csv"
    local output_file="$TEMP_DIR/customer_data_cleaned.csv"
    local reject_file="$TEMP_DIR/customer_data_rejected.csv"
    
    # Count input records
    local input_count=$(wc -l < "$input_file")
    log_message "INFO" "Processing $input_count customer records"
    
    # Clean data: handle nulls, validate phone numbers, standardize dates
    awk -F',' '
    BEGIN { OFS=","; print "customer_id,name,phone,email,registration_date,status,credit_limit" }
    NR > 1 {
        # Handle null values
        customer_id = ($1 == "" || $1 == "NULL") ? "UNKNOWN" : $1
        name = ($2 == "" || $2 == "NULL") ? "UNKNOWN_CUSTOMER" : $2
        phone = ($3 == "" || $3 == "NULL") ? "0000000000" : $3
        email = ($4 == "" || $4 == "NULL") ? "noemail@unknown.com" : $4
        reg_date = ($5 == "" || $5 == "NULL") ? "1900-01-01" : $5
        status = ($6 == "" || $6 == "NULL") ? "INACTIVE" : $6
        credit_limit = ($7 == "" || $7 == "NULL") ? 0 : $7
        
        # Validate phone number (must be 10 digits)
        if (length(phone) != 10 || phone !~ /^[0-9]+$/) {
            print customer_id,name,phone,email,reg_date,status,credit_limit >> "'$reject_file'"
        } else {
            # Format date (convert YYYYMMDD to YYYY-MM-DD)
            if (length(reg_date) == 8 && reg_date ~ /^[0-9]+$/) {
                reg_date = substr(reg_date,1,4) "-" substr(reg_date,5,2) "-" substr(reg_date,7,2)
            }
            print customer_id,name,phone,email,reg_date,status,credit_limit
        }
    }' "$input_file" > "$output_file"
    
    local output_count=$(wc -l < "$output_file")
    local reject_count=0
    if [ -f "$reject_file" ]; then
        reject_count=$(wc -l < "$reject_file")
    fi
    
    log_message "INFO" "Customer data cleaning completed - Input: $input_count, Output: $((output_count-1)), Rejected: $reject_count"
}

####################################################################################
## Function: deduplicate_usage_data  
####################################################################################
deduplicate_usage_data() {
    log_message "INFO" "Starting usage data deduplication"
    update_process_status "ETL_ProcessStatusFlag" "2"
    update_process_status "ETL_ProcessName" "deduplicate_usage_data"
    
    local input_file="$DATA_DIR/usage_data.txt"
    local output_file="$TEMP_DIR/usage_data_deduped.txt"
    
    local input_count=$(wc -l < "$input_file")
    log_message "INFO" "Processing $input_count usage records for deduplication"
    
    awk '!seen[$0]++' "$input_file" > "$output_file"
    
    local output_count=$(wc -l < "$output_file")
    local duplicates_removed=$((input_count - output_count))
    
    log_message "INFO" "Deduplication completed - Removed $duplicates_removed duplicate records"
    log_message "INFO" "Final usage records: $output_count"
}

####################################################################################
## Function: aggregate_billing_data
####################################################################################
aggregate_billing_data() {
    log_message "INFO" "Starting billing data aggregation"
    update_process_status "ETL_ProcessStatusFlag" "3" 
    update_process_status "ETL_ProcessName" "aggregate_billing_data"
    
    local input_file="$DATA_DIR/billing_records.txt"
    local output_file="$TEMP_DIR/billing_aggregated.txt"
    
    # Complex aggregation with multiple calculations
    awk -F'|' '
    BEGIN { 
        OFS="|"
        print "customer_id|total_amount|avg_amount|record_count|min_amount|max_amount|last_billing_date"
    }
    NR > 1 {
        customer_id = $1
        amount = ($2 == "" || $2 == "NULL") ? 0 : $2
        billing_date = $3
        
        # Aggregate calculations
        total[customer_id] += amount
        count[customer_id]++
        
        if (min[customer_id] == "" || amount < min[customer_id]) {
            min[customer_id] = amount
        }
        if (max[customer_id] == "" || amount > max[customer_id]) {
            max[customer_id] = amount  
        }
        if (last_date[customer_id] == "" || billing_date > last_date[customer_id]) {
            last_date[customer_id] = billing_date
        }
    }
    END {
        for (cust_id in total) {
            avg = count[cust_id] > 0 ? total[cust_id] / count[cust_id] : 0
            print cust_id, total[cust_id], avg, count[cust_id], min[cust_id], max[cust_id], last_date[cust_id]
        }
    }' "$input_file" > "$output_file"
    
    local customer_count=$(wc -l < "$output_file")
    log_message "INFO" "Billing aggregation completed for $((customer_count-1)) customers"
}

####################################################################################
## Function: create_final_report
####################################################################################
create_final_report() {
    log_message "INFO" "Creating final consolidated report"
    update_process_status "ETL_ProcessStatusFlag" "4"
    update_process_status "ETL_ProcessName" "create_final_report"
    
    local customer_file="$TEMP_DIR/customer_data_cleaned.csv"
    local usage_file="$TEMP_DIR/usage_data_deduped.txt"
    local billing_file="$TEMP_DIR/billing_aggregated.txt"
    local final_report="$OUTPUT_DIR/telecom_daily_report_$(date +%Y%m%d).txt"
    
    # Create header for final report
    echo "=== TELECOM DAILY ETL REPORT - $(date '+%Y-%m-%d %H:%M:%S') ===" > "$final_report"
    echo "" >> "$final_report"
    
    # Customer statistics
    local customer_count=$(wc -l < "$customer_file")
    echo "CUSTOMER DATA STATISTICS:" >> "$final_report"
    echo "  - Total customers processed: $((customer_count-1))" >> "$final_report"
    echo "  - Active customers: $(awk -F',' 'NR>1 && $6=="ACTIVE" {count++} END {print count+0}' "$customer_file")" >> "$final_report"
    echo "" >> "$final_report"
    
    # Usage statistics  
    local usage_count=$(wc -l < "$usage_file")
    echo "USAGE DATA STATISTICS:" >> "$final_report"
    echo "  - Total usage records: $usage_count" >> "$final_report"
    echo "  - Total data usage (MB): $(awk '{sum+=$3} END {print sum+0}' "$usage_file")" >> "$final_report"
    echo "" >> "$final_report"
    
    # Billing statistics
    local billing_customer_count=$(wc -l < "$billing_file")
    echo "BILLING DATA STATISTICS:" >> "$final_report" 
    echo "  - Customers with billing: $((billing_customer_count-1))" >> "$final_report"
    echo "  - Total revenue: $(awk -F'|' 'NR>1 {sum+=$2} END {printf "%.2f", sum+0}' "$billing_file")" >> "$final_report"
    echo "" >> "$final_report"
    
    # Processing summary
    echo "PROCESSING SUMMARY:" >> "$final_report"
    echo "  - Script: $SCRIPT_NAME" >> "$final_report"
    echo "  - Start time: $(head -1 $ETL_LOG_FILE | awk '{print $1, $2}')" >> "$final_report"
    echo "  - End time: $(date '+%Y-%m-%d %H:%M:%S')" >> "$final_report"
    echo "  - Status: SUCCESS" >> "$final_report"
    
    log_message "INFO" "Final report created: $final_report"
}

####################################################################################
## Function: cleanup_temp_files
####################################################################################
cleanup_temp_files() {
    log_message "INFO" "Cleaning up temporary files"
    
    if [ -d "$TEMP_DIR" ]; then
        rm -f "$TEMP_DIR"/*
        log_message "INFO" "Temporary files cleaned up"
    fi
}

####################################################################################
## Function: terminate_with_success
####################################################################################
terminate_with_success() {
    update_process_status "ETL_ProcessStatusFlag" "0"
    update_process_status "ETL_ProcessName" "ETL_COMPLETED"
    update_process_status "ETL_JobStatus" "SUCCESS"
    update_process_status "ETL_JobRunTime" "$(date '+%Y-%m-%d %H:%M:%S')"
    
    log_message "INFO" "Script $SCRIPT_NAME terminated successfully at $(date '+%Y-%m-%d %H:%M:%S')"
    log_message "INFO" "END TIME: $(date)"
    log_message "INFO" "******************************************"
    
    cleanup_temp_files
    exit 0
}

####################################################################################
## Function: terminate_with_failure
####################################################################################
terminate_with_failure() {
    local error_message="$1"
    
    update_process_status "ETL_JobStatus" "FAILURE" 
    update_process_status "ETL_JobRunTime" "$(date '+%Y-%m-%d %H:%M:%S')"
    
    log_message "ERROR" "Script $SCRIPT_NAME terminated unsuccessfully at $(date '+%Y-%m-%d %H:%M:%S')"
    log_message "ERROR" "Error: $error_message"
    
    # In real implementation, this would send email notifications
    log_message "INFO" "Email notification would be sent to admin team"
    
    cleanup_temp_files
    exit 1
}

####################################################################################
## Function: check_previous_process
####################################################################################
check_previous_process() {
    local pid_file="$SCRIPT_DIR/config/${SCRIPT_NAME}.pid"
    
    if [ -f "$pid_file" ]; then
        local previous_pid=$(cat "$pid_file")
        if ps -p "$previous_pid" > /dev/null 2>&1; then
            log_message "WARNING" "Previous process (PID: $previous_pid) is still running"
            log_message "INFO" "Exiting to avoid concurrent execution"
            exit 0
        else
            log_message "INFO" "Previous process (PID: $previous_pid) is no longer running"
            rm -f "$pid_file"
        fi
    fi
    
    # Write current PID
    echo $$ > "$pid_file"
    log_message "INFO" "Process started with PID: $$"
}

####################################################################################
## MAIN PROGRAM EXECUTION
####################################################################################

log_message "INFO" "******************************************"
log_message "INFO" "START TIME: $(date)"
log_message "INFO" "Starting telecom ETL pipeline processing"

check_previous_process

# Trap to handle script interruption
trap 'terminate_with_failure "Script interrupted"' INT TERM

try_execution() {
    # Execute ETL steps in sequence
    validate_input_files
    clean_customer_data
    deduplicate_usage_data  
    aggregate_billing_data
    create_final_report
    
    terminate_with_success
}

# Execute with error handling
if ! try_execution; then
    terminate_with_failure "ETL pipeline execution failed"
fi