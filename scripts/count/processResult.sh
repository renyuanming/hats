#!/bin/bash


# The reuslt file for plot
AVG_READ_LATENCY_RES="avgRead.txt"
TAIL_READ_LATENCY_RES="tailRead.txt"
AVG_UPDATE_LATENCY_RES="avgUpdate.txt"
TAIL_UPDATE_LATENCY_RES="tailUpdate.txt"
OVERALL_THG_RES="throughput.txt"
CPU_TIME_RES="cpuTime.txt"
DISK_READ_IO_RES="diskReadIO.txt"
DISK_WRITE_IO_RES="diskWriteIO.txt"
AVG_SCAN_LATENCY_RES="avgScan.txt"
TAIL_SCAN_LATENCY_RES="tailScan.txt"
AVG_INSERT_LATENCY_RES="avgInsert.txt"
TAIL_INSERT_LATENCY_RES="tailInsert.txt"

TITLE_LINE="Type        CL      Mean        SD"



function extract_value() {
    if [[ $1 == *"count"* ]]; then
        awk -v pattern="$1" '$0 ~ pattern {print $NF}' "$2"
    else
        awk -v pattern="$1" '$0 ~ pattern {printf "%.2f\n", $NF}' "$2"
    fi
    # awk -v pattern="$1" '$0 ~ pattern {print $NF}' "$2"
    # awk -v pattern="$1" '$0 ~ pattern {printf "%.2f\n", $NF}' "$2"
}



function initResultFiles() {
    run_dir=$1

    AVG_READ_LATENCY_RES="${run_dir}/${AVG_READ_LATENCY_RES}"
    TAIL_READ_LATENCY_RES="${run_dir}/${TAIL_READ_LATENCY_RES}"
    AVG_UPDATE_LATENCY_RES="${run_dir}/${AVG_UPDATE_LATENCY_RES}"
    TAIL_UPDATE_LATENCY_RES="${run_dir}/${TAIL_UPDATE_LATENCY_RES}"
    OVERALL_THG_RES="${run_dir}/${OVERALL_THG_RES}"
    CPU_TIME_RES="${run_dir}/${CPU_TIME_RES}"
    DISK_READ_IO_RES="${run_dir}/${DISK_READ_IO_RES}"
    DISK_WRITE_IO_RES="${run_dir}/${DISK_WRITE_IO_RES}"
    AVG_SCAN_LATENCY_RES="${run_dir}/${AVG_SCAN_LATENCY_RES}"
    TAIL_SCAN_LATENCY_RES="${run_dir}/${TAIL_SCAN_LATENCY_RES}"
    AVG_INSERT_LATENCY_RES="${run_dir}/${AVG_INSERT_LATENCY_RES}"
    TAIL_INSERT_LATENCY_RES="${run_dir}/${TAIL_INSERT_LATENCY_RES}"

    > "$AVG_READ_LATENCY_RES" && echo "$TITLE_LINE" > "$AVG_READ_LATENCY_RES"
    > "$TAIL_READ_LATENCY_RES" && echo "$TITLE_LINE" > "$TAIL_READ_LATENCY_RES"
    > "$AVG_UPDATE_LATENCY_RES" && echo "$TITLE_LINE" > "$AVG_UPDATE_LATENCY_RES"
    > "$TAIL_UPDATE_LATENCY_RES" && echo "$TITLE_LINE" > "$TAIL_UPDATE_LATENCY_RES"
    > "$OVERALL_THG_RES" && echo "$TITLE_LINE" > "$OVERALL_THG_RES"
    > "$CPU_TIME_RES" && echo "$TITLE_LINE" > "$CPU_TIME_RES"
    > "$DISK_READ_IO_RES" && echo "$TITLE_LINE" > "$DISK_READ_IO_RES"
    > "$DISK_WRITE_IO_RES" && echo "$TITLE_LINE" > "$DISK_WRITE_IO_RES"
    > "$AVG_SCAN_LATENCY_RES" && echo "$TITLE_LINE" > "$AVG_SCAN_LATENCY_RES"
    > "$TAIL_SCAN_LATENCY_RES" && echo "$TITLE_LINE" > "$TAIL_SCAN_LATENCY_RES"
    > "$AVG_INSERT_LATENCY_RES" && echo "$TITLE_LINE" > "$AVG_INSERT_LATENCY_RES"
    > "$TAIL_INSERT_LATENCY_RES" && echo "$TITLE_LINE" > "$TAIL_INSERT_LATENCY_RES"
}

function process_directory() {
    local run_dir=$1

    # Initialize the output files
    initResultFiles $run_dir

    for scheme_dir in "$run_dir"/*/; do
        echo "Processing directory: $scheme_dir"
        local scheme_name=$(basename "$scheme_dir")
        process_sub_directory "$scheme_dir" "$scheme_name"
        
    done

}

function process_sub_directory {


    local scheme_dir=$1
    local scheme_name=$2
    declare -A overall_data

    for round_dir in "$scheme_dir"/*/; do
        round=$(basename "$round_dir")
        declare -A round_data


        disk_read_io_of_all_nodes=()
        disk_write_io_of_all_nodes=()
        user_time_of_all_nodes=()
        sys_time_of_all_nodes=()
        local_read_count_of_all_nodes=()
        local_read_latency_of_all_nodes=()
        coordinator_read_count_of_all_nodes=()
        coordinator_read_latency_of_all_nodes=()
        local_write_count_of_all_nodes=()
        local_write_latency_of_all_nodes=()
        local_scan_count_of_all_nodes=()
        local_scan_latency_of_all_nodes=()
        coordinator_scan_count_of_all_nodes=()
        coordinator_scan_latency_of_all_nodes=()

        for node_dir in "$round_dir"node*/; do
            node_name=$(basename "$node_dir")
            # echo "Processing $node_dir"
            
            # Get the CPU 
            cpu_file="$node_dir/metrics/After-normal-run_cpu_summary.txt"
            uptime=$(printf "%.0f" $(extract_value "Process uptime" "$cpu_file"))
            user_time=$(printf "%.0f" $(extract_value "User time" "$cpu_file"))
            sys_time=$(printf "%.0f" $(extract_value "System time" "$cpu_file"))

            # Get the disk I/O
            disk_io_file_after="$node_dir/metrics/After-normal-run_disk_io_total.txt"
            disk_io_file_before="$node_dir/metrics/Before-run_disk_io_total.txt"

            after_read=$(extract_value "Disk KiB read" "$disk_io_file_after")
            before_read=$(extract_value "Disk KiB read" "$disk_io_file_before")

            after_read=$(printf "%f" $after_read)
            before_read=$(printf "%f" $before_read)
            
            disk_read_io=$(echo "scale=0; (${after_read:-0} - ${before_read:-0}) / 1024" | bc)

            after_written=$(extract_value "Disk KiB written" "$disk_io_file_after")
            before_written=$(extract_value "Disk KiB written" "$disk_io_file_before")
            disk_write_io=$(echo "scale=0; (${after_written:-0} - ${before_written:-0}) / 1024" | bc)

            # echo "=======================Disk I/O diff=================================="
            # echo "Disk I/O Difference for $node_name:"
            # echo "Read Difference (MiB): $disk_read_io"
            # echo "Write Difference (MiB): $disk_write_io"
            # echo "======================================================================"


            # Get the network I/O
            network_file_after="$node_dir/metrics/After-normal-run_network_summary.txt"
            network_file_before="$node_dir/metrics/Before-run_network_summary.txt"

            after_received=$(extract_value "Bytes received" "$network_file_after")
            before_received=$(extract_value "Bytes received" "$network_file_before")
            network_recv_io=$(echo "scale=0; (${after_received:-0} - ${before_received:-0}) / 1048576" | bc)

            after_sent=$(extract_value "Bytes sent" "$network_file_after")
            before_sent=$(extract_value "Bytes sent" "$network_file_before")
            network_send_io=$(echo "scale=0; (${after_sent:-0} - ${before_sent:-0}) / 1048576" | bc)

            # Get the read and write counts
            # db_stats_file_after="$node_dir/metrics/After-normal-run_db_stats.txt"
            # db_stats_file_before="$node_dir/metrics/Before-run_db_stats.txt"

            # after_reads=$(extract_value "Local read count" "$db_stats_file_after")
            # before_reads=$(extract_value "Local read count" "$db_stats_file_before")
            # read_count=$(echo "scale=0; (${after_reads:-0} - ${before_reads:-0})" | bc)

            # after_writes=$(extract_value "Local write count" "$db_stats_file_after")
            # before_writes=$(extract_value "Local write count" "$db_stats_file_before")
            # write_count=$(echo "scale=0; (${after_writes:-0} - ${before_writes:-0})" | bc)

            break_down_file="$node_dir/metrics/breakdown.txt"
            local_read_count=$(extract_value "Local read count" "$break_down_file")
            local_read_latency=$(extract_value "Local read latency" "$break_down_file")
            coordinator_read_count=$(extract_value "Coordinator read count" "$break_down_file")
            coordinator_read_latency=$(extract_value "Coordinator read latency" "$break_down_file")
            local_write_count=$(extract_value "Local write count" "$break_down_file")
            local_write_latency=$(extract_value "Local write latency" "$break_down_file")
            local_scan_count=$(extract_value "Local range count" "$break_down_file")
            local_scan_latency=$(extract_value "Local range latency" "$break_down_file")
            coordinator_scan_count=$(extract_value "Coordinator scan count" "$break_down_file")
            coordinator_scan_latency=$(extract_value "Coordinator scan latency" "$break_down_file")




            eval "round_data[$node_name,node_name]=\"$node_name\""
            eval "round_data[$node_name,uptime]+=\"$uptime \""
            eval "round_data[$node_name,user_time]+=\"$user_time \""
            eval "round_data[$node_name,sys_time]+=\"$sys_time \""
            eval "round_data[$node_name,disk_read_io]+=\"$disk_read_io \""
            eval "round_data[$node_name,disk_write_io]+=\"$disk_write_io \""
            eval "round_data[$node_name,network_recv_io]+=\"$network_recv_io \""
            eval "round_data[$node_name,network_send_io]+=\"$network_send_io \""
            # eval "round_data[$node_name,read_count]+=\"s"$write_count \""
            eval "round_data[$node_name,local_read_count]+=\"$local_read_count \""
            eval "round_data[$node_name,local_read_latency]+=\"$local_read_latency \""
            eval "round_data[$node_name,coordinator_read_count]+=\"$coordinator_read_count \""
            eval "round_data[$node_name,coordinator_read_latency]+=\"$coordinator_read_latency \""
            eval "round_data[$node_name,local_write_count]+=\"$local_write_count \""
            eval "round_data[$node_name,local_write_latency]+=\"$local_write_latency \""
            eval "round_data[$node_name,local_scan_count]+=\"$local_scan_count \""
            eval "round_data[$node_name,local_scan_latency]+=\"$local_scan_latency \""
            eval "round_data[$node_name,coordinator_scan_count]+=\"$coordinator_scan_count \""
            eval "round_data[$node_name,coordinator_scan_latency]+=\"$coordinator_scan_latency \""

            user_time_of_all_nodes+=("$user_time")
            sys_time_of_all_nodes+=("$sys_time")
            disk_read_io_of_all_nodes+=("$disk_read_io")
            disk_write_io_of_all_nodes+=("$disk_write_io")
            local_read_count_of_all_nodes+=("$local_read_count")
            local_read_latency_of_all_nodes+=("$local_read_latency")
            coordinator_read_count_of_all_nodes+=("$coordinator_read_count")
            coordinator_read_latency_of_all_nodes+=("$coordinator_read_latency")
            local_write_count_of_all_nodes+=("$local_write_count")
            local_write_latency_of_all_nodes+=("$local_write_latency")
            local_scan_count_of_all_nodes+=("$local_scan_count")
            local_scan_latency_of_all_nodes+=("$local_scan_latency")
            coordinator_scan_count_of_all_nodes+=("$coordinator_scan_count")
            coordinator_scan_latency_of_all_nodes+=("$coordinator_scan_latency")
        done

        # Get the average read latency
        # average_read_latency=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[READ\], AverageLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        average_read_latency=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[READ\], AverageLatency(us), " "{}" | awk -F ", " "{printf \"%.0f\", \$3}"' \;)
        echo "Average Read Latency: $average_read_latency"
        meidan_read_latency=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[READ\], MedianLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        tail_read_latency_75th=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[READ\], 75thPercentileLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        tail_read_latency_99th=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[READ\], 99thPercentileLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        tail_read_latency_999th=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[READ\], 999thPercentileLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        average_update_latency=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[UPDATE\], AverageLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        tail_update_latency_99th=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[UPDATE\], 99thPercentileLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        average_scan_latency=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[SCAN\], AverageLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        tail_scan_latency_99th=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[SCAN\], 99thPercentileLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        # average_scan_latency=$(echo "scale=0; $average_scan_latency" | bc)
        # tail_scan_latency_99th=$(echo "scale=0; $tail_scan_latency_99th" | bc)
        average_insert_latency=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[INSERT\], AverageLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        tail_insert_latency_99th=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[INSERT\], 99thPercentileLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        overall_runtime=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[OVERALL\], RunTime(ms), " "{}" | awk -F ", " "{print \$3}"' \;)
        overall_throughput=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[OVERALL\], Throughput(ops/sec), " "{}" | awk -F ", " "{print \$3}"' \;)
        avg_user_time=$(echo "scale=0; ($(IFS=+; echo "(${user_time_of_all_nodes[*]})") / ${#user_time_of_all_nodes[@]})" | bc)
        avg_sys_time=$(echo "scale=0; ($(IFS=+; echo "(${sys_time_of_all_nodes[*]})") / ${#sys_time_of_all_nodes[@]})" | bc)
        avg_disk_read_io=$(echo "scale=0; ($(IFS=+; echo "(${disk_read_io_of_all_nodes[*]})") / ${#disk_read_io_of_all_nodes[@]})" | bc)
        avg_disk_write_io=$(echo "scale=0; ($(IFS=+; echo "(${disk_write_io_of_all_nodes[*]})") / ${#disk_write_io_of_all_nodes[@]})" | bc)
        average_local_read_latency=$(calculate_arithmetic_mean local_read_count_of_all_nodes[@] local_read_latency_of_all_nodes[@])
        echo "Average Local Read Latency: $average_local_read_latency"
        average_coordinator_read_latency=$(calculate_arithmetic_mean coordinator_read_count_of_all_nodes[@] coordinator_read_latency_of_all_nodes[@])
        # echo "Average Coordinator Read Latency: $average_coordinator_read_latency"
        average_local_write_latency=$(calculate_arithmetic_mean local_write_count_of_all_nodes[@] local_write_latency_of_all_nodes[@])
        # echo "Average Local Write Latency: $average_local_write_latency"

        average_local_scan_latency=$(calculate_arithmetic_mean local_scan_count_of_all_nodes[@] local_scan_latency_of_all_nodes[@])
        # echo "Average Local Scan Latency: $average_local_scan_latency"
        average_coordinator_scan_latency=$(calculate_arithmetic_mean coordinator_scan_count_of_all_nodes[@] coordinator_scan_latency_of_all_nodes[@])


        # replica_selection_cost=$(bc <<< "scale=0; $average_coordinator_read_latency - $average_local_read_latency")
        replica_selection_cost=$(echo "$average_coordinator_read_latency - $average_local_read_latency" | bc)
        echo "Replica Selection Cost: $replica_selection_cost"
        # read_network_cost=$(bc <<< "$average_read_latency - $average_local_read_latency")
        echo "Average Read Latency: $average_read_latency, Average Local Read Latency: $average_local_read_latency"
        read_network_cost=$(echo "${average_read_latency} - ${average_coordinator_read_latency}" | bc)
        echo "Read Network Cost: $read_network_cost"

        eval "round_data[average_read_latency]=\"$average_read_latency \""
        eval "rount_data[median_read_latency]=\"$meidan_read_latency \""
        eval "round_data[tail_read_latency_75th]=\"$tail_read_latency_75th \""
        eval "round_data[tail_read_latency_99th]=\"$tail_read_latency_99th \""
        eval "round_data[tail_read_latency_999th]=\"$tail_read_latency_999th \""
        eval "round_data[average_update_latency]=\"$average_update_latency \""
        eval "round_data[tail_update_latency_99th]=\"$tail_update_latency_99th \""
        eval "round_data[average_scan_latency]=\"$average_scan_latency \""
        eval "round_data[tail_scan_latency_99th]=\"$tail_scan_latency_99th \""
        eval "round_data[average_insert_latency]=\"$average_insert_latency \""
        eval "round_data[tail_insert_latency_99th]=\"$tail_insert_latency_99th \""
        eval "round_data[overall_runtime]=\"$overall_runtime \""
        eval "round_data[overall_throughput]=\"$overall_throughput \""
        eval "round_data[average_user_time]=\"$avg_user_time\""
        eval "round_data[average_sys_time]=\"$avg_sys_time\""
        eval "round_data[average_disk_read_io]=\"$avg_disk_read_io\""
        eval "round_data[average_disk_write_io]=\"$avg_disk_write_io\""
        eval "round_data[average_local_read_latency]=\"$average_local_read_latency\""
        eval "round_data[average_coordinator_read_latency]=\"$average_coordinator_read_latency\""
        eval "round_data[average_local_write_latency]=\"$average_local_write_latency\""
        eval "round_data[average_local_scan_latency]=\"$average_local_scan_latency\""
        eval "round_data[average_coordinator_scan_latency]=\"$average_coordinator_scan_latency\""
        eval "round_data[read_network_cost]=\"$read_network_cost\""
        eval "round_data[replica_selection_cost]=\"$replica_selection_cost\""

        eval "overall_data[$round,average_read_latency]=\"$average_read_latency \""
        eval "overall_data[$round,median_read_latency]=\"$meidan_read_latency \""
        eval "overall_data[$round,tail_read_latency_75th]=\"$tail_read_latency_75th \""
        eval "overall_data[$round,tail_read_latency_99th]=\"$tail_read_latency_99th \""
        eval "overall_data[$round,tail_read_latency_999th]=\"$tail_read_latency_999th \""
        eval "overall_data[$round,average_update_latency]=\"$average_update_latency \""
        eval "overall_data[$round,tail_update_latency_99th]=\"$tail_update_latency_99th \""
        eval "overall_data[$round,average_scan_latency]=\"$average_scan_latency \""
        eval "overall_data[$round,tail_scan_latency_99th]=\"$tail_scan_latency_99th \""
        eval "overall_data[$round,average_insert_latency]=\"$average_insert_latency \""
        eval "overall_data[$round,tail_insert_latency_99th]=\"$tail_insert_latency_99th \""
        eval "overall_data[$round,overall_runtime]=\"$overall_runtime \""
        eval "overall_data[$round,overall_throughput]=\"$overall_throughput \""
        eval "overall_data[$round,average_user_time]=\"$avg_user_time\""
        eval "overall_data[$round,average_sys_time]=\"$avg_sys_time\""
        eval "overall_data[$round,average_disk_read_io]=\"$avg_disk_read_io\""
        eval "overall_data[$round,average_disk_write_io]=\"$avg_disk_write_io\""
        eval "overall_data[$round,average_local_read_latency]=\"$average_local_read_latency\""
        eval "overall_data[$round,average_coordinator_read_latency]=\"$average_coordinator_read_latency\""
        eval "overall_data[$round,average_local_write_latency]=\"$average_local_write_latency\""
        eval "overall_data[$round,average_local_scan_latency]=\"$average_local_scan_latency\""
        eval "overall_data[$round,average_coordinator_scan_latency]=\"$average_coordinator_scan_latency\""
        eval "overall_data[$round,replica_selection_cost]=\"$replica_selection_cost\""
        eval "overall_data[$round,read_network_cost]=\"$read_network_cost\""
        eval "overall_data[$round,round]=\"$round\""

        # Output the results for each round
        print_round_results round_data $round "$scheme_dir"
        unset round_data

    done

    # Output the overall results
    print_overall_results overall_data $scheme_name

    

}


calculate_arithmetic_mean() {
    local count_array=("${!1}")  # Array containing counts
    local value_array=("${!2}")  # Array containing values
    # echo "Count Array: ${count_array[@]}"
    # echo "Value Array: ${value_array[@]}"

    local num_attributes="${#count_array[@]}"
    
    if [ "$num_attributes" -ne "${#value_array[@]}" ]; then
        echo "Error: Number of elements in the arrays do not match."
        # return 1
    fi

    local total_sum=0
    local total_count=0

    # Iterate through each attribute
    for ((i=0; i<num_attributes; i++)); do
        local count="${count_array[$i]}"
        local value="${value_array[$i]}"

        # Check if count is a valid number
        if ! [[ "$count" =~ ^[0-9]+$ ]]; then
            echo "Error: Invalid count at index $i, count is $count."
            return 0
        fi

        if ! [[ "$value" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
            value=0
            # echo "Invalid value at index $i. Setting it to 0."
            return 0
        fi

        # Calculate the sum for each attribute
        local sum=$(bc <<< "scale=0; $count * $value")
        total_sum=$(bc <<< "scale=0; $total_sum + $sum" )
        
        # Increment total count
        total_count=$(( total_count + count ))
    done

    # Check if total_count is 0 to avoid division by zero
    if [ "$total_count" -eq 0 ]; then
        echo "Error: Total count is zero.###############################################################"
        return 0
    fi

    # Calculate the arithmetic mean
    local arithmetic_mean=$(bc <<< "scale=0; $total_sum / $total_count")
    echo "$arithmetic_mean"
}



function get_standard_deviation() {
    local -a arr=("${@:2}")
    local n=${#arr[@]}
    local mean=$1
    

    local sum_diff_sq=0
    for num in "${arr[@]}"; do
        local diff=$(bc <<< "scale=0; ($num - $mean)^2")
        sum_diff_sq=$(bc <<< "scale=0; $sum_diff_sq + $diff")
    done
    standard_deviation=$(bc <<< "scale=0; sqrt($sum_diff_sq / ($n - 1))")
    
    echo "$standard_deviation"
}

function print_overall_results() {

    local -n data=$1
    local scheme_name=$2
    local output_file="${scheme_name}_results.csv"

    local type=""
    local cl=""

    if echo "$scheme_name" | grep -q "RunA"; then
        type="Scatter"
        cl="Zero"
    elif echo "$scheme_name" | grep -q "RunB"; then
        type="Centralized"
        cl="Zero"
    elif echo "$scheme_name" | grep -q "RunC"; then
        type="Scatter"
        cl="One"
    elif echo "$scheme_name" | grep -q "RunD"; then
        type="Centralized"
        cl="One"
    elif echo "$scheme_name" | grep -q "RunE"; then
        type="Scatter"
        cl="All"
    elif echo "$scheme_name" | grep -q "RunF"; then
        type="Centralized"
        cl="All"
    fi



    # sort the rounds
    local -a rounds=()
    for round_key in "${!data[@]}"; do
        if [[ $round_key == *",round" ]]; then
            rounds+=("${data[$round_key]}")
        fi
    done
    IFS=$'\n' sorted_rounds=($(sort <<<"${rounds[*]}"))
    unset IFS

    echo "Printing the sorted rounds ${sorted_rounds[@]}"

    # clean the output file
    > "$output_file"

    echo -n "Metrics," >> "$output_file"
    echo -n "Mean," >> "$output_file"
    echo -n "Student's t Error," >> "$output_file"
    echo -n "Lower Bound," >> "$output_file"
    echo -n "Upper Bound," >> "$output_file"
    echo "" >> "$output_file"

    # "average_read_latency" "tail_read_latency_99th" "average_update_latency" "tail_update_latency_99th" "overall_runtime" "overall_throughput" "average_user_time" "average_sys_time" "average_disk_read_io" "average_disk_write_io" "average_local_read_latency" "average_coordinator_read_latency" "average_local_write_latency" "replica_selection_cost" "read_network_cost"
    # "average_read_latency" "tail_read_latency_99th" "overall_runtime" "overall_throughput" "average_user_time" "average_sys_time" "average_disk_read_io" "average_disk_write_io" "average_local_read_latency" "average_coordinator_read_latency" "replica_selection_cost" "read_network_cost"
    # calculate the student t distribution for 95% confidence interval
    for key in "average_read_latency" "median_read_latency" "tail_read_latency_75th" "tail_read_latency_99th" "tail_read_latency_999th" "average_update_latency" "tail_update_latency_99th" "overall_runtime" "overall_throughput" "average_user_time" "average_sys_time" "average_disk_read_io" "average_disk_write_io" "average_local_read_latency" "average_coordinator_read_latency" "average_local_write_latency" "average_local_scan_latency" "average_coordinator_scan_latency" "replica_selection_cost" "read_network_cost" "average_scan_latency" "tail_scan_latency_99th" "average_insert_latency" "tail_insert_latency_99th"; do
        metrics=()
        for round in "${sorted_rounds[@]}"; do
            echo "Round: $round, Key: $key, value: ${data[$round,$key]}"
            metrics+=("${data[$round,$key]}")
        done

        mean=$(echo "scale=0; ($(IFS=+; echo "(${metrics[*]})") / ${#metrics[@]})" | bc)
        # echo "Mean for $key: $mean"
        standard_deviation=$(get_standard_deviation "${metrics[@]}" $mean)
        # echo "Standard Deviation for $key: $standard_deviation"
        standard_error=$(echo "scale=0; ${standard_deviation} / sqrt(${#metrics[@]})" | bc)
        # echo "Standard Error for $key: $standard_error"


        degrees_of_freedom=$((${#metrics[@]} - 1))
        # echo "Degrees of Freedom for $key: $degrees_of_freedom"
        t_distribution=$(Rscript -e "qt(0.975, df=$degrees_of_freedom)" | awk '{print $2}')
        # echo "T Distribution for $key: $t_distribution"
        t_error=$(echo "scale=0; $t_distribution * $standard_error" | bc)
        # echo "T Error for $key: $t_error"


        lower_bound=$(echo "scale=0; $mean - ${t_error}" | bc)
        # echo "Lower Bound for $key: $lower_bound"
        upper_bound=$(echo "scale=0; $mean + ${t_error}" | bc)
        # echo "Upper Bound for $key: $upper_bound"

        echo -n "$key," >> "$output_file"
        echo -n "$mean," >> "$output_file"
        echo -n "$t_error," >> "$output_file"
        echo -n "$lower_bound," >> "$output_file"
        echo -n "$upper_bound," >> "$output_file"

        echo "" >> "$output_file"

        output_to_res_file $key $type $cl $mean $t_error

    done
}


function output_to_res_file {
    key=$1
    type=$2
    cl=$3
    mean=$4
    t_error=$5

    if [ $key == "average_read_latency" ]; then
        echo -n "" >> "$AVG_READ_LATENCY_RES"
        echo -n "$type    " >> "$AVG_READ_LATENCY_RES"
        echo -n "$cl    " >> "$AVG_READ_LATENCY_RES"
        echo -n "$mean    " >> "$AVG_READ_LATENCY_RES"
        echo -n "$t_error    " >> "$AVG_READ_LATENCY_RES"
        echo "" >> "$AVG_READ_LATENCY_RES"
    elif [ $key == "median_read_latency" ]; then
        echo -n "" >> "$AVG_READ_LATENCY_RES"
        echo -n "$type    " >> "$AVG_READ_LATENCY_RES"
        echo -n "$cl    " >> "$AVG_READ_LATENCY_RES"
        echo -n "$mean    " >> "$AVG_READ_LATENCY_RES"
        echo -n "$t_error    " >> "$AVG_READ_LATENCY_RES"
        echo "" >> "$AVG_READ_LATENCY_RES"
    elif [ $key == "tail_read_latency_75th" ]; then
        echo -n "" >> "$TAIL_READ_LATENCY_RES"
        echo -n "$type    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$cl    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$mean    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$t_error    " >> "$TAIL_READ_LATENCY_RES"
        echo "" >> "$TAIL_READ_LATENCY_RES"
    elif [ $key == "tail_read_latency_99th" ]; then
        echo -n "" >> "$TAIL_READ_LATENCY_RES"
        echo -n "$type    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$cl    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$mean    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$t_error    " >> "$TAIL_READ_LATENCY_RES"
        echo "" >> "$TAIL_READ_LATENCY_RES"
    elif [ $key == "tail_read_latency_999th" ]; then
        echo -n "" >> "$TAIL_READ_LATENCY_RES"
        echo -n "$type    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$cl    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$mean    " >> "$TAIL_READ_LATENCY_RES"
        echo -n "$t_error    " >> "$TAIL_READ_LATENCY_RES"
        echo "" >> "$TAIL_READ_LATENCY_RES"
    elif [ $key == "average_update_latency" ]; then
        echo -n "" >> "$AVG_UPDATE_LATENCY_RES"
        echo -n "$type    " >> "$AVG_UPDATE_LATENCY_RES"
        echo -n "$cl    " >> "$AVG_UPDATE_LATENCY_RES"
        echo -n "$mean    " >> "$AVG_UPDATE_LATENCY_RES"
        echo -n "$t_error    " >> "$AVG_UPDATE_LATENCY_RES"
        echo "" >> "$AVG_UPDATE_LATENCY_RES"
    elif [ $key == "tail_update_latency_99th" ]; then
        echo -n "" >> "$TAIL_UPDATE_LATENCY_RES"
        echo -n "$type    " >> "$TAIL_UPDATE_LATENCY_RES"
        echo -n "$cl    " >> "$TAIL_UPDATE_LATENCY_RES"
        echo -n "$mean    " >> "$TAIL_UPDATE_LATENCY_RES"
        echo -n "$t_error    " >> "$TAIL_UPDATE_LATENCY_RES"
        echo "" >> "$TAIL_UPDATE_LATENCY_RES"
    elif [ $key == "average_scan_latency" ]; then
        echo -n "" >> "$AVG_SCAN_LATENCY_RES"
        echo -n "$type    " >> "$AVG_SCAN_LATENCY_RES"
        echo -n "$cl    " >> "$AVG_SCAN_LATENCY_RES"
        echo -n "$mean    " >> "$AVG_SCAN_LATENCY_RES"
        echo -n "$t_error    " >> "$AVG_SCAN_LATENCY_RES"
        echo "" >> "$AVG_SCAN_LATENCY_RES"
    elif [ $key == "tail_scan_latency_99th" ]; then
        echo -n "" >> "$TAIL_SCAN_LATENCY_RES"
        echo -n "$type    " >> "$TAIL_SCAN_LATENCY_RES"
        echo -n "$cl    " >> "$TAIL_SCAN_LATENCY_RES"
        echo -n "$mean    " >> "$TAIL_SCAN_LATENCY_RES"
        echo -n "$t_error    " >> "$TAIL_SCAN_LATENCY_RES"
        echo "" >> "$TAIL_SCAN_LATENCY_RES"
    elif [ $key == "average_insert_latency" ]; then
        echo -n "" >> "$AVG_INSERT_LATENCY_RES"
        echo -n "$type    " >> "$AVG_INSERT_LATENCY_RES"
        echo -n "$cl    " >> "$AVG_INSERT_LATENCY_RES"
        echo -n "$mean    " >> "$AVG_INSERT_LATENCY_RES"
        echo -n "$t_error    " >> "$AVG_INSERT_LATENCY_RES"
        echo "" >> "$AVG_INSERT_LATENCY_RES"
    elif [ $key == "tail_insert_latency_99th" ]; then
        echo -n "" >> "$TAIL_INSERT_LATENCY_RES"
        echo -n "$type    " >> "$TAIL_INSERT_LATENCY_RES"
        echo -n "$cl    " >> "$TAIL_INSERT_LATENCY_RES"
        echo -n "$mean    " >> "$TAIL_INSERT_LATENCY_RES"
        echo -n "$t_error    " >> "$TAIL_INSERT_LATENCY_RES"
        echo "" >> "$TAIL_INSERT_LATENCY_RES"
    elif [ $key == "overall_runtime" ]; then
        echo -n "" >> "$OVERALL_THG_RES"
        echo -n "$type    " >> "$OVERALL_THG_RES"
        echo -n "$cl    " >> "$OVERALL_THG_RES"
        echo -n "$mean    " >> "$OVERALL_THG_RES"
        echo -n "$t_error    " >> "$OVERALL_THG_RES"
        echo "" >> "$OVERALL_THG_RES"
    elif [ $key == "overall_throughput" ]; then
        echo -n "" >> "$OVERALL_THG_RES"
        echo -n "$type    " >> "$OVERALL_THG_RES"
        echo -n "$cl    " >> "$OVERALL_THG_RES"
        echo -n "$mean    " >> "$OVERALL_THG_RES"
        echo -n "$t_error    " >> "$OVERALL_THG_RES"
        echo "" >> "$OVERALL_THG_RES"
    elif [ $key == "average_user_time" ]; then
        echo -n "" >> "$CPU_TIME_RES"
        echo -n "$type    " >> "$CPU_TIME_RES"
        echo -n "$cl    " >> "$CPU_TIME_RES"
        echo -n "$mean    " >> "$CPU_TIME_RES"
        echo -n "$t_error    " >> "$CPU_TIME_RES"
        echo "" >> "$CPU_TIME_RES"
    elif [ $key == "average_disk_read_io" ]; then
        echo -n "" >> "$DISK_READ_IO_RES"
        echo -n "$type    " >> "$DISK_READ_IO_RES"
        echo -n "$cl    " >> "$DISK_READ_IO_RES"
        echo -n "$mean    " >> "$DISK_READ_IO_RES"
        echo -n "$t_error    " >> "$DISK_READ_IO_RES"
        echo "" >> "$DISK_READ_IO_RES"
    elif [ $key == "average_disk_write_io" ]; then
        echo -n "" >> "$DISK_WRITE_IO_RES"
        echo -n "$type    " >> "$DISK_WRITE_IO_RES"
        echo -n "$cl    " >> "$DISK_WRITE_IO_RES"
        echo -n "$mean    " >> "$DISK_WRITE_IO_RES"
        echo -n "$t_error    " >> "$DISK_WRITE_IO_RES"
        echo "" >> "$DISK_WRITE_IO_RES"
    fi
}

# print the round results
function print_round_results() {
    local -n data=$1
    local round=$2
    local scheme_dir=$3
    local output_file="${scheme_dir}/${round}_results.csv"

    # sort the nodes
    local -a nodes=()
    for node_key in "${!data[@]}"; do
        if [[ $node_key == *",node_name" ]]; then
            nodes+=("${data[$node_key]}")
        fi
    done
    IFS=$'\n' sorted_nodes=($(sort <<<"${nodes[*]}"))
    unset IFS

    # clean the output file
    > "$output_file"

    # echo -n "Data Type," >> "$output_file"
    for node in "${sorted_nodes[@]}"; do
        echo -n "$node," >> "$output_file"
    done
    echo "" >> "$output_file"
    for key in "node_name" "uptime" "user_time" "sys_time" "disk_read_io" "disk_write_io" "network_recv_io" "network_send_io" "local_read_count" "local_read_latency" "coordinator_read_count" "coordinator_read_latency" "local_write_count" "local_write_latency" "local_scan_count" "local_scan_latency" "coordinator_scan_count" "coordinator_scan_latency"; do
        echo -n "$key," >> "$output_file"
        for node in "${sorted_nodes[@]}"; do
            echo -n "${data[$node,$key]}," >> "$output_file"
        done
        echo "" >> "$output_file"
    done

    echo -n "Average Read Latency (us)," >> "$output_file"
    echo -n "${data[average_read_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average Local Read Latency (us)," >> "$output_file"
    echo -n "${data[average_local_read_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average Coordinator Read Latency (us)," >> "$output_file"
    echo -n "${data[average_coordinator_read_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average Local Write Latency (us)," >> "$output_file"
    echo -n "${data[average_local_write_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average Local Scan Latency (us)," >> "$output_file"
    echo -n "${data[average_local_scan_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average Coordinator Scan Latency (us)," >> "$output_file"
    echo -n "${data[average_coordinator_scan_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Replica Selection Cost (us)," >> "$output_file"
    echo -n "${data[replica_selection_cost]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Read Network Cost (us)," >> "$output_file"
    echo -n "${data[read_network_cost]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Median Read Latency (us)," >> "$output_file"
    echo -n "${data[median_read_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "75th Percentile Read Latency (us)," >> "$output_file"
    echo -n "${data[tail_read_latency_75th]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "99th Percentile Read Latency (us)," >> "$output_file"
    echo -n "${data[tail_read_latency_99th]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "999th Percentile Read Latency (us)," >> "$output_file"
    echo -n "${data[tail_read_latency_999th]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average Update Latency (us)," >> "$output_file"
    echo -n "${data[average_update_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "99th Percentile Update Latency (us)," >> "$output_file"
    echo -n "${data[tail_update_latency_99th]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Overall Runtime (ms)," >> "$output_file"
    echo -n "${data[overall_runtime]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Overall Throughput (ops/sec)," >> "$output_file"
    echo -n "${data[overall_throughput]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average User Time (s)," >> "$output_file"
    echo -n "${data[average_user_time]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average System Time (s)," >> "$output_file"
    echo -n "${data[average_sys_time]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average Disk Read I/O (MiB)," >> "$output_file"
    echo -n "${data[average_disk_read_io]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "Average Disk Write I/O (MiB)," >> "$output_file"
    echo -n "${data[average_disk_write_io]}," >> "$output_file"
    echo "" >> "$output_file"
    

    # print the results
    # echo -n "Average Read Latency (us),"
    # echo -n "${data[average_read_latency]},"
    # echo ""
    # echo -n "99th Percentile Read Latency (us),"
    # echo -n "${data[tail_read_latency_99th]},"
    # echo ""
    # echo -n "Average Update Latency (us),"
    # echo -n "${data[average_update_latency]},"
    # echo ""
    # echo -n "99th Percentile Update Latency (us),"
    # echo -n "${data[tail_update_latency_99th]},"
    # echo ""
    # echo -n "Overall Runtime (ms),"
    # echo -n "${data[overall_runtime]},"
    # echo ""
    # echo -n "Overall Throughput (ops/sec),"
    # echo -n "${data[overall_throughput]},"
    # echo ""
    # echo -n "Average User Time (s),"
    # echo -n "${data[average_user_time]},"
    # echo ""
    # echo -n "Average System Time (s),"
    # echo -n "${data[average_sys_time]},"
    # echo ""
    # echo -n "Average Disk Read I/O (MiB),"
    # echo -n "${data[average_disk_read_io]},"
    # echo ""
    # echo -n "Average Disk Write I/O (MiB),"
    # echo -n "${data[average_disk_write_io]},"
    # echo ""
    
}



function main {
    local main_dir=$1
    if [[ -z "$main_dir" ]]; then
        echo "Usage: $0 <directory>"
        exit 1
    fi

    process_directory "$main_dir"
}

main "$1"
