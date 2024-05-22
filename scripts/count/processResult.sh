#!/bin/bash


# The reuslt file for plot
avg_read_latency_result="avgRead.txt"
tail_read_latency_99th_result="tailRead.txt"
avg_update_latency_result="avgUpdate.txt"
tail_update_latency_99th_result="tailUpdate.txt"
overall_throughput_result="throughput.txt"
cpu_time_result="cpuTime.txt"
disk_read_io_result="diskReadIO.txt"
disk_write_io_result="diskWriteIO.txt"

title_line="Type    Mean    SD"


function extract_value() {
    awk -v pattern="$1" '$0 ~ pattern {print $NF}' "$2"
}

function process_directory() {
    local run_dir=$1

    # Initialize the output files
    avg_read_latency_result="${run_dir}/${avg_read_latency_result}"
    tail_read_latency_99th_result="${run_dir}/${tail_read_latency_99th_result}"
    avg_update_latency_result="${run_dir}/${avg_update_latency_result}"
    tail_update_latency_99th_result="${run_dir}/${tail_update_latency_99th_result}"
    overall_throughput_result="${run_dir}/${overall_throughput_result}"
    cpu_time_result="${run_dir}/${cpu_time_result}"
    disk_read_io_result="${run_dir}/${disk_read_io_result}"
    disk_write_io_result="${run_dir}/${disk_write_io_result}"

    > "$avg_read_latency_result" && echo "$title_line" > "$avg_read_latency_result"
    > "$tail_read_latency_99th_result" && echo "$title_line" > "$tail_read_latency_99th_result"
    > "$avg_update_latency_result" && echo "$title_line" > "$avg_update_latency_result"
    > "$tail_update_latency_99th_result" && echo "$title_line" > "$tail_update_latency_99th_result"
    > "$overall_throughput_result" && echo "$title_line" > "$overall_throughput_result"
    > "$cpu_time_result" && echo "$title_line" > "$cpu_time_result"
    > "$disk_read_io_result" && echo "$title_line" > "$disk_read_io_result"
    > "$disk_write_io_result" && echo "$title_line" > "$disk_write_io_result"




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

        for node_dir in "$round_dir"node*/; do
            node_name=$(basename "$node_dir")
            # echo "Processing $node_dir"
            
            # Get the CPU 
            cpu_file="$node_dir/metrics/After-normal-run_cpu_summary.txt"
            uptime=$(printf "%.2f" $(extract_value "Process uptime" "$cpu_file"))
            user_time=$(printf "%.2f" $(extract_value "User time" "$cpu_file"))
            sys_time=$(printf "%.2f" $(extract_value "System time" "$cpu_file"))

            # Get the disk I/O
            disk_io_file_after="$node_dir/metrics/After-normal-run_disk_io_total.txt"
            disk_io_file_before="$node_dir/metrics/Before-run_disk_io_total.txt"

            after_read=$(extract_value "Disk KiB read" "$disk_io_file_after")
            before_read=$(extract_value "Disk KiB read" "$disk_io_file_before")

            after_read=$(printf "%f" $after_read)
            before_read=$(printf "%f" $before_read)
            
            disk_read_io=$(echo "scale=2; (${after_read:-0} - ${before_read:-0}) / 1024" | bc)

            after_written=$(extract_value "Disk KiB written" "$disk_io_file_after")
            before_written=$(extract_value "Disk KiB written" "$disk_io_file_before")
            disk_write_io=$(echo "scale=2; (${after_written:-0} - ${before_written:-0}) / 1024" | bc)

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
            network_recv_io=$(echo "scale=2; (${after_received:-0} - ${before_received:-0}) / 1048576" | bc)

            after_sent=$(extract_value "Bytes sent" "$network_file_after")
            before_sent=$(extract_value "Bytes sent" "$network_file_before")
            network_send_io=$(echo "scale=2; (${after_sent:-0} - ${before_sent:-0}) / 1048576" | bc)

            # Get the read and write counts
            db_stats_file_after="$node_dir/metrics/After-normal-run_db_stats.txt"
            db_stats_file_before="$node_dir/metrics/Before-run_db_stats.txt"

            after_reads=$(extract_value "Local read count" "$db_stats_file_after")
            before_reads=$(extract_value "Local read count" "$db_stats_file_before")
            read_count=$(echo "scale=2; (${after_reads:-0} - ${before_reads:-0})" | bc)

            after_writes=$(extract_value "Local write count" "$db_stats_file_after")
            before_writes=$(extract_value "Local write count" "$db_stats_file_before")
            write_count=$(echo "scale=2; (${after_writes:-0} - ${before_writes:-0})" | bc)



            eval "round_data[$node_name,node_name]=\"$node_name\""
            eval "round_data[$node_name,uptime]+=\"$uptime \""
            eval "round_data[$node_name,user_time]+=\"$user_time \""
            eval "round_data[$node_name,sys_time]+=\"$sys_time \""
            eval "round_data[$node_name,disk_read_io]+=\"$disk_read_io \""
            eval "round_data[$node_name,disk_write_io]+=\"$disk_write_io \""
            eval "round_data[$node_name,network_recv_io]+=\"$network_recv_io \""
            eval "round_data[$node_name,network_send_io]+=\"$network_send_io \""
            eval "round_data[$node_name,read_count]+=\"$read_count \""
            eval "round_data[$node_name,write_count]+=\"$write_count \""

            user_time_of_all_nodes+=("$user_time")
            sys_time_of_all_nodes+=("$sys_time")
            disk_read_io_of_all_nodes+=("$disk_read_io")
            disk_write_io_of_all_nodes+=("$disk_write_io")
        done

        # Get the average read latency
        average_read_latency=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[READ\], AverageLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        tail_read_latency_99th=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[READ\], 99thPercentileLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        average_update_latency=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[UPDATE\], AverageLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        tail_update_latency_99th=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[UPDATE\], 99thPercentileLatency(us), " "{}" | awk -F ", " "{print \$3}"' \;)
        overall_runtime=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[OVERALL\], RunTime(ms), " "{}" | awk -F ", " "{print \$3}"' \;)
        overall_throughput=$(find $round_dir -name "*.log" -exec sh -c 'grep "\[OVERALL\], Throughput(ops/sec), " "{}" | awk -F ", " "{print \$3}"' \;)
        avg_user_time=$(echo "scale=2; ($(IFS=+; echo "(${user_time_of_all_nodes[*]})") / ${#user_time_of_all_nodes[@]})" | bc)
        avg_sys_time=$(echo "scale=2; ($(IFS=+; echo "(${sys_time_of_all_nodes[*]})") / ${#sys_time_of_all_nodes[@]})" | bc)
        avg_disk_read_io=$(echo "scale=2; ($(IFS=+; echo "(${disk_read_io_of_all_nodes[*]})") / ${#disk_read_io_of_all_nodes[@]})" | bc)
        avg_disk_write_io=$(echo "scale=2; ($(IFS=+; echo "(${disk_write_io_of_all_nodes[*]})") / ${#disk_write_io_of_all_nodes[@]})" | bc)

        eval "round_data[average_read_latency]+=\"$average_read_latency \""
        eval "round_data[tail_read_latency_99th]+=\"$tail_read_latency_99th \""
        eval "round_data[average_update_latency]+=\"$average_update_latency \""
        eval "round_data[tail_update_latency_99th]+=\"$tail_update_latency_99th \""
        eval "round_data[overall_runtime]+=\"$overall_runtime \""
        eval "round_data[overall_throughput]+=\"$overall_throughput \""
        eval "round_data[average_user_time]=\"$avg_user_time\""
        eval "round_data[average_sys_time]=\"$avg_sys_time\""
        eval "round_data[average_disk_read_io]=\"$avg_disk_read_io\""
        eval "round_data[average_disk_write_io]=\"$avg_disk_write_io\""

        eval "overall_data[$round,average_read_latency]=\"$average_read_latency \""
        eval "overall_data[$round,tail_read_latency_99th]=\"$tail_read_latency_99th \""
        eval "overall_data[$round,average_update_latency]=\"$average_update_latency \""
        eval "overall_data[$round,tail_update_latency_99th]=\"$tail_update_latency_99th \""
        eval "overall_data[$round,overall_runtime]=\"$overall_runtime \""
        eval "overall_data[$round,overall_throughput]=\"$overall_throughput \""
        eval "overall_data[$round,average_user_time]=\"$avg_user_time\""
        eval "overall_data[$round,average_sys_time]=\"$avg_sys_time\""
        eval "overall_data[$round,average_disk_read_io]=\"$avg_disk_read_io\""
        eval "overall_data[$round,average_disk_write_io]=\"$avg_disk_write_io\""
        eval "overall_data[$round,round]=\"$round\""

        # Output the results for each round
        # print_round_results round_data $round "$scheme_dir"
        unset round_data

    done

    # Output the overall results
    print_overall_results overall_data $scheme_name

    

}

function get_standard_deviation() {
    local -a arr=("${@:2}")
    local n=${#arr[@]}
    local mean=$1
    

    local sum_diff_sq=0
    for num in "${arr[@]}"; do
        local diff=$(bc <<< "scale=2; ($num - $mean)^2")
        sum_diff_sq=$(bc <<< "scale=2; $sum_diff_sq + $diff")
    done
    standard_deviation=$(bc <<< "scale=2; sqrt($sum_diff_sq / ($n - 1))")
    
    echo "$standard_deviation"
}

function print_overall_results() {

    local -n data=$1
    local scheme_name=$2
    local output_file="${scheme_name}_results.csv"

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

    # calculate the student t distribution for 95% confidence interval
    for key in "average_read_latency" "tail_read_latency_99th" "average_update_latency" "tail_update_latency_99th" "overall_runtime" "overall_throughput" "average_user_time" "average_sys_time" "average_disk_read_io" "average_disk_write_io"; do
        metrics=()
        for round in "${sorted_rounds[@]}"; do
            metrics+=("${data[$round,$key]}")
        done

        mean=$(echo "scale=2; ($(IFS=+; echo "(${metrics[*]})") / ${#metrics[@]})" | bc)
        echo "Mean for $key: $mean"
        standard_deviation=$(get_standard_deviation "${metrics[@]}" $mean)
        # echo "Standard Deviation for $key: $standard_deviation"
        standard_error=$(echo "scale=2; ${standard_deviation} / sqrt(${#metrics[@]})" | bc)
        # echo "Standard Error for $key: $standard_error"


        degrees_of_freedom=$((${#metrics[@]} - 1))
        # echo "Degrees of Freedom for $key: $degrees_of_freedom"
        t_distribution=$(Rscript -e "qt(0.975, df=$degrees_of_freedom)" | awk '{print $2}')
        # echo "T Distribution for $key: $t_distribution"
        t_error=$(echo "scale=2; $t_distribution * $standard_error" | bc)
        echo "T Error for $key: $t_error"


        lower_bound=$(echo "scale=2; $mean - ${t_error}" | bc)
        echo "Lower Bound for $key: $lower_bound"
        upper_bound=$(echo "scale=2; $mean + ${t_error}" | bc)
        echo "Upper Bound for $key: $upper_bound"

        echo -n "$key," >> "$output_file"
        echo -n "$mean," >> "$output_file"
        echo -n "$t_error," >> "$output_file"
        echo -n "$lower_bound," >> "$output_file"
        echo -n "$upper_bound," >> "$output_file"

        echo "" >> "$output_file"

        output_to_plot_file $key $mean $t_error

    done
}


function output_to_plot_file {
    key=$1
    mean=$2
    t_error=$3

    if [ $key == "average_read_latency" ]; then
        echo -n "" >> "$avg_read_latency_result"
        echo -n "$mean    " >> "$avg_read_latency_result"
        echo -n "$t_error    " >> "$avg_read_latency_result"
        echo "" >> "$avg_read_latency_result"
    elif [ $key == "tail_read_latency_99th" ]; then
        echo -n "" >> "$tail_read_latency_99th_result"
        echo -n "$mean    " >> "$tail_read_latency_99th_result"
        echo -n "$t_error    " >> "$tail_read_latency_99th_result"
        echo "" >> "$tail_read_latency_99th_result"
    elif [ $key == "average_update_latency" ]; then
        echo -n "" >> "$avg_update_latency_result"
        echo -n "$mean    " >> "$avg_update_latency_result"
        echo -n "$t_error    " >> "$avg_update_latency_result"
        echo "" >> "$avg_update_latency_result"
    elif [ $key == "tail_update_latency_99th" ]; then
        echo -n "" >> "$tail_update_latency_99th_result"
        echo -n "$mean    " >> "$tail_update_latency_99th_result"
        echo -n "$t_error    " >> "$tail_update_latency_99th_result"
        echo "" >> "$tail_update_latency_99th_result"
    elif [ $key == "overall_throughput" ]; then
        echo -n "" >> "$overall_throughput_result"
        echo -n "$mean    " >> "$overall_throughput_result"
        echo -n "$t_error    " >> "$overall_throughput_result"
        echo "" >> "$overall_throughput_result"
    elif [ $key == "average_user_time" ]; then
        echo -n "" >> "$cpu_time_result"
        echo -n "$mean    " >> "$cpu_time_result"
        echo -n "$t_error    " >> "$cpu_time_result"
        echo "" >> "$cpu_time_result"
    elif [ $key == "average_disk_read_io" ]; then
        echo -n "" >> "$disk_read_io_result"
        echo -n "$mean    " >> "$disk_read_io_result"
        echo -n "$t_error    " >> "$disk_read_io_result"
        echo "" >> "$disk_read_io_result"
    elif [ $key == "average_disk_write_io" ]; then
        echo -n "" >> "$disk_write_io_result"
        echo -n "$mean    " >> "$disk_write_io_result"
        echo -n "$t_error    " >> "$disk_write_io_result"
        echo "" >> "$disk_write_io_result"
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
    for key in "node_name" "uptime" "user_time" "sys_time" "disk_read_io" "disk_write_io" "network_recv_io" "network_send_io" "read_count" "write_count"; do
        echo -n "$key," >> "$output_file"
        for node in "${sorted_nodes[@]}"; do
            echo -n "${data[$node,$key]}," >> "$output_file"
        done
        echo "" >> "$output_file"
    done

    echo -n "Average Read Latency (us)," >> "$output_file"
    echo -n "${data[average_read_latency]}," >> "$output_file"
    echo "" >> "$output_file"
    echo -n "99th Percentile Read Latency (us)," >> "$output_file"
    echo -n "${data[tail_read_latency_99th]}," >> "$output_file"
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
    echo -n "Average Read Latency (us),"
    echo -n "${data[average_read_latency]},"
    echo ""
    echo -n "99th Percentile Read Latency (us),"
    echo -n "${data[tail_read_latency_99th]},"
    echo ""
    echo -n "Average Update Latency (us),"
    echo -n "${data[average_update_latency]},"
    echo ""
    echo -n "99th Percentile Update Latency (us),"
    echo -n "${data[tail_update_latency_99th]},"
    echo ""
    echo -n "Overall Runtime (ms),"
    echo -n "${data[overall_runtime]},"
    echo ""
    echo -n "Overall Throughput (ops/sec),"
    echo -n "${data[overall_throughput]},"
    echo ""
    echo -n "Average User Time (s),"
    echo -n "${data[average_user_time]},"
    echo ""
    echo -n "Average System Time (s),"
    echo -n "${data[average_sys_time]},"
    echo ""
    echo -n "Average Disk Read I/O (MiB),"
    echo -n "${data[average_disk_read_io]},"
    echo ""
    echo -n "Average Disk Write I/O (MiB),"
    echo -n "${data[average_disk_write_io]},"
    echo ""
    
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

