#!/bin/bash

# 定义数组来存储每分钟的总 Thpt
declare -a totalReadThpt
declare -a totalWriteThpt
declare -a totalFlushThpt
declare -a totalCompactionThpt

# 定义一个函数来处理每个节点的文件
process_node_file() {
    local metricFile=$1
    local minuteIndex=0

    while IFS= read -r line; do
        # 提取各个字段的值并去除末尾的 "mb/s"
        readThpt=$(echo "$line" | grep -oP '(?<=Read Thpt: )[^,]+' | sed 's/mb\/s//')
        writeThpt=$(echo "$line" | grep -oP '(?<=Write Thpt: )[^,]+' | sed 's/mb\/s//')
        flushThpt=$(echo "$line" | grep -oP '(?<=Flush Thpt: )[^,]+' | sed 's/mb\/s//')
        compactionThpt=$(echo "$line" | grep -oP '(?<=Compaction Thpt: )[^,]+' | sed 's/mb\/s//')

        # 汇总每分钟的 Thpt
        totalReadThpt[$minuteIndex]=$(echo "${totalReadThpt[$minuteIndex]:-0} + $readThpt" | bc)
        totalWriteThpt[$minuteIndex]=$(echo "${totalWriteThpt[$minuteIndex]:-0} + $writeThpt" | bc)
        totalFlushThpt[$minuteIndex]=$(echo "${totalFlushThpt[$minuteIndex]:-0} + $flushThpt" | bc)
        totalCompactionThpt[$minuteIndex]=$(echo "${totalCompactionThpt[$minuteIndex]:-0} + $compactionThpt" | bc)

        minuteIndex=$((minuteIndex + 1))
    done < "$metricFile"
}

# 处理多个节点的文件
for nodeFile in node_*.txt; do
    process_node_file "$nodeFile"
done

# 输出结果
echo "Minute, Total Read Thpt (mb/s), Total Write Thpt (mb/s), Total Flush Thpt (mb/s), Total Compaction Thpt (mb/s)"
for i in "${!totalReadThpt[@]}"; do
    echo "$i, ${totalReadThpt[$i]}, ${totalWriteThpt[$i]}, ${totalFlushThpt[$i]}, ${totalCompactionThpt[$i]}"
done
