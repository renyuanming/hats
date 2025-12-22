# Obtain the results for Exp#3 (Facebook's production workload performance)
. /etc/profile
# Workload Settings
EXP_NAME="Exp1-effectiveness"
PURE_READ_WORKLOADS=("workloadc")
MIXED_READ_WRITE_WORKLOADS=("workloada" "workloadb")
REQUEST_DISTRIBUTIONS=("zipfian") # zipfian uniform
OPERATION_NUMBER=25000000
KV_NUMBER=100000000
FIELD_LENGTH=(1000)
KEY_LENGTH=(24)
KEY_LENGTHMin=24
KEY_LENGTHMax=24
REPLICAS=(3)
THREAD_NUMBER=(100)
MEMTABLE_SIZE=(2048)
MOTIVATION=("false") # true is only forward the read request to the primary lsm-tree
MEMORY_LIMIT="12G"
USE_DIRECTIO=("false") # enable direct io for read path or not
PURPOSE="ReadAmplification" # To prove ReadAmplification, CompactionOverhead, we select different dataset
READ_SENSISTIVITY=0.9
BRANCH="main"
LOG_LEVEL="error"
SSTABLE_SIZE_IN_MB=160
COMPACTION_STRATEGY=("LCS")
CONSISTENCY_LEVEL=("ONE")

# Debug Settings
REBUILD_SERVER="true"
REBUILD_CLIENT="true"

# Server settings
ROUNDS=1
COMPACTION_LEVEL=("all") # zero one all

# Hats
SCHEDULING_INITIAL_DELAY=120 # seconds
SCHEDULING_INTERVAL=(60) # seconds
STATES_UPDATE_INTERVAL=10 # seconds
THROTLLE_DATA_RATE=(90) # MB/s

JDK_VERSION="17"

SCHEMES=("hats" "depart-5.0" "c3" "mlsm")

function exportEnv {
    scheme=$1
    export BACKUP_MODE="local"
    export SCHEME=$scheme # hats or depart
    export CLUSTER_NAME="1x"
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
    source "${SCRIPT_DIR}/../common.sh"
}