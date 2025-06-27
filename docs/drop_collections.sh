#!/bin/bash

# MongoDB container name or ID
CONTAINER_NAME="mongodb"

# MongoDB database name
DB_NAME=$1

# List of collections to drop
COLLECTIONS=("InstrumentActivityReplayState" "InstrumentActivityState" "ExecutionState" "GeneralLedgerEntery" "GeneralLedgerAccountBalance" "Batch" "sequences" "GeneralLedgerAccountBalanceStage" "GeneralLedgerEnteryStage" "ReclassValues" "MetricLevelLtd" "AttributeLevelLtd" "InstrumentLevelLtd " "InstrumentAttribute" "TransactionActivity")

# MongoDB username and password
USERNAME="root"
PASSWORD="R3s3rv#313"

# Drop each collection
for COLLECTION in "${COLLECTIONS[@]}"; do
  echo "Dropping collection: $COLLECTION"
  docker exec -it $CONTAINER_NAME mongosh $DB_NAME --username $USERNAME --password $PASSWORD --authenticationDatabase admin --eval "db.$COLLECTION.drop()"
done

echo "All specified collections have been dropped."
