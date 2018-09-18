ACCUMULO_TRAINING_ROOT=/home/admin/accumulo_training
TOOL=/usr/hdp/current/accumulo-client/bin/tool.sh

$TOOL $ACCUMULO_TRAINING_ROOT/target/accumulo-1.0-SNAPSHOT.jar $1 -p $ACCUMULO_TRAINING_ROOT/src/main/resources/ingestclient.properties

