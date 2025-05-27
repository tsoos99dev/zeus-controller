#!/bin/zsh
set -e

source .env.deploy

ssh $SDK_SSH_HOST /bin/bash << ENDSSH 
cd $REMOTE_PROJECT_DIR
source $SDK_ENV_SETUP_SCRIPT
git pull
cargo build --target $TARGET_TRIPLE
ENDSSH

scp $SDK_SSH_HOST:$REMOTE_PROJECT_DIR/target/$TARGET_TRIPLE/debug/zeus-controller $TARGET_SSH_HOST:$TARGET_DEPLOYMENT_PATH
