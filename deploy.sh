#!/bin/zsh
set -e

source .env.deploy

CARGO_PROFILE_ARG=""

if [ $PROFILE = "release" ]; then 
    CARGO_PROFILE_ARG="--release";
fi

echo "Building app for the target..."
ssh $SDK_SSH_HOST /bin/bash << ENDSSH 
cd $REMOTE_PROJECT_DIR
source $SDK_ENV_SETUP_SCRIPT
git pull
cargo build $CARGO_PROFILE_ARG --target $TARGET_TRIPLE
$STRIP -s target/$TARGET_TRIPLE/$PROFILE/zeus-controller
ENDSSH

echo "Copying the binary..."
scp $SDK_SSH_HOST:$REMOTE_PROJECT_DIR/target/$TARGET_TRIPLE/$PROFILE/zeus-controller $TARGET_SSH_HOST:$TARGET_DEPLOYMENT_PATH
