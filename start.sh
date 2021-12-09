#!/bin/bash
#echo "Setting up dispatcher schemas and objects"
#python ./dispatcher/setup.py 

echo "PixDispatcher"
echo ""
echo "⏱  Initialization process"
echo "------------------------------------------------------------"
./pixctl login --api http://$GQL_HOST/graphql --username dispatcher --password $DISPATCHER_PASSWORD
if [ $? -eq 0 ]
then
        echo "------------------------------------------------------------"
        echo "🔩 Importing schemas"
        ./pixctl schema import --api http://$GQL_HOST/graphql --recursive ./docs/schemas/original
        echo "------------------------------------------------------------"
        echo "🔧 Patching schemas"
        ./pixctl schema patch --api http://$GQL_HOST/graphql --recursive ./docs/schemas/patch
        echo "------------------------------------------------------------"
        echo "🎬 Uploading media files"
        sleep 10
        ./pixctl media batch --url http://pix-media-server:3000 --filelist ./docs/media.yaml
        echo "------------------------------------------------------------"
        echo "🧮 Starting PixDispatcher"
        python ./dispatcher/dispatcher.py
else
        echo "🔴 Initialization failed" >&2
fi

# if [ $? -eq 0 ]
# then
#   echo "Setup successful, starting dispatcher"
#   python ./dispatcher/dispatcher.py
# else
#   echo "Setup failed with error " >&2
# fi
 