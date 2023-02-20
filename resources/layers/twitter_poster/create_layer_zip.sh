# Remove the python dir if it exists
rm -rf python | true
# Remove the python zip file if it exists
rm -rf python.zip | true

# Run docker to install the packages compatible with the Lambda Linux OS
docker run -i -v "$PWD":/var/task "mlupin/docker-lambda:python3.9-build" /bin/bash -s <<EOF
pip install -r requirements.txt -t python/lib/python3.9/site-packages/
exit
EOF

# Compress the result into a zip file
zip -r python.zip python > /dev/null;
# Remove the python dir again, we don't need it anymore
rm -rf python | true
