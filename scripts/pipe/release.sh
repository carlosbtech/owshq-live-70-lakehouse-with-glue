bucket_name=$1
aws_access_key=$2
aws_access_secret=$3
local_path=$4

# Create a zip of the current directory.
zip -r $local_path . -x .git/ .git/*** .github/workflows/release.yml scripts/pipe/release.sh scripts/pipe/upload-file-to-s3.py

# Install required dependencies for Python script.
pip3 install boto3

# Run upload script
python3 scripts/pipe/upload-file-to-s3.py $bucket_name $aws_key $aws_access_key $aws_access_secret $local_path