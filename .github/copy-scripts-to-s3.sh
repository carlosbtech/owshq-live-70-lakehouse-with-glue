#!/bin/bash
SCRIPTS_LOCAL="/home/ubuntu/owshq-live-lakehouse-with-glue/app";

export TZ=America/Sao_Paulo;
echo $(date +'%F %X %:z');

S3_BUCKET="s3://owshq-aws-glue-scripts-777696598735";

echo "Copiar todos os scripts";
/usr/local/bin/aws s3 cp $SCRIPTS_LOCAL $S3_BUCKET/ --recursive;