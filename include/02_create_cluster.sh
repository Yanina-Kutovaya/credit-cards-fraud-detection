yc dataproc cluster create --name dataproc-1 \
--description 'Data generation cluster' \
--service-account-name fraud-detection-sa \
--zone ru-central1-a \
--version 2.0 \
--services hdfs,yarn,mapreduce,spark,tez \
--subcluster name=dataproc-1-master,'
    'role=masternode,'
    'resource-preset=s2.small,'
    'disk-type=network-ssd,'
    'disk-size=128,'
    'subnet-name=subnet-1-a,'
    'hosts-count=1,'
    'assign-public-ip=true \
--subcluster name=dataproc-1-data,'
    'role=datanode,'
    'resource-preset=s2.small,'
    'disk-type=network-hdd,'
    'disk-size=128,'
    'subnet-name=subnet-1-a,'
    'hosts-count=3,'
    'assign-public-ip=false 
--bucket generated-data \
--ui-proxy=true \
--ssh-public-keys-file C:/Users/ASER/.ssh/id_rsa.pub \
--security-group-ids enpkh1qj9vtvb877p4dv \
--deletion-protection=false