import boto3
import json
import pandas as pd
from pandas.io.json import json_normalize

ebs = boto3.client('ebs')
ec2 = boto3.client('ec2')

# get sorted list of snapshots
snapshots = ec2.describe_snapshots(OwnerIds=['self'])
df = pd.DataFrame.from_dict(snapshots['Snapshots'])
df.sort_values(by=['OwnerId', "VolumeId", "StartTime"], inplace = True)

# per volumeid lineage, get for each one changed blocks
i = 0
l = len(df.index)
first = True
blockSize = 524288
for index, row in df.iterrows():
    if i == l:
        break
    date = str(row['StartTime']).split('.',1)[0].split(' ')[0]
    time = str(row['StartTime']).split('.',1)[0].split(' ')[1]
    if first:
        v_prev = row['VolumeId']
        sid_prev = row['SnapshotId']
        print(date + ',' + time + ',' + v_prev + "," + 'snap-00000000000000000' + "," + sid_prev + "," + str(row['VolumeSize'] * 1024 * 1024 * 1024))
        first = False
        i = i + 1
        continue
    v = row['VolumeId']
    sid = row['SnapshotId']
    if v == v_prev:
        changed = len(ebs.list_changed_blocks(FirstSnapshotId = sid_prev,SecondSnapshotId = sid)['ChangedBlocks'])
        print(date + ',' + time + ',' + v + "," + sid_prev + "," + sid + "," + str(changed * blockSize))
    else:
        print(date + ',' + time + ',' + v + "," + 'snap-00000000000000000' + "," + sid + "," + str(row['VolumeSize'] * 1024 * 1024 * 1024))
    v_prev = v
    sid_prev = sid
    i = i + 1