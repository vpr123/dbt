import uuid
import time
import datetime
from random import randint

f = open("c:/tmp/kafka_messages.csv", "a")

def say(txt):
    print(txt)
    f.write(txt)

num_of_partitions = 4
num_of_messages = 20
start_offset = 41

bad_partition = randint(0, num_of_partitions - 1)

bad_offset1 = randint(start_offset, start_offset + num_of_messages + 1)
bad_offset2 = randint(start_offset, start_offset + num_of_messages + 1)

bad_offset_min = min(bad_offset1, bad_offset2)
bad_offset_max = max(bad_offset1, bad_offset2)

print(str(bad_partition) + ' ' + str(bad_offset_min) + ' ' + str(bad_offset_max))

if start_offset == 1:
    say("partition,offset,session_id,payload,event_ts\n")

for partition in range(num_of_partitions):
    for offset in range(start_offset,start_offset + num_of_messages + 1):
        time.sleep(0.1)
        if not (partition == bad_partition and ( offset >= bad_offset_min and offset <= bad_offset_max )):
           say(str(partition) + "," + str(offset) + ',"' + str(uuid.uuid4()) + '","test","' + str(datetime.datetime.now()) + '"\n')

f.close()
