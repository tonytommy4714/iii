def kafka_push(table,port,topic):

    import pandas as pd 

    from kafka import KafkaProducer

    import numpy as np

    table_array=np.array(table)

    table_list=list(map(lambda x:list(x), table_array))

    producer = KafkaProducer(bootstrap_servers=['localhost:{port}'.format(port=port)])

    for i in table_list:

        kafka_str=str(i).replace("[","").replace("]","").replace("\'","")

        producer.send(topic,kafka_str.encode("utf-8"))