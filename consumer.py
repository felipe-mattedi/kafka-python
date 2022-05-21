from kafka import KafkaConsumer

consumer = KafkaConsumer('FELIPE_TOPICO_1', group_id='1' )

try:
    for mensagem in consumer: 
        print(mensagem)
except(KeyboardInterrupt):
    print('Fim de execução')
    consumer.close(autocommit=True)
    


