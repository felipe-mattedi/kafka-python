from kafka import KafkaConsumer

consumer = KafkaConsumer('FELIPE_TOPICO_1','FELIPE_TOPICO_A', group_id='GRUPO1', enable_auto_commit=False )

try:
    for mensagem in consumer: 
        print('Mensagem consumida em', mensagem.topic, 'Offset: ', mensagem.offset, 'Conteúdo: ', mensagem.value.decode('utf-8'))
except(KeyboardInterrupt):
    print('Fim de execução')
    consumer.close(autocommit=True)

    


