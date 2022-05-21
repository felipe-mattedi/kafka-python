from kafka import KafkaProducer


def sucesso_envio(record_metadata):
    print('Mensagem postada com sucesso na particao: ', record_metadata.partition, 'do topico: ', record_metadata.topic)

def erro_envio(excp):
    print('Erro no envio da mensagem')
    print(excp)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

mensagem = input('Insira a mensagem para postagem no tópico: ').encode('utf-8')
producer.send('FELIPE_TOPICO_1', mensagem, partition=3).add_callback(sucesso_envio).add_errback(erro_envio)
producer.send('FELIPE_TOPICO_A', mensagem).add_callback(sucesso_envio).add_errback(erro_envio)
producer.flush()


