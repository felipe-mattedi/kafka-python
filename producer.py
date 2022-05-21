from kafka import KafkaProducer


def sucesso_envio(record_metadata):
    print('Mensagem postada com sucesso')
    print(record_metadata)

def erro_envio(excp):
    print('Erro no envio da mensagem')
    print(excp)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

mensagem = input('Insira a mensagem para postagem no tópico: ').encode('utf-8')
producer.send('FELIPE_TOPICO_1', mensagem).add_callback(sucesso_envio).add_errback(erro_envio)
producer.flush()


