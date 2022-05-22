from kafka import KafkaProducer

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def envia_mensagem(topico, mensagem):
    produtor = Producer()
    produtor.producer.send(topico, mensagem.encode('utf-8')).add_callback(sucesso_envio).add_errback(erro_envio)
    produtor.producer.flush()
    return

def sucesso_envio(record_metadata):
    print('Mensagem postada com sucesso na particao: ', record_metadata.partition, 'do topico: ', record_metadata.topic)

def erro_envio(excp):
    print('Erro no envio da mensagem')
    print(excp)


