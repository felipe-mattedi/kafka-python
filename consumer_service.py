from collections import namedtuple
from kafka import KafkaConsumer
import time

class Consumer:
    def __init__(self, topico, grupo):
        self.topico = topico
        self.grupo = grupo
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            max_poll_records = 1,
            group_id = self.grupo,
            enable_auto_commit = True )
        self.consumer.subscribe([self.topico])

    def consome_mensagem(self):

        try:
            while True:
                message = self.consumer.poll()

                if message:
                    self.consumer.close()
                    return message

                else:
                    time.sleep(2)
                    continue

        except(KeyboardInterrupt):
            print('Fim de execução do consumidor: ' + self.grupo)
            self.consumer.close()
            return False

        if(self.consumer): 
            print('Mensagem consumida em', mensagem.topic, 'Offset: ', mensagem.offset, 'Conteúdo: ', mensagem.value.decode('utf-8'))
            print(mensagem)
            return mensagem.value.decode('utf-8')



