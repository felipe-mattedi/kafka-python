from collections import namedtuple
from kafka import KafkaConsumer
import time

class Consumer:
    def __init__(self, topico, grupo, classe):
        self.topico = topico
        self.grupo = grupo
        self.classe = classe
        self.consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092'],
            max_poll_records = 1,
            group_id = self.grupo,
            enable_auto_commit = True )
        self.consumer.subscribe([self.topico])
        self.classe.aviso_tela()
        self.consome_mensagem()
        self.classe.aviso_tela_fim()

    def analisa_mensagem(self,message):
        self.classe.processa_mensagem(message)

    def consome_mensagem(self):
        try:
            while True:
                message = self.consumer.poll()

                if message:
                    self.analisa_mensagem(message)
                    continue

                else:
                    time.sleep(2)
                    continue

        except(KeyboardInterrupt):
            print('Fim de execução do consumidor: ' + self.grupo)
            self.consumer.close()



