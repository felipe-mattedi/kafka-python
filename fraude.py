from consumer_service import Consumer
from threading import Thread
from kafka import TopicPartition
import json
import time

class Fraudes():

    def processa_mensagem(self, mensagem):
        for item in mensagem.values():
            print('Fraude sendo processada para código compra: ', item[0].value.decode('utf-8'))
            time.sleep(3)
            print('Análise de fraude processada!')

    def aviso_tela(self):
        print('---------------------')
        print('Start serviço fraudes')
        print('---------------------')

    def aviso_tela_fim(self):
        print('---------------------')
        print('Stop serviço fraudes')
        print('---------------------')

analise_fraude = Fraudes()
Consumer('TOPICO_RECEPCAO_PRODUTO', 'GRUPO_FRAUDES', analise_fraude)

