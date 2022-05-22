from consumer_service import Consumer
from kafka import TopicPartition
import json
import time


def aviso_tela():
    print('---------------------')
    print('Start serviço fraudes')
    print('---------------------')

def aviso_tela_fim():
    print('---------------------')
    print('Stop serviço fraudes')
    print('---------------------')

def roteiro_principal():
    consumidor = Consumer('TOPICO_RECEPCAO_PRODUTO', 'GRUPO_FRAUDES')
    mensagem_consumida = consumidor.consome_mensagem()
    if(mensagem_consumida):
        for chave,valor in mensagem_consumida.items():
            print('Fraude sendo processada para código compra: ', valor[0].value.decode('utf-8'))
            time.sleep(3)
            print('Análise de fraude processada!')
            return True 
    else:
        return False

aviso_tela()

servico_fraude_on = True

while(servico_fraude_on):
    servico_fraude_on = roteiro_principal()

aviso_tela_fim()