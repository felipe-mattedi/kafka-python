from producer_service import envia_mensagem
import json
import uuid

class Produto:

    def __init__(self, codigo, quantidade):
        self.quantidade = quantidade
        self.codigo = codigo
        self.topico = "TOPICO_RECEPCAO_PRODUTO"
        self.uuid = str(uuid.uuid4())
    
    def gera_aviso_compra(self):
        envia_mensagem(self.topico, json.dumps(self.__dict__))


def compras():
    produto = cria_produto()
    produto.gera_aviso_compra()
    return

def cria_produto():
    codigo_produto = input('Digite o código do produto que deseja comprar: ')
    quantidade_produto = input('Digite a quantidade que deseja comprar: ')
    return Produto(codigo_produto, quantidade_produto)
