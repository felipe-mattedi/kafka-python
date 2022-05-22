from compras import compras
from desfazimento import desfazimento


def exibe_menu():
    opcao = input('Digite a opção desejada: ')

    match opcao:
        case "1":
            compras()
            return True
        case "2":
            desfazimento()
            return True
        case "3":
            print('Termino do Sistema')
            return False
        case _:
            print('Opção Invalida')
            return True

print('========================================')
print('=== BEM VINDO AO SISTEMA DE COMPRAS! ===')
print('========================================')
print('1 - Efetuar uma compra')
print('2 - Cancelar uma compra')
print('3 - Sair')

opcao = True
while (opcao == True):
    opcao = exibe_menu()

    

