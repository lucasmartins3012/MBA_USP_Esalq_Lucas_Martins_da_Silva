
from pylogix import PLC
import random
from confluent_kafka import Producer
from confluent_kafka import Consumer
import json
import time
import datetime

inicio = time.perf_counter()

comm = PLC()
comm.IPAddress = '192.168.10.10'
comm.ProcessorSlot = 2
x = 1

while (x==1):

    #GERADOR DE DADOS ALEATORIOS PARA ESCRITA NO PLC
    B10_TempBanho = random.randrange(35, 42)
    B10_TempM1 = random.randrange(50, 60)
    B10_PressaoRepouso = random.randrange(2, 4)
    B10_PressaoCiclo = random.randrange(5, 8)
    B10_NivelB10Repouso = random.randrange(40, 45)
    B10_NivelB10Ciclo = random.randrange(15, 20)
    B10_FreqInvCiclo = random.randrange(58, 60)
    B10_FreqInvRepouso = random.randrange(28, 30)
    B10_TensaoInvCiclo = random.randrange(372, 380)
    B10_TensaoInvRepouso = random.randrange(180, 200)
    B10_CorrenteInvCiclo = random.randrange(20, 30)
    B10_CorrenteInvRepouso = random.randrange(12, 15)
    B10_VibracaoM1Ciclo = random.randrange(110, 120)
    B10_VibracaoM1Repouso = random.randrange(55, 60)
    B20_TempBanho = random.randrange(35, 42)
    B20_TempM1 = random.randrange(50, 60)
    B20_PressaoRepouso = random.randrange(2, 4)
    B20_PressaoCiclo = random.randrange(5, 8)
    B20_NivelB10Repouso = random.randrange(40, 45)
    B20_NivelB10Ciclo = random.randrange(15, 20)
    B20_FreqInvCiclo = random.randrange(58, 60)
    B20_FreqInvRepouso = random.randrange(28, 30)
    B20_TensaoInvCiclo = random.randrange(372, 380)
    B20_TensaoInvRepouso = random.randrange(180, 200)
    B20_CorrenteInvCiclo = random.randrange(20, 30)
    B20_CorrenteInvRepouso = random.randrange(12, 15)
    B20_VibracaoM1Ciclo = random.randrange(110, 120)
    B20_VibracaoM1Repouso = random.randrange(55, 60)
    B30_TempBanho = random.randrange(35, 42)
    B30_TempM1 = random.randrange(50, 60)
    B30_PressaoRepouso = random.randrange(2, 4)
    B30_PressaoCiclo = random.randrange(5, 8)
    B30_NivelB10Repouso = random.randrange(40, 45)
    B30_NivelB10Ciclo = random.randrange(15, 20)
    B30_FreqInvCiclo = random.randrange(58, 60)
    B30_FreqInvRepouso = random.randrange(28, 30)
    B30_TensaoInvCiclo = random.randrange(372, 380)
    B30_TensaoInvRepouso = random.randrange(180, 200)
    B30_CorrenteInvCiclo = random.randrange(20, 30)
    B30_CorrenteInvRepouso = random.randrange(12, 15)
    B30_VibracaoM1Ciclo = random.randrange(110, 120)
    B30_VibracaoM1Repouso = random.randrange(55, 60)

    #ESCRITA DE DADOS SIMULADOS NO PLC
    #B10
    comm.Write('B10_TEMPERATURA_BANHO_SIMULADO', B10_TempBanho)
    comm.Write('B10_TEMPERATURA_M1_SIMULADO', B10_TempM1)
    comm.Write('B10_PRESSAO_REPOUSO_SIMULADO', B10_PressaoRepouso)
    comm.Write('B10_PRESSAO_CICLO_SIMULADO', B10_PressaoCiclo)
    comm.Write('B10_NIVEL_REPOUSO_SIMULADO', B10_NivelB10Repouso)
    comm.Write('B10_NIVEL_CICLO_SIMULADO', B10_NivelB10Ciclo)
    comm.Write('B10_FREQUENCIA_INVERSOR_REPOUSO_SIMULADO', B10_FreqInvRepouso)
    comm.Write('B10_FREQUENCIA_INVERSOR_CICLO_SIMULADO', B10_FreqInvCiclo)
    comm.Write('B10_TENSAO_INVERSOR_REPOUSO_SIMULADO', B10_TensaoInvRepouso)
    comm.Write('B10_TENSAO_INVERSOR_CICLO_SIMULADO', B10_TensaoInvCiclo)
    comm.Write('B10_CORRENTE_INVERSOR_REPOUSO_SIMULADO', B10_CorrenteInvRepouso)
    comm.Write('B10_CORRENTE_INVERSOR_CICLO_SIMULADO', B10_CorrenteInvCiclo)
    comm.Write('B10_VIBRACAO_M1_REPOUSO_SIMULADO', B10_VibracaoM1Repouso)
    comm.Write('B10_VIBRACAO_M1_CICLO_SIMULADO', B10_VibracaoM1Ciclo)
    #B20
    comm.Write('B20_TEMPERATURA_BANHO_SIMULADO', B20_TempBanho)
    comm.Write('B20_TEMPERATURA_M1_SIMULADO', B20_TempM1)
    comm.Write('B20_PRESSAO_REPOUSO_SIMULADO', B20_PressaoRepouso)
    comm.Write('B20_PRESSAO_CICLO_SIMULADO', B20_PressaoCiclo)
    comm.Write('B20_NIVEL_REPOUSO_SIMULADO', B20_NivelB10Repouso)
    comm.Write('B20_NIVEL_CICLO_SIMULADO', B20_NivelB10Ciclo)
    comm.Write('B20_FREQUENCIA_INVERSOR_REPOUSO_SIMULADO', B20_FreqInvRepouso)
    comm.Write('B20_FREQUENCIA_INVERSOR_CICLO_SIMULADO', B20_FreqInvCiclo)
    comm.Write('B20_TENSAO_INVERSOR_REPOUSO_SIMULADO', B20_TensaoInvRepouso)
    comm.Write('B20_TENSAO_INVERSOR_CICLO_SIMULADO', B20_TensaoInvCiclo)
    comm.Write('B20_CORRENTE_INVERSOR_REPOUSO_SIMULADO', B20_CorrenteInvRepouso)
    comm.Write('B20_CORRENTE_INVERSOR_CICLO_SIMULADO', B20_CorrenteInvCiclo)
    comm.Write('B20_VIBRACAO_M1_REPOUSO_SIMULADO', B20_VibracaoM1Repouso)
    comm.Write('B20_VIBRACAO_M1_CICLO_SIMULADO', B20_VibracaoM1Ciclo)
    #B30
    comm.Write('B30_TEMPERATURA_BANHO_SIMULADO', B30_TempBanho)
    comm.Write('B30_TEMPERATURA_M1_SIMULADO', B30_TempM1)
    comm.Write('B30_PRESSAO_REPOUSO_SIMULADO', B30_PressaoRepouso)
    comm.Write('B30_PRESSAO_CICLO_SIMULADO', B30_PressaoCiclo)
    comm.Write('B30_NIVEL_REPOUSO_SIMULADO', B30_NivelB10Repouso)
    comm.Write('B30_NIVEL_CICLO_SIMULADO', B30_NivelB10Ciclo)
    comm.Write('B30_FREQUENCIA_INVERSOR_REPOUSO_SIMULADO', B30_FreqInvRepouso)
    comm.Write('B30_FREQUENCIA_INVERSOR_CICLO_SIMULADO', B30_FreqInvCiclo)
    comm.Write('B30_TENSAO_INVERSOR_REPOUSO_SIMULADO', B30_TensaoInvRepouso)
    comm.Write('B30_TENSAO_INVERSOR_CICLO_SIMULADO', B30_TensaoInvCiclo)
    comm.Write('B30_CORRENTE_INVERSOR_REPOUSO_SIMULADO', B30_CorrenteInvRepouso)
    comm.Write('B30_CORRENTE_INVERSOR_CICLO_SIMULADO', B30_CorrenteInvCiclo)
    comm.Write('B30_VIBRACAO_M1_REPOUSO_SIMULADO', B30_VibracaoM1Repouso)
    comm.Write('B30_VIBRACAO_M1_CICLO_SIMULADO', B30_VibracaoM1Ciclo)
    
    #LEITURA DE DADOS ANALOGICOS PROVENIENTES DO PLC
    #B10
    B10_TempBanho_Real = comm.Read('B10_TEMPERATURA_BANHO_REAL')
    B10_Temp_M1_Real = comm.Read('B10_TEMPERATURA_M1_REAL')
    B10_Pressao_Real = comm.Read('B10_PRESSAO_REAL')
    B10_Nivel_Real = comm.Read('B10_NIVEL_REAL')
    B10_Frequencia_Real = comm.Read('B10_FREQUENCIA_INVERSOR_REAL')
    B10_Tensao_Real = comm.Read('B10_TENSAO_INVERSOR_REAL')
    B10_Corrente_Real = comm.Read('B10_CORRENTE_INVERSOR_REAL')
    B10_Vibracao_Real = comm.Read('B10_VIBRACAO_REAL')
    #B20
    B20_TempBanho_Real = comm.Read('B20_TEMPERATURA_BANHO_REAL')
    B20_Temp_M1_Real = comm.Read('B20_TEMPERATURA_M1_REAL')
    B20_Pressao_Real = comm.Read('B20_PRESSAO_REAL')
    B20_Nivel_Real = comm.Read('B20_NIVEL_REAL')
    B20_Frequencia_Real = comm.Read('B20_FREQUENCIA_INVERSOR_REAL')
    B20_Tensao_Real = comm.Read('B20_TENSAO_INVERSOR_REAL')
    B20_Corrente_Real = comm.Read('B20_CORRENTE_INVERSOR_REAL')
    B20_Vibracao_Real = comm.Read('B20_VIBRACAO_REAL')
    #B30
    B30_TempBanho_Real = comm.Read('B30_TEMPERATURA_BANHO_REAL')
    B30_Temp_M1_Real = comm.Read('B30_TEMPERATURA_M1_REAL')
    B30_Pressao_Real = comm.Read('B30_PRESSAO_REAL')
    B30_Nivel_Real = comm.Read('B30_NIVEL_REAL')
    B30_Frequencia_Real = comm.Read('B30_FREQUENCIA_INVERSOR_REAL')
    B30_Tensao_Real = comm.Read('B30_TENSAO_INVERSOR_REAL')
    B30_Corrente_Real = comm.Read('B30_CORRENTE_INVERSOR_REAL')
    B30_Vibracao_Real = comm.Read('B30_VIBRACAO_REAL')
    Modelo = comm.Read('MODELO')

    #LEITURA DE DADOS DIGITAIS PROVENIENTES DO PLC
    #CABINE
    Cabine_Emergencia = comm.Read('CABINE_EMERGENCIA')
    Cabine_Manual = comm.Read('CABINE_MANUAL')
    Cabine_Automatico = comm.Read('CABINE_AUTOMATICO')
    Cabine_Porta_Entrada_Aberta = comm.Read('CABINE_PORTAABERTA_ENTRADA')
    Cabine_Porta_Entrada_Fechada = comm.Read('CABINE_PORTAFECHADA_ENTRADA')
    Cabine_Porta_Entrada_Falha = comm.Read('CABINE_PORTA_ENTRADA_FALHA')
    Cabine_Porta_Saida_Aberta = comm.Read('CABINE_PORTAABERTA_SAIDA')
    Cabine_Porta_Saida_Fechada = comm.Read('CABINE_PORTAFECHADA_SAIDA')
    Cabine_Porta_Saida_Falha = comm.Read('CABINE_PORTA_SAIDA_FALHA')

    #LEITURA DE DADOS DIGITAIS PROVENIENTES DO PLC
    #B10
    B10_Valvula_Spray_Aberta = comm.Read('B10_VALVULASPRAY_ABERTA')
    B10_Valvula_Spray_Fechada = comm.Read('B10_VALVULASPRAY_FECHADA')
    B10_Valvula_Spray_Falha = comm.Read('B10_VALVULASPRAY_FALHA')
    B10_Valvula_Escoamento_Aberta = comm.Read('B10_VALVULAESCOAMENTO_ABERTA')
    B10_Valvula_Escoamento_Fechada = comm.Read('B10_VALVULAESCOAMENTO_FECHADA')
    B10_Valvula_Escoamento_Falha = comm.Read('B10_VALVULAESCOAMENTO_FALHA')
    B10_Inversor_Falha = comm.Read('B10_INVERSOR_FALHA')
    B10_Cabine_Falha = comm.Read('B10_CABINE_FALHA')
    B10_Cabine_Ocupada = comm.Read('B10_CABINE_OCUPADA')
    B10_Tempo_Ciclo_Excedido = comm.Read('B10_TEMPO_CICLO_EXCEDIDO')
    B10_Ciclo = comm.Read('B10_CICLO')
    
    #B20
    B20_Valvula_Spray_Aberta = comm.Read('B20_VALVULASPRAY_ABERTA')
    B20_Valvula_Spray_Fechada = comm.Read('B20_VALVULASPRAY_FECHADA')
    B20_Valvula_Spray_Falha = comm.Read('B20_VALVULASPRAY_FALHA')
    B20_Valvula_Escoamento_Aberta = comm.Read('B20_VALVULAESCOAMENTO_ABERTA')
    B20_Valvula_Escoamento_Fechada = comm.Read('B20_VALVULAESCOAMENTO_FECHADA')
    B20_Valvula_Escoamento_Falha = comm.Read('B20_VALVULAESCOAMENTO_FALHA')
    B20_Inversor_Falha = comm.Read('B20_INVERSOR_FALHA')
    B20_Cabine_Falha = comm.Read('B20_CABINE_FALHA')
    B20_Cabine_Ocupada = comm.Read('B20_CABINE_OCUPADA')
    B20_Tempo_Ciclo_Excedido = comm.Read('B20_TEMPO_CICLO_EXCEDIDO')
    B20_Ciclo = comm.Read('B20_CICLO')
    #B30
    B30_Valvula_Spray_Aberta = comm.Read('B30_VALVULASPRAY_ABERTA')
    B30_Valvula_Spray_Fechada = comm.Read('B30_VALVULASPRAY_FECHADA')
    B30_Valvula_Spray_Falha = comm.Read('B30_VALVULASPRAY_FALHA')
    B30_Valvula_Escoamento_Aberta = comm.Read('B30_VALVULAESCOAMENTO_ABERTA')
    B30_Valvula_Escoamento_Fechada = comm.Read('B30_VALVULAESCOAMENTO_FECHADA')
    B30_Valvula_Escoamento_Falha = comm.Read('B30_VALVULAESCOAMENTO_FALHA')
    B30_Inversor_Falha = comm.Read('B30_INVERSOR_FALHA')
    B30_Cabine_Falha = comm.Read('B30_CABINE_FALHA')
    B30_Cabine_Ocupada = comm.Read('B30_CABINE_OCUPADA')
    B30_Tempo_Ciclo_Excedido = comm.Read('B30_TEMPO_CICLO_EXCEDIDO')
    B30_Ciclo = comm.Read('B30_CICLO') 

    '''
    #PRINT PARA EFEITO DE TESTE
    print('***Dados Analogicos B10***')
    print('B10 Temperatura do Banho:', B10_TempBanho_Real.Value)
    print('B10 Temperatura do Motor M1:', B10_Temp_M1_Real.Value) 
    print('B10 Pressao:', B10_Pressao_Real.Value) 
    print('B10 Nivel do Tanque:', B10_Nivel_Real.Value) 
    print('B10 Frequencia do Inversor:', B10_Frequencia_Real.Value)
    print('B10 Tensao do Inversor:', B10_Tensao_Real.Value) 
    print('B10 Corrente do Inversor:', B10_Corrente_Real.Value) 
    print('B10 Vibracao do Motor M1:', B10_Vibracao_Real.Value) 

    print('Dados Analogicos B20')
    print('B20 Temperatura do Banho:', B20_TempBanho_Real.Value)
    print('B20 Temperatura do Motor M1', B20_Temp_M1_Real.Value) 
    print('B20 Pressao', B20_Pressao_Real.Value) 
    print('B20 Nivel do Tanque', B20_Nivel_Real.Value) 
    print('B20 Frequencia do Inversor', B20_Frequencia_Real.Value)
    print('B20 Tensao do Inversor', B20_Tensao_Real.Value) 
    print('B20 Corrente do Inversor', B20_Corrente_Real.Value) 
    print('B20 Vibracao do Motor M1', B20_Vibracao_Real.Value) 

    print('Dados Analogicos B30')
    print('B30 Temperatura do Banho:', B30_TempBanho_Real.Value)
    print('B30 Temperatura do Motor M1', B30_Temp_M1_Real.Value) 
    print('B30 Pressao', B30_Pressao_Real.Value) 
    print('B30 Nivel do Tanque', B30_Nivel_Real.Value) 
    print('B30 Frequencia do Inversor', B30_Frequencia_Real.Value)
    print('B30 Tensao do Inversor', B30_Tensao_Real.Value) 
    print('B30 Corrente do Inversor', B30_Corrente_Real.Value) 
    print('B30 Vibracao do Motor M1', B30_Vibracao_Real.Value) 
    print('Modelo Presente na Cabine', Modelo.Value)
    
    
    print('***Dados Digitais Cabine***')
    print('Cabine Emergencia:', Cabine_Emergencia.Value) 
    print('Cabine Manual:', Cabine_Manual.Value) 
    print('Cabine Automatico:', Cabine_Automatico.Value)  
    print('Cabine Porta de Entrada Aberta:', Cabine_Porta_Entrada_Aberta.Value)  
    print('Cabine Porta de Entrada Fechada:', Cabine_Porta_Entrada_Fechada.Value)  
    print('Cabine Porta de Entrada em Falha:', Cabine_Porta_Entrada_Falha.Value)  
    print('Cabine Porta de Saida Aberta:', Cabine_Porta_Saida_Aberta.Value) 
    print('Cabine Porta de Saida Fechada:', Cabine_Porta_Saida_Fechada.Value) 
    print('Cabine Porta de Saida em Falha:', Cabine_Porta_Saida_Falha.Value)

    print('***Dados Digitais B10***')
    print('B10 Valvula Spray Aberta:', B10_Valvula_Spray_Aberta.Value)  
    print('B10 Valvula Spray Fechada:', B10_Valvula_Spray_Fechada.Value)  
    print('B10 Valvula Spray em Falha:', B10_Valvula_Spray_Falha.Value)  
    print('B10 Valvula Escoamento Aberta:', B10_Valvula_Escoamento_Aberta.Value)  
    print('B10 Valvula Escoamento Fechada:', B10_Valvula_Escoamento_Fechada.Value)  
    print('B10 Valvula Escoamento em Falha:', B10_Valvula_Escoamento_Falha.Value) 
    print('B10 Inversor em Falha:', B10_Inversor_Falha.Value)  
    print('B10 Cabine em Falha:', B10_Cabine_Falha.Value) 
    print('B10 Cabine Ocupada:', B10_Cabine_Ocupada.Value) 
    print('B10 Tempo de Ciclo Excedido:', B10_Tempo_Ciclo_Excedido.Value) 
    print('B10 Cabine em Ciclo:', B10_Ciclo.Value) 
    
    print('Dados Digitais B20')
    print('B20 Valvula Spray Aberta', B20_Valvula_Spray_Aberta.Value)  
    print('B20 Valvula Spray Fechada', B20_Valvula_Spray_Fechada.Value)  
    print('B20 Valvula Spray em Falha', B20_Valvula_Spray_Falha.Value)  
    print('B20 Valvula Escoamento Aberta', B20_Valvula_Escoamento_Aberta.Value)  
    print('B20 Valvula Escoamento Fechada', B20_Valvula_Escoamento_Fechada.Value)  
    print('B20 Valvula Escoamento em Falha', B20_Valvula_Escoamento_Falha.Value) 
    print('B20 Inversor em Falha', B20_Inversor_Falha.Value)  
    print('B20 Cabine em Falha', B20_Cabine_Falha.Value) 
    print('B20 Tempo de Ciclo Excedido', B20_Tempo_Ciclo_Excedido.Value) 
    print('B20 Cabine em Ciclo', B20_Ciclo.Value) 

    print('Dados Digitais B30')
    print('B30 Valvula Spray Aberta', B30_Valvula_Spray_Aberta.Value)  
    print('B30 Valvula Spray Fechada', B30_Valvula_Spray_Fechada.Value)  
    print('B30 Valvula Spray em Falha', B30_Valvula_Spray_Falha.Value)  
    print('B30 Valvula Escoamento Aberta', B30_Valvula_Escoamento_Aberta.Value)  
    print('B30 Valvula Escoamento Fechada', B30_Valvula_Escoamento_Fechada.Value)  
    print('B30 Valvula Escoamento em Falha', B30_Valvula_Escoamento_Falha.Value) 
    print('B30 Inversor em Falha', B30_Inversor_Falha.Value)  
    print('B30 Cabine em Falha', B30_Cabine_Falha.Value) 
    print('B30 Tempo de Ciclo Excedido', B30_Tempo_Ciclo_Excedido.Value) 
    print('B30 Cabine em Ciclo', B30_Ciclo.Value) 
    '''
    x=2

 
timestamp = (time.time_ns())

# Dicionário em Python
datajson = {
    "B10": {
        "Dados_Analogicos": {
            "B10_Temperatura_Banho": B10_TempBanho_Real.Value,
            "B10_Temperatura_Motor": B10_Temp_M1_Real.Value,
            "B10_Vibracao_do_Motor": B10_Vibracao_Real.Value,
            "B10_Pressao": B10_Pressao_Real.Value,
            "B10_Nivel_do_Tanque": B10_Nivel_Real.Value,
            "B10_Frequencia_do_Inversor": B10_Frequencia_Real.Value,
            "B10_Tensao_do_Inversor": B10_Tensao_Real.Value,
            "B10_Corrente_do_Inversor": B10_Corrente_Real.Value
        },
        "Dados_Digitais": {
            "B10_Valvula_Spray_Fechada": int(B10_Valvula_Spray_Fechada.Value),
            "B10_Valvula_Spray_Aberta": int(B10_Valvula_Spray_Aberta.Value),
            "B10_Valvula_Spray_em_Falha": int(B10_Valvula_Spray_Falha.Value),
            "B10_Valvula_Escoamento_Fechada": int(B10_Valvula_Escoamento_Fechada.Value),
            "B10_Valvula_Escoamento_Aberta": int(B10_Valvula_Escoamento_Aberta.Value),
            "B10_Valvula_Escoamento_em_Falha": int(B10_Valvula_Escoamento_Falha.Value),
            "B10_Inversor_em_Falha": int(B10_Inversor_Falha.Value),
            "B10_Cabine_em_Falha": int(B10_Cabine_Falha.Value),
            "B10_Cabine_Ocupada": int(B10_Cabine_Ocupada.Value),
            "B10_Tempo_de_Ciclo_Excedido": int(B10_Tempo_Ciclo_Excedido.Value),
            "B10_Cabine_em_Ciclo": int(B10_Ciclo.Value)
        }
    },
    "B20": {
        "Dados_Analogicos": {
            "B20_Temperatura_Banho": B20_TempBanho_Real.Value,
            "B20_Temperatura_Motor": B20_Temp_M1_Real.Value,
            "B20_Vibracao_do_Motor": B20_Vibracao_Real.Value,
            "B20_Pressao": B20_Pressao_Real.Value,
            "B20_Nivel_do_Tanque": B20_Nivel_Real.Value,
            "B20_Frequencia_do_Inversor": B20_Frequencia_Real.Value,
            "B20_Tensao_do_Inversor": B20_Tensao_Real.Value,
            "B20_Corrente_do_Inversor": B20_Corrente_Real.Value
        },
        "Dados_Digitais": {
            "B20_Valvula_Spray_Fechada": int(B20_Valvula_Spray_Fechada.Value),
            "B20_Valvula_Spray_Aberta": int(B20_Valvula_Spray_Aberta.Value),
            "B20_Valvula_Spray_em_Falha": int(B20_Valvula_Spray_Falha.Value),
            "B20_Valvula_Escoamento_Fechada": int(B20_Valvula_Escoamento_Fechada.Value),
            "B20_Valvula_Escoamento_Aberta": int(B20_Valvula_Escoamento_Aberta.Value),
            "B20_Valvula_Escoamento_em_Falha": int(B20_Valvula_Escoamento_Falha.Value),
            "B20_Inversor_em_Falha": int(B20_Inversor_Falha.Value),
            "B20_Cabine_em_Falha": int(B20_Cabine_Falha.Value),
            "B20_Cabine_Ocupada": int(B20_Cabine_Ocupada.Value),
            "B20_Tempo_de_Ciclo_Excedido": int(B20_Tempo_Ciclo_Excedido.Value),
            "B20_Cabine_em_Ciclo": int(B20_Ciclo.Value)
        }
    },
    "B30": {
        "Dados_Analogicos": {
            "B30_Temperatura_Banho": B30_TempBanho_Real.Value,
            "B30_Temperatura_Motor": B30_Temp_M1_Real.Value,
            "B30_Vibracao_do_Motor": B30_Vibracao_Real.Value,
            "B30_Pressao": B30_Pressao_Real.Value,
            "B30_Nivel_do_Tanque": B30_Nivel_Real.Value,
            "B30_Frequencia_do_Inversor": B30_Frequencia_Real.Value,
            "B30_Tensao_do_Inversor": B30_Tensao_Real.Value,
            "B30_Corrente_do_Inversor": B30_Corrente_Real.Value
        },
        "Dados_Digitais": {
            "B30_Valvula_Spray_Fechada": int(B30_Valvula_Spray_Fechada.Value),
            "B30_Valvula_Spray_Aberta": int(B30_Valvula_Spray_Aberta.Value),
            "B30_Valvula_Spray_em_Falha": int(B30_Valvula_Spray_Falha.Value),
            "B30_Valvula_Escoamento_Fechada": int(B30_Valvula_Escoamento_Fechada.Value),
            "B30_Valvula_Escoamento_Aberta": int(B30_Valvula_Escoamento_Aberta.Value),
            "B30_Valvula_Escoamento_em_Falha": int(B30_Valvula_Escoamento_Falha.Value),
            "B30_Inversor_em_Falha": int(B30_Inversor_Falha.Value),
            "B30_Cabine_em_Falha": int(B30_Cabine_Falha.Value),
            "B30_Cabine_Ocupada": int(B30_Cabine_Ocupada.Value),
            "B30_Tempo_de_Ciclo_Excedido": int(B30_Tempo_Ciclo_Excedido.Value),
            "B30_Cabine_em_Ciclo": int(B30_Ciclo.Value)
        }  
    },
    "timestamp": timestamp  
}

# Serializar para JSON
json_data = json.dumps(datajson)

# Configuração do Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Servidor Kafka 
    'client.id': 'plc_producer',
}

consumer_config = {

    'bootstrap.servers': 'localhost:9092',
    'group.id': 'plc_consumer_group',
    'auto.offset.reset': 'latest',  

}

# Inicializar o produtor Kafka
producer = Producer(kafka_config)
value_serializer=lambda v: json.dumps(v).encode('utf-8')

# Função de callback para confirmar entrega
def delivery_report(err, msg):
    if err is not None:
        print(f"Erro ao enviar mensagem: {err}")
    else:
        print(f"Mensagem enviada para {msg.topic()} [partição {msg.partition()}]")

timestamp = time.time()

data_to_send = {
     json_data
    }

print(data_to_send)

start_envio = time.perf_counter() 
start_latencia = time.time()

# Enviar mensagens ao tópico
topic_name = 'teste2'

for data in data_to_send:
    # Converter o dicionário para JSON
    producer.produce(topic_name, value=str(data), callback=delivery_report)

# Garantir envio de todas as mensagens
producer.flush()

""" 

#Logica para calculo de metricas de desempenho
end_envio = time.perf_counter()

tempo_envio = (end_envio - start_envio)*1000

fim = time.perf_counter()
tempo_execucao = (fim - inicio)*1000

# Inicializa o consumidor Kafka
consumer = Consumer(consumer_config)

# Inscreve-se no tópico
topic_name = 'teste2'
consumer.subscribe([topic_name])
print(f"Tempo de envio: {tempo_envio}")
print(f"Tempo de execucao): {tempo_execucao}")

# Processa as mensagens e aguarda envio continuo

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Aguarda até 1 segundo para uma mensagem

        if msg is None:
            continue
            # print("Nenhuma mensagem recebida.")
        elif msg.error():
            print(f"Erro na mensagem: {msg.error()}")
        else:
            # Decodifica a mensagem JSON recebida
            mensagem = json.loads(msg.value().decode('utf-8'))
            timestamp_recebido = mensagem.get('timestamp', None)

            if timestamp_recebido:
                # Calcula o timestamp local no momento de recebimento da mensagem
                timestamp_local = time.time_ns()

                # Calcula a latência
                latencia = (timestamp_local - timestamp_recebido)/1000000

                
                #print(f"Mensagem recebida: {mensagem}")
                print(f"timestamp_local: {timestamp_local}")
                #print(f"Tempo de execucao): {tempo_execucao}")
                print(f"Latência: {latencia} ms")
            else:
                print("Mensagem sem timestamp.")
                
except KeyboardInterrupt:
    print("Interrompido pelo usuário.")
finally:
    # Fechar o consumidor após o processamento
    consumer.close()

 """

