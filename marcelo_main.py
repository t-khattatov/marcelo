import requests
import pandas as pd
import random
from datetime import date, datetime, timedelta
import time
import json

TOKEN = '1ac60e98-0396-48b4-a9fa-bd63dc47da7c'


def return_internal_id(cnpj):
    time.sleep(random.random())
    url = "https://api-v2.idwall.co/relatorios"
    json = {
        "matriz": "alice_bgc_conmpliance_micro_pj",
        "parametros": {
            "cnpj_numero": cnpj
        }
    }
    headers = {'Authorization': f"{TOKEN}"}
    r = requests.post(url, headers=headers, json=json)
    if r.ok:
        return r.json()['result']['numero']
    else:
        return ''


def find_answer_valid(numero):
    if not numero:
        return dict()
    headers = {'Authorization': f"{TOKEN}"}
    r = requests.get(f"https://api-v2.idwall.co/relatorios/{numero}/validacoes", headers=headers)
    if r.ok:
        return r.json()['result']

    else:
        print(r.status_code)
        return dict()


def find_answer_data(numero):
    time.sleep(random.random())

    if not numero:
        return dict()
    headers = {'Authorization': f"{TOKEN}"}
    r = requests.get(f'https://api-v2.idwall.co/relatorios/{numero}/dados', headers=headers)
    if r.ok:

        return r.json()['result']
    else:
        print(r.status_code)
        return dict()


def read_excel(file_name, sheets):
    df_list = list()
    for sheet in sheets:
        df = pd.read_excel(file_name, sheet_name=sheet)
        df['type'] = sheet
        df_list.append(df)
    return pd.concat(df_list, ignore_index=True)


if __name__ == '__main__':
    sheets = ['BGC Micro', 'BGC PME']  # 'Check MEI',
    xlsx_name = 'marcelo.xlsx'
    output_file_name_to_share = 'marcelo_out.xlsx'  # could be C:/Users/marcelo/
    output_file_for_logs = f'out_{date.today().strftime("%Y-%m-%d")}.xlsx'

    # real_shit
    df = read_excel(file_name=xlsx_name, sheets=sheets)
    df['request_id'] = df['CNPJ'].apply(return_internal_id)
    df['valid_result'] = None
    df['valid_finished'] = False
    df['data_result'] = None
    df['data_finished'] = False
    df.loc[0, 'valid_result'] = False
    time_start = datetime.now()
    while (not all(df['valid_finished']) or not all(df['data_finished'])) and datetime.now() - time_start < timedelta(
            minutes=10):
        number_of_request = 0
        for i in df.index:
            print('Number of requests in the queue', number_of_request)
            print('Unfinished jobs', sum(~df['valid_finished']) + sum(~df['data_finished']))
            request_id = df.loc[i, "request_id"]
            if not request_id:
                df.loc[i, 'valid_finished'] = True
                df.loc[i, 'data_finished'] = True

                continue

            if not df.loc[i, 'valid_finished']:

                valid_ans = find_answer_valid(request_id)
                number_of_request += 1
                if valid_ans.get('status', '') != 'PROCESSANDO':
                    df.loc[i, 'valid_finished'] = True
                    df.loc[i, 'valid_result'] = json.dumps(valid_ans)
                    number_of_request -= 1
                else:
                    df.loc[i, 'valid_result'] = json.dumps(valid_ans)
            if number_of_request >= 19:
                time.sleep(30)

                break

            if not df.loc[i, 'data_finished']:

                ans = find_answer_data(request_id)
                number_of_request += 1
                if ans.get('status', '') != 'PROCESSANDO':
                    df.loc[i, 'data_finished'] = True
                    df.loc[i, 'data_result'] = json.dumps(ans)
                    number_of_request -= 1
                else:
                    df.loc[i, 'data_result'] = json.dumps(ans)
        time.sleep(30)
        print('Number of requests in the queue', number_of_request)
        print('Unfinished jobs', sum(~df['valid_finished']) + sum(~df['data_finished']))

    df['resultado'] = df['valid_result'].apply(
        lambda x: json.loads(x).get('resultado', 'API ERROR') if x else 'API ERROR')
    df['atividade_principal'] = df['data_result'].apply(
        lambda x: json.loads(x).get('cnpj', dict()).get('atividade_principal', 'API ERROR') if x else 'API ERROR')
    df['atividades_secundarias'] = df['data_result'].apply(
        lambda x: json.loads(x).get('cnpj', dict()).get('atividades_secundarias', ['API ERROR']) if x else [
            'API ERROR'])
    df['atividades_secundarias'] = df['atividades_secundarias'].apply(lambda x: ', '.join(x))
    df.to_excel(output_file_for_logs)
    df[['CNPJ', 'type', 'resultado', 'atividade_principal', 'atividades_secundarias']].to_excel(
        output_file_name_to_share)