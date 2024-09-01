import mysql.connector
from solana.rpc.api import Client as SolClient
from solders.pubkey import Pubkey
from solders.signature import Signature
import json
import clean_sum
from datetime import datetime, timezone


def fetch_test(transactions, client, market_dict, url, headers, cur, myDb):
    data = []
    for tx in transactions:
        signature = Signature.from_string(tx)
        data.append(await client.client.get_transaction(signature, "jsonParsed", "confirmed", 0))
    for i in range(len(data)):
        wallet = "unknown"
        for accts in json.loads(data[i].to_json())['result']['accounts']:
            if accts['signer']:
                wallet = accts['pubkey']
                break
        tx_sig = json.loads(data[i].to_json())['result'][0]['sigs'][0]
        blocktime = json.loads(data[i].to_json())['result'][0]['blockTime']
        buy = False
        for j in range(1, len(json.loads(data[i].to_json())['result'])):
            if 'pure' in json.loads(data[i].to_json())['result'][j].keys():
                buy_price = json.loads(data[i].to_json())['result'][j]['pure'] / 1E9
                buy = True
                break
        for msgs in json.loads(data[i].to_json())['result'][0]['messages']:
            sft = ""
            mark = ""
            if "OrderSummary" in msgs:
                order_sum = clean_sum(msgs)
                order_dict = json.loads(order_sum)
                base_qty = order_dict['qty']
                for mint in json.loads(data[i].to_json()):
                    for sfts in market_dict.keys():
                        if mint['mint'] == market_dict[sfts]:
                            sft = sfts
                            mark = mint['mint']
                            break
                    break
                if buy:
                    url += f'{blocktime - 100000}&time_to={blocktime + 100000}'
                    response = requests.get(url, headers=headers)
                    bird_data = json.loads(response.text)
                    sol_price = (bird_data[0]['value'] + bird_data[1]['value']) / 2
                    usdc = sol_price * (buy_price / base_qty)
                    try:
                        cur.execute(f'INSERT INTO meData(txSig, market, timestamp, kind, qty, price, walletBy, item, usdc) '
                                    f'VALUES("{tx_sig}", "{mark}", {blocktime}, "buy", {base_qty}, {buy_price / base_qty}, '
                                    f'"{wallet}", "{sft}", {usdc})')
                        myDB.commit()
                    except:
                        pass  # ignore any duplicates
                break
