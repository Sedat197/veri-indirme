import datetime
import json
import threading
import time
import urllib
from datetime import datetime
from datetime import datetime as dt
from decimal import Decimal
import pandas as pd
import requests
from binance import Client
from binance.enums import HistoricalKlinesType

yeniKacBar = 10
toplamVeri = 2000
BASE_URL = 'https://fapi.binance.com/'
periyotlar = ['1m', '5m', '15m', '30m', '1h', '4h', '1d']

baslik = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume',
          'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Can be ignored']
aralik = [0, 50]

while True:
    try:
        client = Client(None, None)
        break
    except requests.exceptions.ConnectionError or urllib.error.URLError:
        print('HATA - Baglanti yok. 5 sn sonra tekrar deniyorum', datetime.now().strftime("%X"))
        time.sleep(5)


def getAllSymbols():
    while True:
        try:
            response = urllib.request.urlopen(f"{BASE_URL}fapi/v1/exchangeInfo").read()
            tumSemboller = list(map(lambda symbol: symbol['symbol'], json.loads(response)['symbols']))
            break
        except:
            time.sleep(5)
    return tumSemboller


def getOnlyUSDTSymbols():
    symbols = []
    for symbol in getAllSymbols():
        if symbol[-4:] == 'USDT':
            symbols.append(symbol)
    return symbols


def yuvarlaKatina(anaSayi, katlari, yon):
    # '1' yukari yuvarlama
    # '0' yuvarlama
    # '-1' asagi yuvarlama
    yuvarlanmis = float(Decimal(str(round(anaSayi / katlari, 0) * katlari)).quantize(Decimal(str(katlari))))
    if yon == 1:
        if yuvarlanmis < anaSayi:
            yuvarlanmis += katlari
    if yon == -1:
        if yuvarlanmis > anaSayi:
            yuvarlanmis -= katlari
    return float(Decimal(str(yuvarlanmis)).quantize(Decimal(str(katlari))))


def zamanHesapla(timestamp):
    return dt.fromtimestamp(timestamp / 1000)


def verileriGetir(sembol, periyot, baslangic, bitis):
    while True:
        try:
            mumlar = client.get_historical_klines(sembol, periyot, baslangic, bitis,
                                                  klines_type=HistoricalKlinesType.FUTURES)
            mumlar = pd.DataFrame(mumlar, columns=baslik, dtype=float)
            tarihler = pd.Series(map(lambda x: zamanHesapla(x).date(), mumlar['Open time']))
            saatler = pd.Series(map(lambda x: zamanHesapla(x).time(), mumlar['Open time']))
            mumlar['tarih'] = tarihler
            mumlar['saat'] = saatler
            mumlar['Open time'] = pd.Series(map(lambda x: int(x), mumlar['Open time']))
            mumlar['Close time'] = pd.Series(map(lambda x: int(x), mumlar['Close time']))
            mumlar['Number of trades'] = pd.Series(map(lambda x: int(x), mumlar['Number of trades']))
            mumlar['Can be ignored'] = pd.Series(map(lambda x: int(x), mumlar['Can be ignored']))

            mumlar = mumlar[
                ['tarih', 'saat', 'Open', 'High', 'Low', 'Close', 'Open time', 'Close time', 'Number of trades',
                 'Volume', 'Quote asset volume', 'Taker buy base asset volume', 'Taker buy quote asset volume',
                 'Can be ignored']]
            break
        except requests.exceptions.ConnectionError or urllib.error.URLError:
            print('HATA - Baglanti yok. 5 sn sonra tekrar deniyorum', datetime.now().strftime("%X"))
            time.sleep(5)
        except requests.exceptions.ReadTimeout:
            print('HATA - Read Time Out hatasi. Binance kaynakli. 2 sn sonra tekrar deniyorum',
                  datetime.now().strftime("%X"))
            time.sleep(2)
        except requests.exceptions.JSONDecodeError or json.decoder.JSONDecodeError:
            print('HATA - Json Decode hatasi. Binance kaynakli. 2 sn sonra tekrar deniyorum',
                  datetime.now().strftime("%X"))
            time.sleep(2)
        except UnboundLocalError:
            print('HATA - Unbound hatasi. Binance kaynakli. 2 sn sonra tekrar deniyorum', datetime.now().strftime("%X"))
            time.sleep(2)
    return mumlar


def all_symbols():
    response = client.futures_exchange_info()
    return list(map(lambda symbol: symbol['symbol'], response['symbols']))


def barKapanisinaKalanSure(periyotK):
    saat = datetime.now().hour
    if saat == 0:
        saat = 22
    elif saat == 1:
        saat = 23
    else:
        saat -= 2
    dakika = datetime.now().minute
    saniye = datetime.now().second
    aktif_saniye = saat * 60 * 60 + dakika * 60 + saniye
    kalan_sure = periyotK - (aktif_saniye - int(aktif_saniye / periyotK) * periyotK)
    return kalan_sure


def sonrakiBariBekle(periyotK, Coin):
    sure = barKapanisinaKalanSure(periyotK)
    print(Coin, datetime.now().strftime("%d/%m/%Y %H:%M:%S"), ' ', sure, ' sn beklemedeyim')
    time.sleep(sure + 6)


def istenenVerileriHazirla(Coin, periyot, aktifBarTS, sonBarTS_1m, sonBarTS, tur):
    if periyot == '1m':
        periyotK = 60
    elif periyot == '5m':
        periyotK = 300
    elif periyot == '15m':
        periyotK = 900
    elif periyot == '30m':
        periyotK = 1800
    elif periyot == '1h':
        periyotK = 3600
    elif periyot == '4h':
        periyotK = 14400
    elif periyot == '1d':
        periyotK = 86400
    if tur == 'dakikalik paket':
        df = verileriGetir(Coin, '1m', sonBarTS_1m - 60000 * yeniKacBar, sonBarTS_1m - 1)
    elif tur == 'dosya yok':
        if aktifBarTS == sonBarTS_1m:
            df = verileriGetir(Coin, periyot, aktifBarTS - periyotK * 1000 * toplamVeri, aktifBarTS - 1)
        else:
            df1 = pd.read_csv('Veriler/1m/' + Coin + '_1m.csv')
            df = verileriGetir(Coin, periyot, aktifBarTS - periyotK * 1000 * toplamVeri + 1, aktifBarTS)
            for satir in range(len(df1)):
                if df['Open time'][len(df) - 1] == df1['Open time'][satir]:
                    df.loc[len(df) - 1, 'High'] = max(df1[satir:len(df1)]['High'].values.tolist())
                    df.loc[len(df) - 1, 'Low'] = min(df1[satir:len(df1)]['Low'].values.tolist())
                    df.loc[len(df) - 1, 'Close'] = df1['Close'][len(df1) - 1]
                    df.loc[len(df) - 1, 'Volume'] = yuvarlaKatina(sum(df1[satir:len(df1)]['Volume'].values.tolist()),
                                                                  0.001, 0)
                    df.loc[len(df) - 1, 'Close time'] = int(df1['Close time'][len(df1) - 1])
                    df.loc[len(df) - 1, 'Quote asset volume'] = yuvarlaKatina(
                        sum(df1[satir:len(df1)]['Quote asset volume'].values.tolist()), 0.0001, 0)
                    df.loc[len(df) - 1, 'Number of trades'] = int(
                        sum(df1[satir:len(df1)]['Number of trades'].values.tolist()))
                    df.loc[len(df) - 1, 'Taker buy base asset volume'] = yuvarlaKatina(
                        sum(df1[satir:len(df1)]['Taker buy base asset volume'].values.tolist()), 0.001, 0)
                    df.loc[len(df) - 1, 'Taker buy quote asset volume'] = yuvarlaKatina(
                        sum(df1[satir:len(df1)]['Taker buy quote asset volume'].values.tolist()), 0.0001, 0)
                    df.loc[len(df) - 1, 'Can be ignored'] = int(
                        sum(df1[satir:len(df1)]['Can be ignored'].values.tolist()))
                    break
    elif tur == 'eksik veri':
        if aktifBarTS == sonBarTS_1m:
            df = verileriGetir(Coin, periyot, sonBarTS + 1, aktifBarTS - 1)
        else:
            df1 = pd.read_csv('Veriler/1m/' + Coin + '_1m.csv')
            df = verileriGetir(Coin, periyot, sonBarTS + 1, aktifBarTS)
            for satir in range(len(df1)):
                if df['Open time'][len(df) - 1] == df1['Open time'][satir]:
                    df.loc[len(df) - 1, 'High'] = max(df1[satir:len(df1)]['High'].values.tolist())
                    df.loc[len(df) - 1, 'Low'] = min(df1[satir:len(df1)]['Low'].values.tolist())
                    df.loc[len(df) - 1, 'Close'] = df1['Close'][len(df1) - 1]
                    df.loc[len(df) - 1, 'Volume'] = yuvarlaKatina(sum(df1[satir:len(df1)]['Volume'].values.tolist()),
                                                                  0.001, 0)
                    df.loc[len(df) - 1, 'Close time'] = int(df1['Close time'][len(df1) - 1])
                    df.loc[len(df) - 1, 'Quote asset volume'] = yuvarlaKatina(
                        sum(df1[satir:len(df1)]['Quote asset volume'].values.tolist()), 0.0001, 0)
                    df.loc[len(df) - 1, 'Number of trades'] = int(
                        sum(df1[satir:len(df1)]['Number of trades'].values.tolist()))
                    df.loc[len(df) - 1, 'Taker buy base asset volume'] = yuvarlaKatina(
                        sum(df1[satir:len(df1)]['Taker buy base asset volume'].values.tolist()), 0.001, 0)
                    df.loc[len(df) - 1, 'Taker buy quote asset volume'] = yuvarlaKatina(
                        sum(df1[satir:len(df1)]['Taker buy quote asset volume'].values.tolist()), 0.0001, 0)
                    df.loc[len(df) - 1, 'Can be ignored'] = int(
                        sum(df1[satir:len(df1)]['Can be ignored'].values.tolist()))
                    break
    if df.empty is False:
        return df


def veriTamamla(Coin):
    while True:
        try:
            anlikTS = int(time.time())
            # periyotlari tamamlama
            for periyot in periyotlar:
                if periyot == '1m':
                    periyotK = 60
                    while True:
                        aktifBarTS_1m = int(int(anlikTS / 60) * 60000)
                        df1 = istenenVerileriHazirla(Coin, '', '', aktifBarTS_1m, '', 'dakikalik paket')
                        if df1['Open time'][len(df1) - 1] == aktifBarTS_1m - 60000:
                            break
                        else:
                            print(Coin + ' - yeni veri yok tekrar deniyorum')
                            time.sleep(2)
                elif periyot == '5m':
                    periyotK = 300
                elif periyot == '15m':
                    periyotK = 900
                elif periyot == '30m':
                    periyotK = 1800
                elif periyot == '1h':
                    periyotK = 3600
                elif periyot == '4h':
                    periyotK = 14400
                elif periyot == '1d':
                    periyotK = 86400
                yol = 'Veriler/' + periyot + '/'
                dosyaAdi = Coin + '_' + periyot + '.csv'
                aktifBarTS = int(int(anlikTS / periyotK) * periyotK * 1000)
                try:
                    veri = pd.read_csv(yol + dosyaAdi)
                    sonBarTS = int(veri['Open time'][len(veri) - 1])
                    sonrakiBarTS = int(sonBarTS + periyotK * 1000)
                    sonGuncellemeTS = int(veri['Close time'][len(veri) - 1])
                    # Bosluk varsa boslugu kapat
                    if df1['Open time'][0] > sonGuncellemeTS:
                        veri = veri.drop(labels=range(len(veri) - 1, len(veri)), axis=0)
                        veri = veri.reset_index(drop=True)
                        sonBarTS = int(veri['Open time'][len(veri) - 1])
                        df = istenenVerileriHazirla(Coin, periyot, aktifBarTS, aktifBarTS_1m, sonBarTS, 'eksik veri')
                        df = pd.concat([veri, df], ignore_index=True, axis=0)
                        df = df.reset_index(drop=True)
                    else:
                        df = veri.copy()
                        for satir in range(len(df1)):
                            if df1['Open time'][satir] == sonrakiBarTS:
                                df = pd.concat([df, df1[satir:satir + 1]], ignore_index=True, axis=0)
                                df = df.reset_index(drop=True)
                                sonBarTS = int(df['Open time'][len(df) - 1])
                                sonrakiBarTS = int(sonBarTS + periyotK * 1000)
                                sonGuncellemeTS = int(df['Close time'][len(df) - 1])
                            elif df1['Open time'][satir] > sonGuncellemeTS:
                                if df1['High'][satir] > df['High'][len(df) - 1]:
                                    df.loc[len(df) - 1, 'High'] = df1['High'][satir]
                                if df1['Low'][satir] < df['Low'][len(df) - 1]:
                                    df.loc[len(df) - 1, 'Low'] = df1['Low'][satir]
                                df.loc[len(df) - 1, 'Close'] = df1['Close'][satir]
                                df.loc[len(df) - 1, 'Volume'] = yuvarlaKatina(
                                    df.loc[len(df) - 1, 'Volume'] + df1['Volume'][satir], 0.001, 0)
                                df.loc[len(df) - 1, 'Close time'] = int(df1['Close time'][satir])
                                sonGuncellemeTS = int(df1['Close time'][satir])
                                df.loc[len(df) - 1, 'Quote asset volume'] = yuvarlaKatina(
                                    df.loc[len(df) - 1, 'Quote asset volume'] + df1['Quote asset volume'][satir],
                                    0.0001, 0)
                                df.loc[len(df) - 1, 'Number of trades'] = int(
                                    df.loc[len(df) - 1, 'Number of trades'] + df1['Number of trades'][satir])
                                df.loc[len(df) - 1, 'Taker buy base asset volume'] = yuvarlaKatina(
                                    df.loc[len(df) - 1, 'Taker buy base asset volume'] +
                                    df1['Taker buy base asset volume'][satir], 0.001, 0)
                                df.loc[len(df) - 1, 'Taker buy quote asset volume'] = yuvarlaKatina(
                                    df.loc[len(df) - 1, 'Taker buy quote asset volume'] +
                                    df1['Taker buy quote asset volume'][satir], 0.0001, 0)
                                df.loc[len(df) - 1, 'Can be ignored'] = int(
                                    df.loc[len(df) - 1, 'Can be ignored'] + df1['Can be ignored'][satir])
                except FileNotFoundError:
                    # 2000 bar öncesi verileri indir
                    df = istenenVerileriHazirla(Coin, periyot, aktifBarTS, aktifBarTS_1m, '', 'dosya yok')
                while True:
                    if len(df) > toplamVeri:
                        df = df.drop(labels=range(0, 1), axis=0)
                        df = df.reset_index(drop=True)
                    else:
                        break
                df.to_csv(yol + dosyaAdi, index=False)
            sonrakiBariBekle(60, Coin)
        except:
            time.sleep(10)

if __name__ == "__main__":
    coinler = getOnlyUSDTSymbols()
    coinler.sort()
    no = 0
    for coin in coinler:
        no += 1
        if aralik[0] <= no <= aralik[1]:
            locals()['target_' + coin] = threading.Thread(target=veriTamamla, args=(coin,))
    no = 0
    for coin in coinler:
        no += 1
        if aralik[0] <= no <= aralik[1]:
            locals()['target_' + coin].start()
    no = 0
    for coin in coinler:
        no += 1
        if aralik[0] <= no <= aralik[1]:
            locals()['target_' + coin].join()
