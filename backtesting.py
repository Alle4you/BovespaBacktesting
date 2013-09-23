import datetime
import re
import StringIO
import urllib2
import xml.etree
import zipfile
import os
import pickle

DATA_PATH = 'data'
QUOTE_FILE_NAME = 'quote'
QUOTE_WEEK_FILE_NAME = 'week'
QUOTE_MONTH_FILE_NAME = 'month'


def import_quote_from_jgrafix(dataPath):
    """Importa informacoes da cotacao historica do aplicativo JGrafix."""

    def read_quotedoc(path):
        print path
        ret = []
        lines = list(open(path, 'r'))
        for currLine in lines:
            m = re.search(r'(\d+/\d+/\d+) ([0-9.]+) ([0-9.]+) ([0-9.]+) ([0-9.]+) ([0-9]+) ([0-9.]+) ([0-9.]+)', currLine)
            if m:
                ret.append(m.group(1, 2, 3, 4, 5, 6, 7, 8))
        return ret

    def get_week_quote(quote):
        ret = { 'date': [], 'openPrice': [], 'minPrice': [], 'maxPrice': [], 'closePrice': [], 'volume': [] }
        get_week_quote.currWeekNumber = 0

        def init_week(quotePos):
            get_week_quote.currWeekNumber = quote['date'][quotePos].isocalendar()[1]
            ret['date'].append(quote['date'][quotePos])
            ret['openPrice'].append(quote['openPrice'][quotePos])
            ret['minPrice'].append(quote['minPrice'][quotePos])
            ret['maxPrice'].append(quote['maxPrice'][quotePos])
            ret['closePrice'].append(quote['closePrice'][quotePos])
            ret['volume'].append(quote['volume'][quotePos])

        def upd_week(quotePos):
            ret['minPrice'][-1] = min(ret['minPrice'][-1], quote['minPrice'][quotePos])
            ret['maxPrice'][-1] = max(ret['maxPrice'][-1], quote['maxPrice'][quotePos])
            ret['closePrice'][-1] = quote['closePrice'][quotePos]
            ret['volume'][-1] += quote['volume'][quotePos]

        for i in range(len(quote['date'])):
            weekNumber = quote['date'][i].isocalendar()[1]
            upd_week(i) if get_week_quote.currWeekNumber == weekNumber else init_week(i)
        return ret

    def get_month_quote(quote):
        ret = { 'date': [], 'openPrice': [], 'minPrice': [], 'maxPrice': [], 'closePrice': [], 'volume': [] }
        get_month_quote.currMonthNumber = 0

        def init_month(quotePos):
            get_month_quote.currMonthNumber = quote['date'][quotePos].month
            ret['date'].append(quote['date'][quotePos])
            ret['openPrice'].append(quote['openPrice'][quotePos])
            ret['minPrice'].append(quote['minPrice'][quotePos])
            ret['maxPrice'].append(quote['maxPrice'][quotePos])
            ret['closePrice'].append(quote['closePrice'][quotePos])
            ret['volume'].append(quote['volume'][quotePos])

        def upd_month(quotePos):
            ret['minPrice'][-1] = min(ret['minPrice'][-1], quote['minPrice'][quotePos])
            ret['maxPrice'][-1] = max(ret['maxPrice'][-1], quote['maxPrice'][quotePos])
            ret['closePrice'][-1] = quote['closePrice'][quotePos]
            ret['volume'][-1] += quote['volume'][quotePos]

        for i in range(len(quote['date'])):
            monthNumber = quote['date'][i].month
            upd_month(i) if get_month_quote.currMonthNumber == monthNumber else init_month(i)
        return ret

    from os.path import isfile, join
    filterPath = join(DATA_PATH, 'filterCodes')
    quoteDocs = [ f for f in os.listdir(dataPath) if isfile(join(dataPath, f)) ]
    filterCodes = [item.lower()[:-1] for item in list(open(filterPath, 'r'))]
    filterDocs = [ f for f in quoteDocs if f in filterCodes ]
    for doc in filterDocs:
        quote = read_quotedoc(join(dataPath, doc))
        if len(quote) > 0:
            destPath = join(DATA_PATH, doc)
            if not os.path.exists(destPath):
                os.makedirs(destPath)
            quoteDict = { 'date': [], 'openPrice': [], 'minPrice': [], 'maxPrice': [], 'closePrice': [], 'volume': [] }
            for quoteDay in quote:
                quoteDict['date'].append(datetime.date((int)(quoteDay[0][6:]), (int)(quoteDay[0][3:5]), (int)(quoteDay[0][0:2])) )
                quoteDict['openPrice'].append(float(quoteDay[1]))
                quoteDict['minPrice'].append(float(quoteDay[2]))
                quoteDict['maxPrice'].append(float(quoteDay[3]))
                quoteDict['closePrice'].append(float(quoteDay[4]))
                quoteDict['volume'].append(float(quoteDay[7]))
            destFile = join(destPath, QUOTE_FILE_NAME)
            write_data(doc, QUOTE_FILE_NAME, quoteDict)
            weekDict = get_week_quote(quoteDict)
            write_data(doc, QUOTE_WEEK_FILE_NAME, weekDict)
            monthDict = get_month_quote(quoteDict)
            write_data(doc, QUOTE_MONTH_FILE_NAME, monthDict)


def load_data(code, dataName):
    """Carrega informacoes armazenadas na pasta de dados."""
    from os.path import isfile, join
    ret = None
    code = code.lower()
    codePath = join(join(DATA_PATH, code), dataName)
    if os.path.exists(codePath):
        f = open(codePath, 'r')
        ret = pickle.load(f)
        f.close()
    return ret

def write_data(code, dataName, data):
    """Escreve informacoes para a pasta de dados."""
    from os.path import isfile, join
    code = code.lower()
    codePath = join(join(DATA_PATH, code), dataName)
    f = open(codePath, 'w')
    pickle.dump(data, f)
    f.close()

def load_quote_data(code):
    """Carrega historico de cotacao diaria do papel especificado."""
    return load_data(code, QUOTE_FILE_NAME)

def load_week_quote_data(code):
    """Carrega historico de cotacao semanal do papel especificado."""
    return load_data(code, QUOTE_WEEK_FILE_NAME)

def load_month_quote_data(code):
    """Carrega historico de cotacao mensal do papel especificado."""
    return load_data(code, QUOTE_MONTH_FILE_NAME)

def load_known_codes():
    """Retorna a lista de papeis conhecidos."""
    from os.path import isdir, join
    ret = [ f for f in os.listdir(DATA_PATH) if isdir(join(DATA_PATH, f)) ]
    return ret


class Trade:
    """Informacoes de um trade."""
    pass

class Money:
    """Informacoes do gerenciamento de dinheiro."""
    pass


def export_trades_to_csv(trades, filePath):

    def write_header(f):
        f.write("trade" + ';')
        f.write("begin" + ';')
        f.write("end" + ';')
        f.write("code" + ';')
        f.write("volume" + ';')
        f.write("qtd" + ';')
        f.write("buy" + ';')
        f.write("sell" + ';')
        f.write("result" + ';')
        f.write("stop" + ';')
        f.write("stop2")
        f.write('\n')

    f = open(filePath, 'w')
    write_header(f)
    for trade in trades:
        f.write(str(trade.trade) + ';')
        f.write(str(trade.begin) + ';')
        f.write(str(trade.end) + ';')
        f.write(str(trade.code) + ';')
        f.write(str(trade.volume) + ';')
        f.write(str(trade.qtd) + ';')
        f.write(str(trade.buy) + ';')
        f.write(str(trade.sell) + ';')
        f.write(str(trade.result) + ';')
        f.write(str(trade.stop) + ';')
        f.write(str(trade.stop2))
        f.write('\n')
    f.close()

def export_money_to_csv(money, filePath):

    def write_header(f):
        f.write("day;")
        f.write("cash;")
        f.write("invest;")
        f.write("equity;")
        f.write("risk;")
        f.write("buy;")
        f.write("sell;")
        f.write("wage;")
        f.write("deposit")
        #f.write("begin;")
        #f.write("end")
        f.write('\n')

    f = open(filePath, 'w')
    write_header(f)
    for m in money:
        f.write(str(m.day) + ';')
        f.write(str(m.cash) + ';')
        f.write(str(m.invest) + ';')
        f.write(str(m.equity) + ';')
        f.write(str(m.risk) + ';')
        f.write(str(m.buy) + ';')
        f.write(str(m.sell) + ';')
        f.write(str(m.wage) + ';')
        f.write(str(m.deposit))
        #f.write(str([x.trade for x in m.begin]) + ';')
        #f.write(str([x.trade for x in m.end]))
        f.write('\n')
    f.close()


def export_to_csv(data, filePath):
    f = open(filePath, 'w')
    maxLine = 100000000
    dataKeys = data.keys()
    for i in dataKeys:
        f.write(str(i) + ';')
        maxLine = min(maxLine, len(data[i]))
    f.write('\n')
    for i in range(0, maxLine):
        for k in dataKeys:
            f.write(str(data[k][i]) + ';')
        f.write('\n')
    f.close()


def sma(quote, days = 10):
    """Calculo de Simple Moving Average."""
    price = quote['closePrice']
    ret = []
    for i in range(len(price)):
        l = price[ max(i - days + 1, 0) : i + 1 ]
        ret.append( sum(l) / len(l) )
    return ret


def ema(quote, days = 10):
    """Calculo de Exponential Moving Average."""
    price = quote['closePrice']
    code_sma = sma(quote, days)
    ret = []
    for i in range(min(len(code_sma), days)):
        ret.append(code_sma[i])
    multiplier = ( 2.0 / (float(days) + 1.0) )
    for i in range(days, len(price)):
        ret.append( (price[i] - ret[i-1]) * multiplier + ret[i-1] )
    return ret


def macd(quote, shortDays = 12, longDays = 26, signalDays = 9):
    """Calculo de MACD."""
    price = quote['closePrice']
    shortMacd = ema(price, shortDays)
    longMacd = ema(price, longDays)
    macd = []
    for i in range(len(price)):
        macd.append(shortMacd[i] - longMacd[i])
    signal = ema(macd, signalDays)
    hist = []
    for i in range(len(price)):
        hist.append(macd[i] - signal[i])
    ret = { 'macd': macd, 'signal': signal, 'hist': hist }
    return ret

def stop_safeplace(quote, multiplier = 4):
    """Calculo de metodo de stop."""
    price = quote['minPrice']
    low = [ 0.0 ]
    for i in range(1, len(price)):
        low.append(price[i-1]-price[i] if price[i-1] > price[i] else 0.0)
    lowSum = []
    lowSumDays = 19
    for i in range(len(price)):
        l = low[ max(i - lowSumDays + 1, 0) : i + 1 ]
        lowSum.append( sum(l) )
    count = []
    for i in range(len(price)):
        count.append(1.0 if low[i] != 0.0 else 0.0)
    countSum = []
    for i in range(len(price)):
        l = count[ max(i - lowSumDays + 1, 0) : i + 1 ]
        countSum.append( sum(l) )
    stop = []
    for i in range(len(price)):
        stop.append(0.0 if countSum[i] == 0.0 else lowSum[i] / countSum[i])
    ret = []
    for i in range(len(price)):
        ret.append(price[i] if stop[i] == 0.0 else price[i] - (stop[i] * multiplier))
    #testRet = { 'low': low, 'lowSum': lowSum, 'count': count, 'countSum': countSum, 'stop': ret }
    return ret


def stop_atr(quote, multiplier = 3):
    """Calculo de metodo de stop."""
    minPrice = quote['minPrice']
    maxPrice = quote['maxPrice']
    closePrice = quote['closePrice']
    highLow = [ maxPrice[0] - minPrice[0] ]
    highClose = [ 0.0 ]
    lowClose = [ 0.0 ]
    tr = [ highLow[0] ]

    for i in range(1, len(minPrice)):
        highLow.append(maxPrice[i] - minPrice[i])
        highClose.append(abs(maxPrice[i] - closePrice[i-1]))
        lowClose.append(abs(minPrice[i] - closePrice[i-1]))
        tr.append(max(highLow[i], highClose[i], lowClose[i]))

    atr = []
    atrSumDays = 14
    ret = []
    for i in range(len(minPrice)):
        l = tr[ max(i - atrSumDays + 1, 0) : i + 1 ]
        atr.append( sum(l) / len(l) if len(l) < atrSumDays else (atr[i-1] * (atrSumDays-1) + tr[i]) / atrSumDays )
        l = minPrice[ max(i - atrSumDays + 1, 0) : i + 1 ]
        ret.append(max(l) - (atr[i] * multiplier))
    #testRet = { 'highLow': highLow, 'highClose': highClose, 'lowClose': lowClose, 'tr': tr, 'atr': atr, 'stop': ret }
    return ret




def trend(code):
    """Calculo de tendencia."""

    def isSelected(select, dt):
        ret = False
        for k, v in sorted(select.iteritems()):
            if k < dt: ret = v
        return ret

    quote = load_data(code, 'week')
    longTrend = ema(quote, 12)
    shortTrend = ema(quote, 6)
    trend = []
    for i in range(12): # ignorando primeiros dias
        trend.append('')
    for i in range(12, len(longTrend)):
            diff = shortTrend[i] - longTrend[i]
            if abs(diff) / ((shortTrend[i] + longTrend[i]) / 2.0) < 0.01: # diferenca menor que 1%
                trend.append('')
            else:
                trend.append('buy' if shortTrend[i] > longTrend[i] else 'sell')
    ret = { 'date': quote['date'], 'ema-12': longTrend, 'ema-6': shortTrend, 'trend': trend }
    return ret


class Trend:
    def __init__(self, quoteResolution = 'week', getLongTrend = ema, getShortTrend = ema, 
                longTrendResolution = 12, shortTrendResolution = 6):
        self.quoteResolution = quoteResolution
        self.getLongTrend = getLongTrend
        self.getShortTrend = getShortTrend
        self.longTrendResolution = longTrendResolution
        self.shortTrendResolution = shortTrendResolution

    def calc(self, code):
        """Calculo de tendencia (definindo funcoes para tendencia de longo e curto prazos)."""

        quote = load_data(code, self.quoteResolution)
        longTrend = self.getLongTrend(quote, self.longTrendResolution)
        shortTrend = self.getShortTrend(quote, self.shortTrendResolution)
        trend = []
        for i in range(self.longTrendResolution): # ignorando primeiros periodos
            trend.append('')
        for i in range(self.longTrendResolution, len(longTrend)):
                diff = shortTrend[i] - longTrend[i]
                if abs(diff) / ((shortTrend[i] + longTrend[i]) / 2.0) < 0.01: # diferenca menor que 1%
                    trend.append('')
                else:
                    trend.append('buy' if shortTrend[i] > longTrend[i] else 'sell')
        ret = { 'date': quote['date'], 'longTrend': longTrend, 'shortTrend': shortTrend, 'trend': trend }
        return ret


def signal(code):
    """Calculo de sinal de operacao."""
    quote = load_quote_data(code)
    longTrend = ema(quote, 24)
    shortTrend = ema(quote, 12)
    stop = stop_safeplace(quote)
    signal = []
    for i in range(24): # ignorando primeiros dias
        signal.append('')
    for i in range(24, len(longTrend)):
        diff = shortTrend[i] - longTrend[i]
        diffOk = abs(diff) / ((shortTrend[i] + longTrend[i]) / 2.0) > 0.01 # diferenca maior que 1%
        emaOk = True if shortTrend[i] > longTrend[i] else False
        stopOk = True if stop[i] < quote['minPrice'][i] else False
        signal.append('buy' if diffOk and emaOk and stopOk else '')
    ret = { 'date': quote['date'], 'ema-24': longTrend, 'ema-12': shortTrend, 'signal': signal, 'stop': stop, 'min': quote['minPrice'] }
    return ret


class Signal:
    def __init__(self, quoteResolution = 'quote', getLongSignal = ema, getShortSignal = ema, 
                longSignalResolution = 24, shortSignalResolution = 12, getStop = stop_safeplace):
        self.quoteResolution = quoteResolution
        self.getLongSignal = getLongSignal
        self.getShortSignal = getShortSignal
        self.longSignalResolution = longSignalResolution
        self.shortSignalResolution = shortSignalResolution
        self.getStop = stop_safeplace

    def calc(self, code):
        """Calculo de sinal de operacao."""
        quote = load_quote_data(code)
        longSignal = self.getLongSignal(quote, self.longSignalResolution)
        shortSignal = self.getShortSignal(quote, self.shortSignalResolution)
        stop = self.getStop(quote)
        signal = []
        for i in range(self.longSignalResolution): # ignorando primeiros dias
            signal.append('')
        for i in range(self.longSignalResolution, len(longSignal)):
            diff = shortSignal[i] - longSignal[i]
            diffOk = abs(diff) / ((shortSignal[i] + longSignal[i]) / 2.0) > 0.01 # diferenca maior que 1%
            signalOk = True if shortSignal[i] > longSignal[i] else False
            stopOk = True if stop[i] < quote['minPrice'][i] else False
            signal.append('buy' if diffOk and signalOk and stopOk else '')
        ret = { 'date': quote['date'], 'longSignal': longSignal, 'shortSignal': shortSignal, 'signal': signal, 'stop': stop, 'min': quote['minPrice'] }
        return ret


def calc_trades(code, trend, signal):
    ret = []
    quote = load_quote_data(code)
    code_trend = trend.calc(code)
    code_signal = signal.calc(code)
    tradeCount = 0

    for week in range(24, len(code_trend['trend'])):

        if code_trend['trend'][week] == 'buy': # semana de compras
            signalIdx = code_signal['date'].index(code_trend['date'][week]) # inicio da semana em dias
            lastDay = code_trend['date'][week+1] if week+1 < len(code_trend['date']) else datetime.date.today() # ultimo dia do trend atual

            while signalIdx < len(code_signal['signal']) and code_signal['date'][signalIdx] < lastDay: # enquanto houver dias e estivermos no trend atual

                if tradeCount > 0: # estamos comprados
                    if quote['minPrice'][signalIdx] <= ret[-1].stop2: # estourou o stop
                        for trades in range(-tradeCount, 0):
                            ret[trades].sell = quote['minPrice'][signalIdx] # usando pior preco para garantir papeis iliquidos
                            ret[trades].end = code_signal['date'][signalIdx]
                            ret[trades].result = ret[trades].sell - ret[trades].buy
                        tradeCount = 0
                        sigIdx = signalIdx
                        while sigIdx < len(code_signal['signal']) and code_signal['signal'][sigIdx] == 'buy':
                            code_signal['signal'][sigIdx] = ''
                            sigIdx = sigIdx + 1

                    elif code_signal['signal'][signalIdx] != 'buy': # hora de vender
                        for trades in range(-tradeCount, 0):
                            ret[trades].sell = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                            ret[trades].end = code_signal['date'][signalIdx]
                            ret[trades].result = ret[trades].sell - ret[trades].buy
                        tradeCount = 0

                    else: # apenas atualiza dados
                        for trades in range(-tradeCount, 0):
                            ret[trades].sell = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                            ret[trades].end = code_signal['date'][signalIdx]
                            ret[trades].result = ret[trades].sell - ret[trades].buy
                            stopSpread = quote['minPrice'][signalIdx] - ret[trades].stop
                            buySpread = ret[trades].buy - ret[trades].stop
                        if ret[-1].result > (ret[-1].buy - ret[-1].stop) * 2: # acumulando
                            trade = Trade()
                            trade.code = code
                            trade.qtd = 0
                            trade.begin = code_signal['date'][signalIdx]
                            trade.buy = quote['maxPrice'][signalIdx] # usando pior preco para compensar slipage
                            trade.volume = quote['volume'][signalIdx] # pegando o volume do dia para quando calcular trades
                            trade.sell = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                            trade.stop = ret[-1].stop
                            trade.stop2 = ret[-1].stop2
                            trade.end = code_signal['date'][signalIdx]
                            trade.result = trade.sell - trade.buy
                            ret.append(trade)
                            tradeCount = tradeCount + 1

                elif code_signal['signal'][signalIdx] == 'buy': # hora de comprar
                    trade = Trade()
                    trade.code = code
                    trade.qtd = 0
                    trade.begin = code_signal['date'][signalIdx]
                    trade.buy = quote['maxPrice'][signalIdx] # usando pior preco para compensar slipage
                    trade.volume = quote['volume'][signalIdx] # pegando o volume do dia para quando calcular trades
                    trade.sell = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                    trade.stop = code_signal['stop'][signalIdx]
                    trade.stop2 = code_signal['stop'][signalIdx]
                    trade.end = code_signal['date'][signalIdx]
                    trade.result = trade.sell - trade.buy
                    ret.append(trade)
                    tradeCount = tradeCount + 1

                signalIdx = signalIdx + 1

    return ret


def calc_all_trades():
    ret = []
    codes = load_known_codes()

    trend = Trend('week', ema, ema, 12, 6)
    signal = Signal('quote', ema, ema, 24, 12, stop_safeplace)

    for code in codes:
        print 'BackTesting: ' + code
        ret = ret + calc_trades(code, trend, signal)
    ret = sorted(ret, key = lambda trade: trade.begin)
    for i in range(len(ret)): # para cada posicao com data de entrada e saida distintas
        ret[i].trade = i

    return ret


def convertBacktesting(bt):
    ret = []
    for i in range(len(bt['begin'])):
        ret.append(BackTesting(bt['begin'][i], bt['end'][i], bt['buy'][i], bt['sell'][i], bt['result'][i], 
            bt['code'][i], bt['backtesting'][i], bt['stop'][i], bt['stop2'][i], bt['volume'][i]))
    ret = sorted(ret, key=lambda k: k.begin)
    return ret


def convertBacktesting2(bt):
    begin = []
    end = []
    buy = []
    sell = []
    result = []
    code = []
    backtesting = []
    stop = []
    stop2 = []
    equity = []
    invest = []
    liquid = []
    trade = []
    trades = []
    qtd = []
    volume = []
    for b in bt:
        begin.append(b.begin)
        end.append(b.end)
        buy.append(b.buy)
        sell.append(b.sell)
        result.append(b.result)
        code.append(b.code)
        backtesting.append(b.backtesting)
        stop.append(b.stop)
        stop2.append(b.stop2)
        equity.append(b.equity)
        invest.append(b.invest)
        liquid.append(b.liquid)
        trade.append(b.trade)
        trades.append(b.trades)
        qtd.append(b.qtd)
        volume.append(b.volume)
    ret = { 'begin': begin, 'end': end, 'buy': buy, 'sell': sell, 'result': result, 
    'code': code, 'backtesting': backtesting, 'stop': stop, 'stop2': stop2, 'equity': equity, 
    'trade': trade, 'trades': trades
    ,'qtd': qtd
    ,'invest': invest
    ,'liquid': liquid
    ,'volume': volume
    }
    return ret


def calctaxes(invest):
    ret = 17 + (invest * 0.00325) # corretagem + emolumentos bovespa
    return ret




def calc_total_trades(equity, risk, b1, bs):
    b1.trades = 1
    b1.equity = equity
    b1.liquid = equity
    b1.invest = 0
    for b2 in bs:
        if b1.equity <= 0.0: # acabou o dinheiro: estamos falidos
            break
        elif b2.trade == b1.trade: # somos nos mesmos
            #maxLoss = b1.equity * risk # maximo de perda sobre o patrimonio total (atual)
            maxLoss = equity * risk # maximo de perda sobre o patrimonio total (atual)
            calcTrade(maxLoss, b1)
            b1.liquid = b1.equity - b1.invest
            b1Invest = b1.buy * b1.qtd
            if b1Invest <= b1.liquid:
                b1.invest = b1.invest + b1Invest
                b1.liquid = b1.liquid - b1Invest
            else:
                b1.qtd = 0
            break
        elif b2.end <= b1.begin: # trades que terminaram antes
            b2Invest = b2.qtd * b2.buy
            b2Result = b2.qtd * b2.result - calctaxes(b2.qtd * b2.buy) - calctaxes(b2.qtd * b2.sell)
            if b2Result > 0.0:
                b2Result = b2Result * 0.80 # tirando imposto de renda por antecipacao
            b1.equity = b1.equity + b2Result
        elif b2.begin <= b1.begin and b2.end >= b1.begin: # trades que comecaram antes ou ao mesmo tempo
            b2Invest = b2.qtd * b2.buy
            b1.invest = b1.invest + b2Invest
            b1.trades = b1.trades + 1
        elif b2.end <= b1.end and b2.end >= b1.begin: # trades que comecaram depois
            pass
        elif b2.begin >= b1.begin and b2.end <= b1.end: # trades que comecaram depois e terminaram antes
            pass


def calc_money(trades, equity, risk, deposit, wage):

    def calc_trade(maxLoss, trade):
        lossPerUnit = trade.buy - trade.stop
        if lossPerUnit > 0:
            trade.qtd = round(maxLoss / lossPerUnit, -2)
            cash = trade.qtd * trade.buy
            if cash > (trade.volume / 2):
                trade.qtd = round(trade.volume / trade.buy / 2.0, -2) # no maximo compramos metade do book do dia?
        else:
            trade.qtd = 0

    maxDrawDown = 0.10
    ret = []
    begin = trades[0].begin
    end = datetime.date.today()
    diff = end - begin
    totalequity = equity # patrimonio total
    totalcash = totalequity # quanto esta em dinheiro
    totalinvest = 0.0 # quanto esta investido

    for i in range(0, diff.days + 1):
        money = Money()
        money.day = begin + datetime.timedelta(i)
        ret.append(money)

    print 'Money:'
    topequity = totalequity
    lowequity = totalequity
    drawdown = 0.0
    investdrawdown = 0.10
    investgrow = 0.50
    for day in ret:

        # define risco atual
        topequity = max(topequity, totalequity)
        drawdown = (topequity - totalequity) / topequity
        percMaxDrawDown = drawdown / maxDrawDown
        percRisk = 1 - percMaxDrawDown
        day.risk = risk * percRisk

        # seleciona os trades que entram e saem no dia
        day.begin = filter(lambda beg: day.day == beg.begin, trades)
        day.end = filter(lambda beg: day.day == beg.end, trades)
        day.wage = 0.0
        day.deposit = 0.0

        # para cada entrada, calcula cash envolvido e o resultado total
        day.buy = 0.0
        for trade in day.begin:
            maxLoss = totalcash * day.risk
            calc_trade(maxLoss, trade)
            cash = trade.qtd * trade.buy
            if cash <= totalcash:
                day.buy = day.buy + cash
                totalcash = totalcash - cash
                totalinvest = totalinvest + cash

        # para cada saida, calcula cash resultante
        day.sell = 0.0
        for trade in day.end:
            cash = trade.qtd * trade.sell
            buy = trade.qtd * trade.buy
            day.sell = day.sell + cash
            totalcash = totalcash + cash
            totalinvest = totalinvest - buy

        totalequity = totalinvest + totalcash

        # se for dia de pagamento, retiramos nosso "salario"...
        if day.day.month == 1 and day.day.day == 1:
            day.wage = wage(totalequity, day.risk)
            totalcash -= day.wage
            totalequity -= day.wage
            # ... e separa o valor de reinvestimento (ou investimento)
            if day.wage:
                day.deposit = day.wage * 0.10
            else:
                day.deposit = deposit
            totalcash += day.deposit
            totalequity += day.deposit

        day.equity = totalequity
        day.invest = totalinvest
        day.cash = totalcash

        if day.day.day == 1:
            print day.day
    print ''

    return ret


def backtesting_analysis():
    codes = load_known_codes()
    ret = { }

    for code in codes:
        print 'Analysis: ' + code
        quote = load_quote_data(code)
        signal = signal(code)
        trend = trend(code)

        trendDate = []
        trendValue = []

        for i in range(len(trend['date']) - 1):
            now = signal['date'].index(trend['date'][i])
            after = signal['date'].index(trend['date'][i+1])
            trendDate = trendDate + [trend['date'][i]] * (after - now)

            now = signal['date'].index(trend['date'][i])
            after = signal['date'].index(trend['date'][i+1])
            trendValue = trendValue + [trend['trend'][i]] * (after - now)

        trendDate = trendDate + [trend['date'][-1]] * ( len(signal['date']) - len(trendDate) )
        trendValue = trendValue + [trend['trend'][-1]] * ( len(signal['date']) - len(trendValue) )

        moreItems = dict(quote.items() + signal.items())
        moreItems['code'] = [code] * len(moreItems['date'])
        moreItems['trendDate'] = trendDate
        moreItems['trend'] = trendValue

        if len(ret.keys()) == 0:
            ret = moreItems
        else:
            for i in ret.keys():
                ret[i] = ret[i] + moreItems[i]

    return ret


def backtesting(imp = False, money = False, analysis = False, equity = 100000, risk = 0.005, deposit = 50000, minwageequity = 1000000):

    def calcwage(minequity):
        def retwage(equity, risk):
            if equity >= minequity:
                return equity * risk * 12
            return 0.0
        return retwage

    if imp:
        import_quote_from_jgrafix(r'C:\Tools\JGrafix\dados')

    trades = calc_all_trades()

    if money:
        money = calc_money(trades, equity, risk, deposit, calcwage(minwageequity))
        export_money_to_csv(money, 'Money.csv')

    export_trades_to_csv(trades, 'Trades.csv')

    #if analysis:
    #    analysis = backtesting_analysis()
    #    export_to_csv(analysis, 'Analysis.csv')


def impquote():
    import_quote_from_jgrafix(r'C:\Tools\JGrafix\dados') # importando dados (que ja devem estar atualizados)

def trades():
    trades = calc_all_trades()
    lastest = trades[-1].end

    # seleciona os trades que entram e saem no dia
    lasttrades = filter(lambda beg: beg.end == lastest, trades)
    for trade in lasttrades:
        print trade.code + '    @' + str(trade.buy) + '    stop ' + str(trade.stop2)

def day():
    impquote()
    trades()

