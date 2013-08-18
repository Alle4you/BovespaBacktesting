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

def load_known_codes():
    """Retorna a lista de papeis conhecidos."""
    from os.path import isdir, join
    ret = [ f for f in os.listdir(DATA_PATH) if isdir(join(DATA_PATH, f)) ]
    return ret


class Trade:
    """Informacoes de um trade."""
    pass


def export_trades_to_csv(trades, filePath):

    def write_header(f):
        f.write("trade" + ';')
        f.write("code" + ';')
        f.write("begin" + ';')
        f.write("buy" + ';')
        f.write("volume" + ';')
        f.write("sell" + ';')
        f.write("stop" + ';')
        f.write("stop2" + ';')
        f.write("end" + ';')
        f.write("result")
        f.write('\n')

    f = open(filePath, 'w')
    write_header(f)
    for trade in trades:
        f.write(str(trade.trade) + ';')
        f.write(str(trade.code) + ';')
        f.write(str(trade.begin) + ';')
        f.write(str(trade.buy) + ';')
        f.write(str(trade.volume) + ';')
        f.write(str(trade.sell) + ';')
        f.write(str(trade.stop) + ';')
        f.write(str(trade.stop2) + ';')
        f.write(str(trade.end) + ';')
        f.write(str(trade.result))
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
    ema12 = ema(quote, 12)
    ema6 = ema(quote, 6)
    trend = []
    for i in range(12): # ignorando primeiros dias
        trend.append('')
    for i in range(12, len(ema12)):
            diff = ema6[i] - ema12[i]
            if abs(diff) / ((ema6[i] + ema12[i]) / 2.0) < 0.01: # diferenca menor que 1%
                trend.append('')
            else:
                trend.append('buy' if ema6[i] > ema12[i] else 'sell')
        #else:
        #    trend.append('')
    ret = { 'date': quote['date'], 'ema-12': ema12, 'ema-6': ema6, 'trend': trend }
    return ret


def signal(code):
    """Calculo de sinal de operacao."""
    quote = load_quote_data(code)
    ema24 = ema(quote, 24)
    ema12 = ema(quote, 12)
    stop = stop_safeplace(quote)
    signal = []
    for i in range(24): # ignorando primeiros dias
        signal.append('')
    for i in range(24, len(ema24)):
        diff = ema12[i] - ema24[i]
        diffOk = abs(diff) / ((ema12[i] + ema24[i]) / 2.0) > 0.01 # diferenca maior que 1%
        emaOk = True if ema12[i] > ema24[i] else False
        stopOk = True if stop[i] < quote['minPrice'][i] else False
        signal.append('buy' if diffOk and emaOk and stopOk else '')
    ret = { 'date': quote['date'], 'ema-24': ema24, 'ema-12': ema12, 'signal': signal, 'stop': stop, 'min': quote['minPrice'] }
    return ret


def calc_trades(code):
    ret = []
    quote = load_quote_data(code)
    code_trend = trend(code)
    code_signal = signal(code)
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
                            drawdown = ret[trades].buy - ret[trades].stop
                            #if result[trades] > drawdown * 2: # lucro atual eh maior que a perda no stop vezes dois
                            #    newStop = sell[trades] - drawdown * 2
                            #    stop2[trades] = max(newStop, stop2[trades])
                            stopSpread = quote['minPrice'][signalIdx] - ret[trades].stop
                            buySpread = ret[trades].buy - ret[trades].stop
                        #if stop[-1] >= buy[-1]: # risco do ultimo trade ja eh zero; podemos continuar comprando
                        #    begin.append(code_signal['date'][signalIdx])
                        #    buy.append(quote['maxPrice'][signalIdx]) # usando pior preco para compensar slipage
                        #    volume.append(quote['volume'][signalIdx]) # pegando o volume do dia para quando calcular trades
                        #    sell.append(quote['minPrice'][signalIdx]) # usando pior preco para compensar slipage
                        #    stop.append(code_signal['stop'][signalIdx])
                        #    end.append(code_signal['date'][signalIdx])
                        #    result.append(sell[-1] - buy[-1])
                        #    tradeCount = tradeCount + 1

                elif code_signal['signal'][signalIdx] == 'buy': # hora de comprar
                    trade = Trade()
                    ret.append(trade)
                    trade.code = code
                    trade.begin = code_signal['date'][signalIdx]
                    trade.buy = quote['maxPrice'][signalIdx] # usando pior preco para compensar slipage
                    trade.volume = quote['volume'][signalIdx] # pegando o volume do dia para quando calcular trades
                    trade.sell = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                    trade.stop = code_signal['stop'][signalIdx]
                    trade.stop2 = code_signal['stop'][signalIdx]
                    trade.end = code_signal['date'][signalIdx]
                    trade.result = ret[-1].sell - ret[-1].buy
                    tradeCount = tradeCount + 1

                signalIdx = signalIdx + 1

    return ret


def get_all_trades():
    ret = []
    codes = load_known_codes()

    for code in codes:
        print 'BackTesting: ' + code
        ret = ret + calc_trades(code)
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


def calcTrade(maxLoss, b):
    lossPerUnit = b.buy - b.stop
    if lossPerUnit > 0:
        b.qtd = int((maxLoss / lossPerUnit) / 100) * 100
        cash = b.qtd * b.buy
        if cash > (b.volume / 2):
            b.qtd = ( (( b.volume / b.buy ) / 2) / 100) * 100 # no maximo compramos metade do book do dia?
    else:
        b.qtd = 0


def calcTotalTrades(equity, risk, b1, bs):
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


def moneytest(bt, equity = 100000, risk = 0.01):
    backtesting = convertBacktesting(bt)

    for i in range(len(backtesting)): # para cada posicao com data de entrada e saida distintas
        backtesting[i].trade = i + 1

    print 'Money: ',
    for i in range(len(backtesting)): # depois reajusta baseado em trades simultaneos
        if i % 10 == 0:
            print '\b$',
        calcTotalTrades(equity, risk, backtesting[i], backtesting)
    print ''

    ret = convertBacktesting2(backtesting)
    return ret


def moneytest2(bt, equity = 100000, risk = 0.01):
    backtesting = convertBacktesting(bt)
    begin = backtesting[0].begin
    end = datetime.date.today()
    diff = end - begin
    days = []

    for i in range(0, diff.days + 1):
        days.append(begin + datetime.timedelta(i))
        
    #print 'Money: ',
    #for i in range(len(backtesting)): # depois reajusta baseado em trades simultaneos
    #    if i % 10 == 0:
    #        print '\b$',
    #    calcTotalTrades(equity, risk, backtesting[i], backtesting)
    #print ''

    #ret = convertBacktesting2(backtesting)

    ret = { 'days': days }
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


def backtesting(imp = False, money = False, analysis = False):
    if imp:
        import_quote_from_jgrafix(r'C:\Tools\JGrafix\dados')

    trades = get_all_trades()
    export_trades_to_csv(trades, 'Trades.csv')

    #if money:
    #    money = moneytest2(bt, 100000, 0.02)
    #    export_to_csv(money, 'Money.csv')

    #if analysis:
    #    analysis = backtesting_analysis()
    #    export_to_csv(analysis, 'Analysis.csv')

