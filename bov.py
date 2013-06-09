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
FIN_ITR_DATES = [ '03-31', '06-30', '09-30' ]


def isitr(dt):
    return True if dt.month in [3, 6, 9] else False

def geturl(cvmcode, pubdata):
    return 'http://www.bmfbovespa.com.br/dxw/Download.asp?moeda=L&site=B&mercado=18&ccvm=' + str(cvmcode) + '&data=' + pubdata + '&tipo=2'

def getquoteurl(year):
    return 'http://www.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A' + str(year) + '.ZIP'

def CarregaArquivoZip(url):
    webRes = urllib2.urlopen(url)
    doc = webRes.read()
    return zipfile.ZipFile(StringIO.StringIO(doc))

def opendoc(cvmcode, year):
    """Carrega informacoes financeiras do site da Bovespa
    
    O cvmcode pode ser obtido no site da Bovespa (Empresas Listadas).
    """
    pubdata = '31/12/' + str(year)
    url = geturl(cvmcode, pubdata)
    try:
        zip = CarregaArquivoZip(url)
        for name in ['DFPCDERE.001', 'WINDOWS/TEMP_CVM/DFPCDERE.001']:
            if name in zip.namelist():
                doc = zip.read(name)
                break
        else: raise KeyError
        reglines = [re.match(r'^(.{6})(.{4})(.{2})(.{2})(.{13})(.{40})(.{15})(.{15})(.{15})(.{1})', x) for x in doc.splitlines()]
        lines = [[x.strip() for x in reglines[i].groups()] for i in range(len(reglines))]
        return [lines[x][4:-3] for x in range(len(lines))]
    except zipfile.BadZipfile:
        print 'Zip invalido para ', pubdata
    except KeyError:
        print 'Documento nao encontrado em ', pubdata
    return None

def getquotedocname(year):
    if int(year) == 2001:
        return 'COTAHIST_A' + str(year)
    elif int(year) < 2001:
        return 'COTAHIST.A' + str(year)
    else:
        return 'COTAHIST_A' + str(year) + '.TXT'

def openquotedoc(year):
    """Carrega cotacao historica do site da Bovespa

    Essa operacao pode demorar um pouco, pois o arquivo geralmente tem alguns megas.
    """
    url = getquoteurl(year)
    try:
        zip = CarregaArquivoZip(url)
        docname = getquotedocname(year)
        if docname in zip.namelist():
            return zip.read(docname)
    except zipfile.BadZipfile:
        print 'Zip invalido para ', year
    except KeyError:
        print 'Documento nao encontrado em ', year
    print 'Erro ao abrir documento do ano ', year
    
def openquotedocbyfile(path):
    """Carrega cotacao historica de arquivo ZIP baixado do site da Bovespa.
    """
    try:
        zip = zipfile.ZipFile(path, 'r')
        docname = zip.namelist()[0]
        return zip.read(docname)
    except zipfile.BadZipfile:
        print 'Zip invalido'
    except KeyError:
        print 'Documento nao encontrado'
    
def printquoteheader():
    print 'Data Pregao;Abertura;Minima;Maxima;Fechamento'

def printquoteline(line):
    date = line[8:10] + '/' + line[6:8] + '/' + line[2:6]
    open = float(line[56:69]) / 100
    min = float(line[82:95]) / 100
    max = float(line[69:82]) / 100
    close = float(line[108:121]) / 100
    print date + ';' + str(open) + ';' + str(min) + ';' + str(max) + ';' + str(close)

def printquotelinesbydate(code, lines, beginDate, endDate):
    printquoteheader()
    for line in lines:
        if line[12:23].strip() == code:
            lineDate = datetime.date(int(line[2:6]), int(line[6:8]), int(line[8:10]))
            if lineDate >= beginDate and lineDate <= endDate:
                printquoteline(line)

def getquotecodes(lines):
    """ Obtem lista de codigos dos papeis que foram operados.

    Passe o resultado da chamada de loadquotelines.
    """
    codelist = []
    for line in lines:
        code = line[12:23].strip()
        if code not in codelist:
            codelist += code
    return codelist

def printquotelines(code, lines):
    printquoteheader()
    for line in lines:
        if line[12:23].strip() == code:
            printquoteline(line)

def printquote(code, year):
    doc = openquotedoc(year)
    lines = doc.split('\n')
    printquotelines(code, lines)

def loadquotelines(beginYear, endYear):
    totalLines = []
    for year in range(beginYear, endYear + 1):
        doc = openquotedoc(year)
        lines = doc.split('\n')
        totalLines = totalLines + lines
    return totalLines

def filter_findoc(path):
    import xml.etree.ElementTree as ET
    tree = ET.parse(path)
    root = tree.getroot()
    newTree = ET.ElementTree()
    newRoot = ET.Element('Data')
    finLista = root.findall('InfoFinaDFin')    
    for finInfo in root.iter('InfoFinaDFin'):
        finCode = finInfo.find('PlanoConta').find('VersaoPlanoConta').find('CodigoTipoInformacaoFinanceira')
        if finCode.text == '2': # informacoes consolidadas
            newEntry = ET.Element('InfoFinaDFin')
            newEntry.append(finInfo.find('PlanoConta').find('NumeroConta'))
            newEntry.append(finInfo.find('DescricaoConta1'))
            newEntry.append(finInfo.find('ValorConta1'))
            newEntry.append(finInfo.find('ValorConta2'))
            newEntry.append(finInfo.find('ValorConta3'))
            newRoot.append(newEntry)
    newTree._setroot(newRoot)
    newTree.write(path[:-4] + '-result.xml')


def catalog_findoc_zip(zip, fileNameTip = ''):
    import xml.etree.ElementTree as ET

    try:
        xmlName = None
        for name in [ 'FormularioDemonstracaoFinanceiraDFP.xml', 'FormularioDemonstracaoFinanceiraITR.xml' ]:
            if name in zip.namelist():
                xmlName = name
                break
        if xmlName != None:
            xmlFile = zip.read(xmlName)
            dfp = ET.fromstring(xmlFile)
            cia = dfp.find('CompanhiaAberta').find('NomeRazaoSocialCompanhiaAberta').text.strip()
            cia = re.sub('[/]', '-', cia)
            data = dfp.find('DataReferenciaDocumento').text[0:10]
            fileNameTip = cia + '-' + xmlName[-7:-4] + '-' + data + '.zip'
            #print 'Arquivo ' + fileName + ' salvo com sucesso!'
        else:
            print 'Arquivo ' + fileNameTip + ' nao possui formulario; ignorando...'
    except Exception as e:
        print 'Erro analisando arquivo: ' + str(e)
    return fileNameTip


def get_company_name(zip):
    import xml.etree.ElementTree as ET
    ret = None

    try:
        xmlName = None
        for name in [ 'FormularioDemonstracaoFinanceiraDFP.xml', 'FormularioDemonstracaoFinanceiraITR.xml' ]:
            if name in zip.namelist():
                xmlName = name
                break
        if xmlName != None:
            xmlFile = zip.read(xmlName)
            dfp = ET.fromstring(xmlFile)
            ret = dfp.find('CompanhiaAberta').find('NomeRazaoSocialCompanhiaAberta').text.strip()
    except Exception as e:
        print 'Erro analisando arquivo: ' + str(e)

    return ret


def get_fin_date(zip):
    import xml.etree.ElementTree as ET
    ret = None

    try:
        xmlName = None
        for name in [ 'FormularioDemonstracaoFinanceiraDFP.xml', 'FormularioDemonstracaoFinanceiraITR.xml' ]:
            if name in zip.namelist():
                xmlName = name
                break
        if xmlName != None:
            xmlFile = zip.read(xmlName)
            dfp = ET.fromstring(xmlFile)
            ret = dfp.find('DataReferenciaDocumento').text[0:10]
    except Exception as e:
        print 'Erro analisando arquivo: ' + str(e)

    return ret



def catalog_findoc_url(url, docNumber):
    import xml.etree.ElementTree as ET

    try:
        webRes = urllib2.urlopen(url)
        content = webRes.read()
        fileName = str(docNumber) + '.zip'

        try:
            zip = zipfile.ZipFile(StringIO.StringIO(content))
            fileName = catalog_findoc_zip(zip, fileName)
        except:
            print 'Arquivo #' + str(docNumber) + ' invalido; ignorando...'

        file = open(fileName, 'wb')
        file.write(content)
        file.close()

    except Exception as e:
        print 'Erro fatal com arquivo #' + str(docNumber) + ':' + str(e)


def download_findoc(docNumber):
    urlbase = r'http://www.rad.cvm.gov.br/enetconsulta/frmDownloadDocumento.aspx?CodigoInstituicao=2&NumeroSequencialDocumento='
    docUrl = urlbase + str(docNumber)
    catalog_findoc_url(docUrl, docNumber)

def rename_findoc(path):
    zip = zipfile.ZipFile(path, 'r')
    return catalog_findoc_zip(zip)

def read_quotedoc(path):
    print path
    ret = []
    lines = list(open(path, 'r'))
    for currLine in lines:
        m = re.search(r'(\d+/\d+/\d+) ([0-9.]+) ([0-9.]+) ([0-9.]+) ([0-9.]+) ([0-9]+) ([0-9.]+) ([0-9.]+)', currLine)
        if m:
            ret.append(m.group(1, 2, 3, 4, 5, 6, 7, 8))
    return ret

def import_from_jgrafix(jgrafixDocsPath):
    from os.path import isfile, join
    filterPath = join(DATA_PATH, 'filterCodes')
    quoteDocs = [ f for f in os.listdir(jgrafixDocsPath) if isfile(join(jgrafixDocsPath, f)) ]
    filterCodes = [item.lower()[:-1] for item in list(open(filterPath, 'r'))]
    filterDocs = [ f for f in quoteDocs if f in filterCodes ]
    for doc in filterDocs:
        quote = read_quotedoc(join(jgrafixDocsPath, doc))
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


def load_companyToCode():
    codes = [item[:-1] for item in list(open(r'data\companyToCode', 'r'))]
    ret = {}
    for cod in codes:
        company = cod[0:13].strip()
        code = cod[13:].strip()
        if company in ret:
            ret[company].append(code)
        else:
            ret[company] = [ code ]
    return ret


def load_data(code, dataName):
    from os.path import isfile, join
    ret = None
    code = code.lower()
    codePath = join(join(DATA_PATH, code), dataName)
    if os.path.exists(codePath):
        f = open(codePath, 'r')
        ret = pickle.load(f)
        f.close()
    return ret


def load_quote(code):
    return load_data(code, QUOTE_FILE_NAME)


def load_week_quote(code):
    return load_data(code, QUOTE_WEEK_FILE_NAME)


def get_quote_codes():
    from os.path import isdir, join
    ret = [ f for f in os.listdir(DATA_PATH) if isdir(join(DATA_PATH, f)) ]
    return ret


def get_fin_accounts():
    return [item[:-1] for item in list(open(r'data\filterAccounts', 'r'))]


def write_data(code, dataName, data):
    from os.path import isfile, join
    code = code.lower()
    codePath = join(join(DATA_PATH, code), dataName)
    f = open(codePath, 'w')
    pickle.dump(data, f)
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


def get_sma(quote, days = 10):
    price = quote['closePrice']
    ret = []
    for i in range(len(price)):
        l = price[ max(i - days + 1, 0) : i + 1 ]
        ret.append( sum(l) / len(l) )
    return ret


def get_ema(quote, days = 10):
    price = quote['closePrice']
    sma = get_sma(quote, days)
    ret = []
    for i in range(min(len(sma), days)):
        ret.append(sma[i])
    multiplier = ( 2.0 / (float(days) + 1.0) )
    for i in range(days, len(price)):
        ret.append( (price[i] - ret[i-1]) * multiplier + ret[i-1] )
    return ret


def get_macd(quote, shortDays = 12, longDays = 26, signalDays = 9):
    price = quote['closePrice']
    shortMacd = get_ema(price, shortDays)
    longMacd = get_ema(price, longDays)
    macd = []
    for i in range(len(price)):
        macd.append(shortMacd[i] - longMacd[i])
    signal = get_ema(macd, signalDays)
    hist = []
    for i in range(len(price)):
        hist.append(macd[i] - signal[i])
    ret = { 'macd': macd, 'signal': signal, 'hist': hist }
    return ret

def get_stop_safeplace(quote, multiplier = 4):
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


def get_stop_candelabro(quote, multiplier = 4):
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
        l = maxPrice[ max(i - atrSumDays + 1, 0) : i + 1 ]
        ret.append(max(l) - (atr[i] * multiplier))
    #testRet = { 'highLow': highLow, 'highClose': highClose, 'lowClose': lowClose, 'tr': tr, 'atr': atr, 'stop': ret }
    return ret


def get_stop_atr(quote, multiplier = 3):
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


def isSelected(select, dt):
    ret = False
    for k, v in sorted(select.iteritems()):
        if k < dt: ret = v
    return ret


def get_trend(code):
    select = select_company(code)
    quote = load_data(code, 'week')
    ema12 = get_ema(quote, 12)
    ema6 = get_ema(quote, 6)
    trend = []
    for i in range(12): # ignorando primeiros dias
        trend.append('')
    for i in range(12, len(ema12)):
        #if isSelected(select, quote['date'][i]):
            diff = ema6[i] - ema12[i]
            if abs(diff) / ((ema6[i] + ema12[i]) / 2.0) < 0.01: # diferenca menor que 1%
                trend.append('')
            else:
                trend.append('buy' if ema6[i] > ema12[i] else 'sell')
        #else:
        #    trend.append('')
    ret = { 'date': quote['date'], 'ema-12': ema12, 'ema-6': ema6, 'trend': trend }
    return ret


def get_signal(code):
    quote = load_quote(code)
    ema24 = get_ema(quote, 24)
    ema12 = get_ema(quote, 12)
    stop = get_stop_safeplace(quote)
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


# aproveita o trend e faz compras enquanto o sinal continuar
# stop nao se mexe
def get_backtesting_fixed_stop(code):
    quote = load_quote(code)
    trend = get_trend(code)
    signal = get_signal(code)

    begin = []
    buy = []
    stop = []
    sell = []
    end = []
    result = []
    isLong = False

    for week in range(24, len(trend['trend'])):

        if trend['trend'][week] == 'buy': # semana de compras
            signalIdx = signal['date'].index(trend['date'][week]) # inicio da semana em dias
            lastDay = trend['date'][week+1] if week+1 < len(trend['date']) else datetime.date.today() # ultimo dia do trend atual

            while signalIdx < len(signal['signal']) and signal['date'][signalIdx] < lastDay: # enquanto houver dias e estivermos no trend atual

                if isLong: # estamos comprados
                    if quote['minPrice'][signalIdx] <= stop[-1]: # estourou o stop
                        sell[-1] = stop[-1]
                        end[-1] = signal['date'][signalIdx]
                        result[-1] = sell[-1] - buy[-1]
                        isLong = False
                    elif signal['signal'][signalIdx] != 'buy': # hora de vender
                        sell[-1] = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                        end[-1] = signal['date'][signalIdx]
                        result[-1] = sell[-1] - buy[-1]
                        isLong = False
                    else: # apenas atualiza dados
                        sell[-1] = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                        end[-1] = signal['date'][signalIdx]
                        result[-1] = sell[-1] - buy[-1]
                        stopSpread = quote['minPrice'][signalIdx] - stop[-1]
                        buySpread = buy[-1] - stop[-1]

                elif signal['signal'][signalIdx] == 'buy': # hora de comprar
                    begin.append(signal['date'][signalIdx])
                    buy.append(quote['maxPrice'][signalIdx]) # usando pior preco para compensar slipage
                    sell.append(quote['minPrice'][signalIdx]) # usando pior preco para compensar slipage
                    stop.append(signal['stop'][signalIdx])
                    end.append(signal['date'][signalIdx])
                    result.append(sell[-1] - buy[-1])
                    isLong = True

                signalIdx = signalIdx + 1

    ret = { 'begin': begin ,'buy': buy ,'stop': stop ,'sell': sell ,'end': end ,'result': result }
    return ret


# aproveita o trend e faz compras enquanto o sinal continuar
# stop se mexe conforme o preco atual se move o dobro do stop anterior
def get_backtesting_moveable_stop(code):
    quote = load_quote(code)
    trend = get_trend(code)
    signal = get_signal(code)

    begin = []
    buy = []
    stop = []
    sell = []
    end = []
    result = []
    isLong = False

    for week in range(24, len(trend['trend'])):

        if trend['trend'][week] == 'buy': # semana de compras
            signalIdx = signal['date'].index(trend['date'][week]) # inicio da semana em dias
            lastDay = trend['date'][week+1] if week+1 < len(trend['date']) else datetime.date.today() # ultimo dia do trend atual

            while signalIdx < len(signal['signal']) and signal['date'][signalIdx] < lastDay: # enquanto houver dias e estivermos no trend atual

                if isLong: # estamos comprados
                    if quote['minPrice'][signalIdx] <= stop[-1]: # estourou o stop
                        sell[-1] = stop[-1]
                        end[-1] = signal['date'][signalIdx]
                        result[-1] = sell[-1] - buy[-1]
                        isLong = False
                    elif signal['signal'][signalIdx] != 'buy': # hora de vender
                        sell[-1] = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                        end[-1] = signal['date'][signalIdx]
                        result[-1] = sell[-1] - buy[-1]
                        isLong = False
                    else: # apenas atualiza dados
                        sell[-1] = quote['minPrice'][signalIdx] # usando pior preco para compensar slipage
                        end[-1] = signal['date'][signalIdx]
                        result[-1] = sell[-1] - buy[-1]
                        stopSpread = quote['minPrice'][signalIdx] - stop[-1]
                        buySpread = buy[-1] - stop[-1]
                        if stopSpread > buySpread * 2: # se der pra subir o stop
                            stop[-1] = signal['stop'][signalIdx]

                elif signal['signal'][signalIdx] == 'buy': # hora de comprar
                    begin.append(signal['date'][signalIdx])
                    buy.append(quote['maxPrice'][signalIdx]) # usando pior preco para compensar slipage
                    sell.append(quote['minPrice'][signalIdx]) # usando pior preco para compensar slipage
                    stop.append(signal['stop'][signalIdx])
                    end.append(signal['date'][signalIdx])
                    result.append(sell[-1] - buy[-1])
                    isLong = True

                signalIdx = signalIdx + 1

    ret = { 'begin': begin ,'buy': buy ,'stop': stop ,'sell': sell ,'end': end ,'result': result }
    return ret


def get_backtesting_all():
    codes = get_quote_codes()
    tests = [ get_backtesting_fixed_stop, get_backtesting_moveable_stop ]
    ret = { 'begin': [] ,'buy': [] ,'stop': [] ,'sell': [] ,'end': [] ,'result': [], 'code': [], 'backtesting': [] }

    for test in tests:
        print test.__name__
        for code in codes:
            print '>' + code
            backtesting = test(code)
            backtestingCount = len(backtesting['begin'])
            ret['code'] = ret['code'] + [ code ] * backtestingCount
            ret['backtesting'] = ret['backtesting'] + [ test.__name__ ] * backtestingCount
            for item in backtesting.keys():
                ret[item] = ret[item] + backtesting[item]

    return ret


def get_backtesting_signal_fixed_stop(code):
    quote = load_quote(code)
    trend = get_trend(code)
    signal = get_signal(code)
    date = []
    retsignal = []

    for week in range(24, len(trend['trend'])):

        if trend['trend'][week] == 'buy': # semana de compras
            signalIdx = signal['date'].index(trend['date'][week]) # inicio da semana em dias
            lastDay = trend['date'][week+1] if week+1 < len(trend['date']) else datetime.date.today() # ultimo dia do trend atual

            while signalIdx < len(signal['signal']) and signal['date'][signalIdx] < lastDay: # enquanto houver dias e estivermos no trend atual

                date.append(signal['date'][signalIdx])
                if signal['signal'][signalIdx] == 'buy': # hora de comprar
                    retsignal.append('buy')
                else:
                    retsignal.append('')

                signalIdx = signalIdx + 1

    ret = { 'date': date, 'signal': retsignal }
    return ret

def backtesting_select_current():
    codes = get_quote_codes()
    date = []
    retsignal = []
    retcode = []
    for code in codes:
        backtesting = get_backtesting_signal_fixed_stop(code)
        if len(backtesting['signal']) and backtesting['signal'][-1] == 'buy':
            date.append(backtesting['date'][-1])
            retsignal.append(backtesting['signal'][-1])
            retcode.append(code)

    ret = { 'date': date, 'code': retcode, 'signal': retsignal }
    return ret



def select_company(code):
    fin = load_data(code, 'fin')
    ret = { }
    if fin != None:
        for dt, dfp in fin.iteritems():
            profit = []
            if '3.11' in dfp and len(dfp['3.11']) == 4:
                profit.append(dfp['3.11'][0])
                profit.append(dfp['3.11'][2])
            elif '3.11' in dfp and len(dfp['3.11']) == 3:
                profit.append(dfp['3.11'][0])
                profit.append(dfp['3.11'][1])
            elif '3.11' in dfp and len(dfp['3.11']) == 2:
                profit.append(dfp['3.11'][0])
                profit.append(dfp['3.11'][1])
            ret[dt] = True if len(profit) == 2 and profit[0] > profit[1] else False
    return ret


def load_inner_fin_zip(zip):
    for name in zip.namelist():
        if name[-3:] == 'itr' or name[-3:] == 'dfp':
            return zipfile.ZipFile(StringIO.StringIO(zip.read(name)))


def import_from_fin_doc(path):
    import xml.etree.ElementTree as ET

    zip = zipfile.ZipFile(path, 'r')
    company = get_company_name(zip)
    finDate = get_fin_date(zip)

    if company and finDate:
        print path, company, finDate
        fdate = datetime.date(int(finDate[0:4]), int(finDate[5:7]), int(finDate[8:10]))

        companies = load_companyToCode()
        codes = set(get_quote_codes())
        finAccounts = get_fin_accounts();

        if company in companies:
            companyCodes = list(set(companies[company]) & codes)
            if len(companyCodes):
                zip = load_inner_fin_zip(zip)
                if zip and 'InfoFinaDFin.xml' in zip.namelist():
                    xmlFile = zip.read('InfoFinaDFin.xml')
                    dfp = ET.fromstring(xmlFile)
                    finLista = dfp.findall('InfoFinaDFin')    
                    finData = { }

                    for finInfo in dfp.iter('InfoFinaDFin'):
                        finCode = finInfo.find('PlanoConta').find('VersaoPlanoConta').find('CodigoTipoInformacaoFinanceira')
                        if finCode.text == '2': # informacoes consolidadas
                            accountCode = finInfo.find('PlanoConta').find('NumeroConta')
                            if accountCode.text in finAccounts:
                                accountValues = []
                                for i in range(1, 13):
                                    accountNumber = 'ValorConta' + str(i)
                                    accountValue = float(finInfo.find(accountNumber).text)
                                    if accountValue != 0.0:
                                        accountValues.append(accountValue)
                                finData[accountCode.text] = accountValues

                    for cod in companyCodes:
                        fin = load_data(cod, 'fin')
                        if fin == None: fin = { }
                        fin[fdate] = finData
                        write_data(cod, 'fin', fin)


def import_from_fin_doc_only_complement(path):
    import xml.etree.ElementTree as ET

    zip = zipfile.ZipFile(path, 'r')
    company = get_company_name(zip)
    finDate = get_fin_date(zip)

    if company and finDate:
        print path, company, finDate
        fdate = datetime.date(int(finDate[0:4]), int(finDate[5:7]), int(finDate[8:10]))

        companies = load_companyToCode()
        codes = set(get_quote_codes())
        finAccounts = get_fin_accounts();

        if company in companies:
            companyCodes = list(set(companies[company]) & codes)
            if len(companyCodes):
                zip = load_inner_fin_zip(zip)
                shareCount = 0.0
                profitPerShare = 0.0

                if zip and 'ComposicaoCapitalSocialDemonstracaoFinanceiraNegocios.xml' in zip.namelist():
                    xmlFile = zip.read('ComposicaoCapitalSocialDemonstracaoFinanceiraNegocios.xml')
                    root = ET.fromstring(xmlFile)
                    shareTag = root.find('ComposicaoCapitalSocialDemonstracaoFinanceira')
                    if shareTag != None:
                        shareCountTag = shareTag.find('QuantidadeTotalAcaoCapitalIntegralizado')
                        if shareCountTag != None:
                            shareCount = float(shareCountTag.text)

                if zip and 'PagamentoProventoDinheiroDemonstracaoFinanceiraNegocios.xml' in zip.namelist():
                    xmlFile = zip.read('PagamentoProventoDinheiroDemonstracaoFinanceiraNegocios.xml')
                    root = ET.fromstring(xmlFile)
                    profitTag = root.find('PagamentoProventoDinheiroDemonstracaoFinanceira')
                    if profitTag != None:
                        profitCountTag = profitTag.find('ValorProventoPorAcao')
                        if profitCountTag != None:
                            profitPerShare = float(profitCountTag.text)

                for cod in companyCodes:
                    fin = load_data(cod, 'fin')
                    if fin != None and fdate in fin:
                        finData = fin[fdate]
                        finData['shareCount'] = shareCount
                        finData['profitPerShare'] = profitPerShare
                        write_data(cod, 'fin', fin)
