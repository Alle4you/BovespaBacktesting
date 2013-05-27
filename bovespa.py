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


def load_data(code, dataName):
    from os.path import isfile, join
    code = code.lower()
    codePath = join(join(DATA_PATH, code), dataName)
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
    sma = get_sma(price[0:days], days)
    ret = []
    for i in range(days):
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
