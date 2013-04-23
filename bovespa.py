import urllib2, zipfile, StringIO, re, datetime

def geturl(cvmcode, pubdata):
    return 'http://www.bmfbovespa.com.br/dxw/Download.asp?moeda=L&site=B&mercado=18&ccvm=' + str(cvmcode) + '&data=' + pubdata + '&tipo=2'

def getquoteurl(year):
    return 'http://www.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A' + str(year) + '.ZIP'

def getzip(url):
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
        zip = getzip(url)
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
        zip = getzip(url)
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

