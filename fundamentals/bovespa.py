import datetime
import re
import StringIO
import urllib2
import xml.etree
import zipfile
import os
import pickle

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


def get_fin_accounts():
    return [item[:-1] for item in list(open(r'data\filterAccounts', 'r'))]


# selecao fundamentalista em revisao
def select_company_old(code):
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


def select_company(code):
    quote = load_quote_data(code)
    ret = { }
    volume = 0.0
    volumeTot = 0.0
    days = 0
    for i in range(len(quote['date'])):
        qv = quote['volume'][i]
        qd = quote['date'][i]
        if days == 0:
            days = days + 1
            volumeTot = qv
            volume = volumeTot
        else:
            if days < 60:
                days = days + 1
            else:
                volumeTot = volumeTot - volume
        volumeTot = volumeTot + qv
        volume = volumeTot / days
        ret[qd] = volume > 100000
    return ret


def load_inner_fin_zip(zip):
    for name in zip.namelist():
        if name[-3:] == 'itr' or name[-3:] == 'dfp':
            return zipfile.ZipFile(StringIO.StringIO(zip.read(name)))
