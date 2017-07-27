import pandas as pd
from pandas_datareader import data as pdr
from bs4 import BeautifulSoup
import requests
from yahoo_finance import Share
from datetime import timedelta,datetime
from src.stock import stock
import fix_yahoo_finance




class stocktw(object):
    
    CaptialCashYear = 3

    def __init__(self, stockNum):
        self.stockNum = str(stockNum)
        self.share = Share(self.stockNum + ".tw")
        self.share.refresh()
        
    def getPrice(self):
        price = 0 
        try:
            price = self.share.get_price()
        except:
            print("get price error:" + self.stockNum)
            
        return price
    
    def isIncrease(self, x, key = lambda x: x): return all([key(x[i]) <= key(x[i + 1]) for i in self.CaptialCashYear])
    
    def isCaptialCashDecrease(self):
        url = str('http://easyfun.concords.com.tw/z/zc/zcb/zcb_%s.djhtm' % self.stockNum) 
        soup = BeautifulSoup(requests.get(url).text,'lxml')
        table =pd.read_html(str(soup.find_all('table')[0]))
        df = table[2]
        df = df.ix[1:,:2]
        df.columns = df.iloc[0]
        df = df[1:]
        ds = df["比重"].str[:-1].apply(pd.to_numeric)
        decreasing = lambda L: all(x<=y for x, y in zip(L,L[1:]))
        check = decreasing(ds[:self.CaptialCashYear])
        return check    
            
        
class financeControler():
    
    PERYear=7
    CashFlow = None
    BalanceSheet = None
    IncomeStatement = None
    PERTable = None
    Finace = None
    yys = None
    
    def __init__(self, stockNum):
        self.stock = stocktw(stockNum)
        self.CashFlow = None
        self.BalanceSheet = None
        self.IncomeStatement = None
        self.PERTable = None
        self.Finace = None
        self.yys = None
        
    def getStockDataFrameFromEasyfun(self,url):
        soup = BeautifulSoup(requests.get(url).text,'lxml')
        table =pd.read_html(str(soup.find_all('table')[0]))
        df = table[2]
        df.columns = (df.iloc[1])
        df = df[2:]
        df.set_index(df.ix[:,0], inplace=True)
        df.drop(df.columns[0], axis=1, inplace=True)
        #df = df[:-1]
        #df.columns = (df.iloc[0].astype(int, errors='ignore')+1911).astype(str, errors='ignore')
        
        return df
    
    def _insertRow(self,newRow,index,df):
        df = pd.concat([df.ix[:index], newRow, df.ix[index+1:]])
        
    def _getCashFlows(self):
        if self.CashFlow is None:
            url = str('http://easyfun.concords.com.tw/z/zc/zc3/zc3_%s.djhtm' % self.stock.stockNum) 
            self.CashFlow = self.getStockDataFrameFromEasyfun(url)
        return self.CashFlow
    
    def _getyys(self):
        if self.yys is None:
            url = str('http://easyfun.concords.com.tw/z/zc/zch/zch_%s.djhtm' % self.stock.stockNum) 
            self.yys = self.getStockDataFrameFromEasyfun(url)
        return self.yys
    
    def getyys(self):
        df = self._getyys()
        df = df[3:]
        df.index.names = ['月營收']
        return df
    
    def getCashFlowsStatement(self):
       
        df = self._getCashFlows()[:-1]
        balanceDf = self._getBalance()
        df2 = pd.DataFrame([df.loc['期末現金及約當現金'].astype(float) / balanceDf.loc['資產總額'].astype(float)],index=['<C> 現金 最好 10% ~ 25%'] )
        df = pd.concat([df.ix[:df.index.get_loc('期末現金及約當現金') + 1], df2, df.ix[df.index.get_loc('期末現金及約當現金') +1:]])
        #自由現金流量
        df2 = pd.DataFrame([df.loc['來自營運之現金流量'].astype(float) + df.loc['投資活動之現金流量'].astype(float)],index=['<C> 自由現金流量(1)+(2) \n( 負數，看 5 年是否正'])
        df = pd.concat([df.ix[:df.index.get_loc('本期產生現金流量') + 1], df2, df.ix[df.index.get_loc('本期產生現金流量') +1:]])
        df.index.set_value(df.index, '來自營運之現金流量', '<C> (1)營業活動之淨現金流入（流出）\nOCF要 > 0，有逐年往上嗎 ?\n( OCF與淨利的趨勢應該要一樣 )')
        df.index.set_value(df.index, '投資活動之現金流量', '<C> (2)投資活動之淨現金流入（流出）\nICF建議要看公司是否有定期投資未來?\n( 負數，代表公司有定期投資未來唷 )')
        df.index.set_value(df.index, '理財活動之現金流量', '<C> 籌資活動之淨現金流入（流出）')
        df.index.set_value(df.index, '期初現金約當現金', '<C> 期初現金約當現金')
        df.index.set_value(df.index, '期末現金及約當現金', '<C> 期末現金及約當現金餘額 \n現金與約當現金佔總資產 \n最好介於 10 ~ 25%')
        df.index.set_value(df.index, '本期產生現金流量', '<C> 本期產生現金流量')
        return df
    
    def _getIcome(self):
        if self.IncomeStatement is None:
            url = str('http://easyfun.concords.com.tw/z/zc/zcq/zcq_%s.djhtm' %  self.stock.stockNum) 
            self.IncomeStatement = self.getStockDataFrameFromEasyfun(url)
        return self.IncomeStatement
     
    def getIcomeStatement(self):
        df = self._getIcome()
        return df
    
    def _getBalance(self):
        if self.BalanceSheet is None:
            url = str('http://easyfun.concords.com.tw/z/zc/zcp/zcpa/zcpa_%s.djhtm' % self.stock.stockNum) 
            self.BalanceSheet = self.getStockDataFrameFromEasyfun(url)
        return self.BalanceSheet
    
    def getBalanceSheet(self):      
        df = self._getBalance()
        incomeDf = self._getIcome() 
        #Count and add ratio
        df2 = pd.DataFrame([df.loc['現金及約當現金'].astype(float) / df.loc['資產總額'].astype(float)],index=['<C> 現金 最好 10% ~ 25%'] )
        df = pd.concat([df.ix[:df.index.get_loc('現金及約當現金') + 1], df2, df.ix[df.index.get_loc('現金及約當現金') +1:]])
        df2 = pd.DataFrame([df.loc['應收帳款及票據'].astype(float) / incomeDf.loc['營業收入淨額'].astype(float)],index=['<C> 應收/營收 ％ 突然變大嗎? ➞ 不好！'] )
        df = pd.concat([df.ix[:df.index.get_loc('現金及約當現金') + 1], df2, df.ix[df.index.get_loc('現金及約當現金') +1:]])
        df2 = pd.DataFrame([df.loc['存貨'].astype(float) / incomeDf.loc['營業收入淨額'].astype(float)],index=['<C> 存貨/營收 ％ 突然變大嗎? ➞ 深入了解細節！'] )
        df = pd.concat([df.ix[:df.index.get_loc('現金及約當現金') + 1], df2, df.ix[df.index.get_loc('現金及約當現金') +1:]])
        
        return df
 
    def _getFianceSheet(self):
        if self.Finace is None:
            url = str('http://easyfun.concords.com.tw/z/zc/zcr/zcr_%s.djhtm' % self.stock.stockNum) 
            self.Finace = self.getStockDataFrameFromEasyfun(url)
        return self.Finace 
    
    def getFianceSheet(self):
    
        df = self._getFianceSheet()
        # Remove[說明] 
        df = df[:-1]
        
        blanceDF = self._getBalance()
        cashDF = self._getCashFlows()
        
        # D 資本結構
        df.index.set_value(df.index, '資本結構', '<C> (D) 資本結構')     
        # 長期資金佔固定資產比率    公式：(長期負債 + 股東權益) / 固定資產   
        df2 = pd.DataFrame([(blanceDF.loc['長期負債'].astype(float) + blanceDF.loc['股東權益總額'].astype(float)) / blanceDF.loc['固定資產'].astype(float)],
                           index=['<C> 長期資金佔不動產、廠房及設備比率(%) 以長支長，愈長愈好'] )
        df = pd.concat([df.ix[:df.index.get_loc('負債對淨值比率') + 1], df2, df.ix[df.index.get_loc('負債對淨值比率') +1:]]) 
        df.index.set_value(df.index, '負債對淨值比率', '<C> 負債對淨值比率\n那根棒子')
        
        # E 償債能力   
        df.index.set_value(df.index, '償債能力', '<C> (E) 償債能力')
        df.index.set_value(df.index, '流動比率', '<C> 流動比率\n欠我的能還嗎 ? 愈多愈好 \n> 300% 比較好')
        df.index.set_value(df.index, '速動比率', '<C> 速動比率\n您欠我的能速速還嗎 ? \n> 150% 比較好')
        
        # B 經營能力
        # 平均收現日數 = 90天/應收帳款週轉率
        df2 = pd.DataFrame([ 90 / df.loc['應收帳款週轉率(次)'].astype(float) ],index=['平均收現日數'] )
        df = pd.concat([df.ix[:df.index.get_loc('應收帳款週轉率(次)') + 1], df2, df.ix[df.index.get_loc('應收帳款週轉率(次)') +1:]])
        # 平均銷貨日數 = 90天/存貨週轉率(次)
        df2 = pd.DataFrame([ 90 / df.loc['存貨週轉率(次)'].astype(float) ],index=['平均銷貨日數'] )
        df = pd.concat([df.ix[:df.index.get_loc('存貨週轉率(次)') + 1], df2, df.ix[df.index.get_loc('存貨週轉率(次)') +1:]])  
        # 做生意的完整週期 = 2 + 3
        df2 = pd.DataFrame([ df.loc['平均收現日數'].astype(float) + df.loc['平均銷貨日數'].astype(float)],
                           index=['<C> 做生意的完整週期 = 2 + 3'] )
        df = pd.concat([df.ix[:df.index.get_loc('平均銷貨日數') + 1], df2, df.ix[df.index.get_loc('平均銷貨日數') +1:]])  
        df.index.set_value(df.index, '平均收現日數', '<C> (3)平均收現日數 \n應收帳款收款正常嗎?\n< 15天，代表做生意收現金')
        df.index.set_value(df.index, '平均銷貨日數', '<C> (2)平均銷貨日數 \n公司的產品好不好賣?')
        df.index.set_value(df.index, '總資產週轉率(次)', '<C> (1) 總資產週轉率(次) \n總資產翻桌率，最好 > 1 \n If < 1 代表資本密集(燒錢) ➞ 馬上看現金唷')
        df.index.set_value(df.index, '經營能力', '<C> (B) 經營能力')
        
        # C 獲利能力
        df.index.set_value(df.index, '每股稅後淨利(元)', '<C> (2) 每股稅後淨利(元) \n看看發生大事時的EPS')
        df.index.set_value(df.index, '營業毛利率', '<C> 營業毛利率')
        df.index.set_value(df.index, '營業利益率', '<C> *營業利益率（最重要）')
        df.index.set_value(df.index, '稅後淨利率', '<C> (3) 稅後淨利率 \n最好 > 2%')
        df.index.set_value(df.index, '股東權益報酬率', '<C> (5) 股東權益報酬率 \n最好 > 20%')
        
        # A 現金流量
        # 現金流量比率 = 營業活動淨現金流量 / 流動負債 
        # 現金再投資比率 = （營業活動淨現金流量 - 現金股利） / （固定資產毛額 + 長期投資 + 其他資產 + 營運資金）
        df2 = pd.DataFrame(index=['現金流量'] )
        df3 = pd.DataFrame([ cashDF.loc['來自營運之現金流量'].astype(float)/blanceDF.loc['流動負債'].astype(float) ],index=['現金流量比率'] )
        df4 = pd.DataFrame([ (cashDF.loc['來自營運之現金流量'].astype(float)+ cashDF.loc['支付現金股利'].astype(float))/
                            (blanceDF.loc['固定資產'].astype(float) + blanceDF.loc['長期投資'].astype(float) + 
                             blanceDF.loc['其他資產'].astype(float) + blanceDF.loc['現金及約當現金'].astype(float))],
                           index=['現金再投資比率'] )
        df = df.append(df2).append(df3).append(df4)[df.columns.tolist()]
           
        return df
         
    def getYesrStockDataFrameFromEasyfun(self,url):
        soup = BeautifulSoup(requests.get(url).text,'lxml')
        table =pd.read_html(str(soup.find_all('table')[0]))
        df = table[2]
        df.set_index(0, inplace=True)
        df.columns = (df.iloc[0].astype(int, errors='ignore')+1911).astype(str, errors='ignore')
        return df
                       
    def getYearIcomeStatement(self,stockNum):
        url = str('http://easyfun.concords.com.tw/z/zc/zcq/zcqa/zcqa_%s.djhtm' %  stockNum) 
        df = self.getYesrStockDataFrameFromEasyfun(url)
        return df.T
    
    def getYearBalanceSheet(self,stockNum):
        url = str('http://easyfun.concords.com.tw/z/zc/zcp/zcpb/zcpb_%s.djhtm' % stockNum) 
        df = self.getYesrStockDataFrameFromEasyfun(url)
        return df.T
    
    def __getHistroicPrice(self,stockNum):
        url = str('http://just.honsec.com.tw/Z/ZC/ZCW/CZKC1.djbcd?a=%s&b=A&c=1440' % stockNum) 
        data = requests.get(url).text.split()
        df = pd.DataFrame({'adj':data[1].split(',')},index = data[0].split(','))
        df['year'] = df.index.astype(str).str[:4]
        df[['Adj_Close']]=df[['adj']].apply(pd.to_numeric)
        
        # Get max and min price of each year
        maxval = df.groupby(['year'])['Adj_Close'].max()
        maxval.name = 'max'
        minval = df.groupby(['year'])['Adj_Close'].min()
        minval.name = 'min'
        df2 = pd.concat([maxval,minval],axis=1).reset_index()
        df2.set_index(df2.columns[0], inplace=True)
        return df2
        
    def __getHistroicPriceOld(self,stockNum):
        
        # Get historical price 8 years 
        stockNum = str(stockNum) + '.tw'
        #price = Share(str(stockNum) + '.tw')
        #now = price.get_trade_datetime()
        #now = datetime.now()
        #end = datetime.strptime(now, "%Y-%m-%d")
        end = datetime.now()
        start = (end-timedelta(days=self.PERYear*365))
        data = pdr.get_data_yahoo(stockNum, start=start.strftime('%Y-01-01'), end=end.strftime('%Y-%m-%d'))

        #hh = (price.get_historical(start.strftime('%Y-01-01'), end.strftime('%Y-%m-%d')))
        #df = pd.DataFrame(hh)
        df = data
        df['year'] = df.index.astype(str).str[:4]
        #df['year']=df.Date.str[:4]
        df['Adj_Close'] = df['Adj Close']
        df[['Adj_Close']]=df[['Adj_Close']].apply(pd.to_numeric)
        
        # Get max and min price of each year
        maxval = df.groupby(['year'])['Adj_Close'].max()
        maxval.name = 'max'
        minval = df.groupby(['year'])['Adj_Close'].min()
        minval.name = 'min'
        df2 = pd.concat([maxval,minval],axis=1).reset_index()
        
        # Set year to index
        df2.set_index(df2.columns[0], inplace=True)
        return df2
    
    def getPERTable(self):
        priceDf = self.__getHistroicPrice(self.stock.stockNum)
        BalanceDf = self.getYearBalanceSheet(self.stock.stockNum)
        IncomeDF = self.getYearIcomeStatement(self.stock.stockNum)
        PER = pd.concat([priceDf, IncomeDF['每股盈餘 (元)']], axis=1)
        PER.loc[:,'普通股股數'] = BalanceDf['普通股股本']/10
        PER.loc[:,'最高本益比'] = PER['max'].astype(float, errors='ignore')/PER['每股盈餘 (元)']
        PER.loc[:,'最低本益比'] = PER['min'].astype(float, errors='ignore')/PER['每股盈餘 (元)']
        PER = PER[::-1]
        return PER
    
    def getReport(self):
        # Get cash flow
        cashFlow= self.getCashFlowsStatement()
        # Get Balance Sheet
        blance = self.getBalanceSheet()
        # Get Finance Sheet
        finance = self.getFianceSheet()
        # Get IcomeStatement
        incomeStatement = self.getIcomeStatement()
        
        #add title and margin
        df1 = pd.DataFrame(index=['',''],columns=cashFlow.columns )
        df2 = df1.append(pd.DataFrame([cashFlow.columns],index=['資產負債季表'],columns=cashFlow.columns ))
        df3 = df1.append(pd.DataFrame([cashFlow.columns],index=['財務比率季表'],columns=cashFlow.columns ))
        df4 = df1.append(pd.DataFrame([cashFlow.columns],index=['損益季表'],columns=cashFlow.columns ))
        data = cashFlow.append(df2).append(blance).append(df3).append(finance).append(df4).append(incomeStatement)[cashFlow.columns.tolist()]
        
        return data
           
class countControler(object):    
    def updatePrice(self, stockDataFrame :pd.DataFrame):
        priceList = {}
        for index,row in stockDataFrame.iterrows():
            stock = stocktw(index)
            priceList[index] = stock.getPrice()
            print(index,priceList[index])
        ds= pd.Series(priceList)
        for i, row in ds.iteritems():
            stockDataFrame.set_value(i,"股價",ds[i])
        return stockDataFrame
    
#     def updateCaptial(self, stockDataFrame :pd.DataFrame):
#         list = {}
#         
#         for index,row in stockDataFrame.iterrows():
#             stock = stocktw(index)
#             list[index] = stock.getPrice()
#             print(index,list[index])
#         for i, row in ds.iteritems():
#             stockDataFrame.set_value(i,"股價",ds[i])
#         return stockDataFrame
    
    
    def updateCaptialCashDecrease(self, stockDataFrame :pd.DataFrame):
        list = {}
        for index,row in stockDataFrame.iterrows():
            stock = stocktw(index)
            list[index] = stock.isCaptialCashDecrease()
            print(index,list[index])
        ds= pd.Series(list)
        ds.name = "無現金增資"
        result = pd.concat([ds,stockDataFrame], axis=1)
        return result

import unittest        
class TestStockMethods(unittest.TestCase):
    def setUp(self):
        self.cc = countControler()
        self.fc = financeControler(2330)
        self.st1 = stocktw(1215)
        self.st2 = stocktw(1536)
        
    def test_fc_getCashFlowsStatement(self):
        self.assert_(self.fc.getCashFlowsStatement())  
        
    def test_st_isCaptialCashIncrease(self):
        self.assertTrue(self.st1.isCaptialCashDecrease())
        self.assertFalse(self.st2.isCaptialCashDecrease())
    def test_fc_isCaptialCashIncrease(self):
        self.assert_(self.fc.getPERTable())  
    def test_fc_getHistroicPrice(self):
        self.assert_(self.fc.__getHistroicPrice(2330))      
    def test_fc_getBalanceSheet(self):
        self.assert_(self.fc.getBalanceSheet())      
