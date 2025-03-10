import json

from futu import *
import talib as ta 
from talib import MA_Type

def lambda_handler(event, context):

    #code = f"HK.800000"
    code = f"HK.MHImain"
    #startDate = "2025-01-01"
    startDate = "2025-02-17"
    endDate = "2025-02-22"

    # Retreive data from Futu
    success, histData = getData(code, startDate, endDate)

    calcRSI(histData, 'close', 14)

    # Very little to no data
    func = "CDL2CROWS"
    func = "CDL3BLACKCROWS" 
    func = "CDL3LINESTRIKE"

    # OK
    func = "CDLHAMMER"
    func = "CDLENGULFING"
    func = "CDLINVERTEDHAMMER"
    func = "CDLENGULFING"

    success, rows = matchPattern(func, histData['open'], histData['high'], histData['low'], histData['close'])

    #success, rows = match3LineStrike(histData['open'], histData['high'], histData['low'], histData['close'])
    #success, rows = matchHammer(histData['open'], histData['high'], histData['low'], histData['close'])
    #success, rows = matchInvHammer(histData['open'], histData['high'], histData['low'], histData['close'])
    #success, rows = matchEngulfing(histData['open'], histData['high'], histData['low'], histData['close'])
    #success, rows = match3StarInSouth(histData['open'], histData['high'], histData['low'], histData['close'])

    # Very little to no data
    #success, rows = matchBreakaway(histData['open'], histData['high'], histData['low'], histData['close'])

    if success:
        for i in range(len(rows)):
            if (rows[i] !=0):
                print(f"{func}: {rows.index[i]} - {rows[i]} - {histData['time_key'][rows.index[i]]}")

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Futu Test! v2')
    }

    if(success):
        # Debug and print first 3 and last 3 rows
        printData(histData, [0, 1, 2, len(histData) - 3, len(histData) - 2, len(histData) - 1])
        #printData(histData)
        print(f"[INFO] Retrieved {len(histData)} rows of data")

# --------------------------------------------------------------------------------------
def calcRSI(data, close, period=14):

    print(f"[INFO] Calculating RSI{period}")
    try:
        data[f"RSI{period}"] = ta.RSI(data[close], period)
    except Exception as exp:
        print(f"[ERROR] Failed calculating RSI{period}")
        return False

    return True

# --------------------------------------------------------------------------------------
def getData(code, start, end, ktype='K_1M'):

    # Get FOD address and port and connect
    try:
        fod = os.environ['FODAddress']
        port = int(os.environ['FODPort'])
    except KeyError:
        print(f"[ERROR] Environment variables FODAddress or port do not exist")
        return False, None

    print(f"[INFO] Connecting FutuOpenD {fod}:{port}")
    try:
        SysConfig.set_init_rsa_file("conn.txt")
        quote_ctx = OpenQuoteContext(host=fod,port=port,is_encrypt=True)
    except:
        print(f"[ERROR] Failed to connect FutuOpenD {host}:{port}")
        return False, None
    
    print(f"[INFO] Connected. Retrieving data for {code} range:{start} to {end} data:{ktype}")
    try:
        ret, data, page_req_key = quote_ctx.request_history_kline(code, start=start, end=end, ktype='K_1M', max_count=None)
    except:
        print(f"[ERROR] Failed to retrieve data for {code} range:{start} to {end} data:{ktype}")
        quote_ctx.close() 
        return False, None
    if ret != RET_OK:
        return False, None

    quote_ctx.close() 

    return True, data

# --------------------------------------------------------------------------------------
def matchPattern(func, open, high, low, close):

    print(f"[INFO] Matching {func} Patterns")
    try:
        matching = getattr(ta, func)
        rows = matching(open, high, low, close)
    except Exception as exp:
        print(f"[ERROR] Failed Matching {func} Patterns")
        return False, []

    return True, rows

# --------------------------------------------------------------------------------------
def printData(data, printRows='ALL'):

    header = list(data)
    print(header)

    if printRows == 'ALL':
        for i in range(len(data)):
            values=""
            for head in header:
                values = f"{values}{data[head][i]}," 
            print(values) 
    else:
        for rowNum in printRows:
            values=""
            for head in header:
                values = f"{values}{data[head][rowNum]}," 
            print(values) 

if __name__ == "__main__":
    print(lambda_handler({}, {}))