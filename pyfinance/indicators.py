import numpy as np

def ts_diff(data,lag=1):
    ddata = np.zeros_like(data)
    ddata[lag:] = data[lag:] - data[:-lag]
    return ddata

def ts_summ(data,lag=1):
    # not implemented yet
    pass

def simple_ma(data,lag=10):
    ma = np.zeros_like(data)
    for i in xrange(lag-1,len(data)):
        ma[i] = data[i-lag+1:i+1].mean()
    return ma

def triangular_ma(data,lag=10):
    w = int(np.ceil((lag+1)/2.0))
    return simple_ma(simple_ma(data,w),w)

def linear_weighted_ma(data,lag=10):
    ma = np.zeros_like(data)
    norm_factor = sum(range(1,lag+1))
    init = True
    for i in range(lag-1,len(data)):
        if(init):
            numrator = np.dot(np.array(range(1,lag+1)),data[:lag])
            init = False
        else:
            numrator = numrator + lag * data[i] - total
        total = data[i-lag+1:i+1].sum()
        ma[i] = numrator
    ma[lag-1:] /= norm_factor
    return ma

def exp_weighted_ma(data,alpha=0.1,lag=0):
    if(lag > 0):
        alpha = 2.0 / (lag+1)
    ma = np.zeros_like(data)
    for i in range(1,len(data)):
        ma[i] = ma[i-1] + alpha * (data[i] - ma[i-1])
    return ma

def modified_ma(data,lag=10):
    a = 1.0 / lag
    return exp_weighted_ma(data,alpha=a)

def ma_conv_div(data,short=12,long=26,f=exp_weighted_ma):
    short_ma = f(data,lag=short)
    long_ma    = f(data,lag=long) 
    macd = short_ma - long_ma
    return macd

def macd_signal(data,f=exp_weighted_ma):
    macd = ma_conv_div(data,f=f)
    signal = macd - f(macd,lag=9)
    return signal

def momentum(data,lag):
    m = np.zeros_like(data)
    m = ts_diff(data,lag)/lag
    return m

def roc(data,lag=1):
    """rate of change, naive implemetation"""
    roc = ts_diff(data, lag)
    roc[lag:] /=  data[:-lag]
    return roc * 100.0

def roc2(data,lag=1):
    """rate of change, logarithm implementation"""
    roc = ts_diff(data, lag)
    r = ts_diff(np.log(data), lag)
    r[:lag] = 0.0
    return r * 100.0

def stdev(data,lag=7,norm=False):
    s = np.zeros_like(data)
    for i in xrange(0,len(data)):
        if i < lag :
            temp = data[:i+1]
        else :
            temp = data[i-lag:i+1]
        
        if norm :
            s[i] = temp.std()/temp.mean()
        else:
            s[i] = temp.std()
    return s

def volatirity(data,lag=7):
    vol = stdev(returns(data),lag)*np.sqrt(250.0)
    return vol


def estrangement(data,f=simple_ma,lag=10):
    ma = f(data,lag=lag)
    return (data - ma) / ma * 100

def hmax(data,lag=30):
    m = np.zeros_like(data)
    for i in xrange(1,len(data)):
        if i < lag :
            temp = data[:i+1]
        else :
            temp = data[i-lag:i+1]
        m[i] = temp.max()
    return m

def hmin(data,lag=30):
    m = np.zeros_like(data)
    for i in xrange(1,len(data)):
        if i < lag :
            temp = data[:i+1]
        else :
            temp = data[i-lag:i+1]
        m[i] = temp.min()
    return m

def williams_R(data,lag=30):
    r = np.zeros_like(data)
    dmax = hmax(data,lag)
    dmin = hmin(data,lag)
    r = (data - dmax) / (dmin - dmax) * 100
    return r
    


#import os,sys,csv
#import numpy as np
#import matplotlib.pyplot as plt
##import svm
#
##fin = sys.argv[1]
#fin = "data.log"
#fp = file(fin,"rb")
#quotes = pickle.load(fp)
#
#data = (np.array(quotes.close_v()))
#f = exp_weighted_ma
#ma1 = f(data,lag=12)
#ma2 = f(data,lag=26)
#macd = ma_conv_div(data,f=f)
#macd_sig = macd_signal(data,f=f)
#est = estrangement(data,f,12)
#roc = rate_of_change(data)
#std = stdev(data)
#vol = volatirity(data)
#r = williams_R(data)
#
#dts = ts_diff(data,7)
#updown = []
#for i in range(30-6,len(dts)):
#    if dts[i] > 0 :
#        label = 1
#    else:
#        label = 0
#    updown.append(label)
#
#
#indata = np.array([ma1,ma2,macd,macd_sig,est,std,vol,r]).T
#indata2 = indata[30:len(indata)-1]
#
##prob = svm.svm_problem(updown,indata2)
##param = svm.svm_parameter()
##m = svm.svm_model(prob,param)
##p = m.predict(indata[-1])
##print p
#            
#
##############################################
#    #x = range(1,len(data)+1)
#    #ax1 = plt.subplot(211)
#    #ax1.plot(data,label="data")
#    #ax1.plot(ma1,label="ma_short")
#    #ax1.plot(ma2,label="ma_long")
#    #ax1.legend()
#    #ax2 = plt.subplot(212,sharex=ax1)
#    #ax2.plot(estrangement(data,f,12),label="R")
#    #ax2.legend()
#    #plt.show()
###############################################
#
#if __name__ == "__main__" :
#    main()
