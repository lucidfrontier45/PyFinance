'''
Created on 2012/03/02

@author: du
'''

import numpy as np
from openopt import QP
from . import indicators

def prepareDataFromTimeSeries(tick_data, lag=1):
    roc = np.array([indicators.roc(ts.data[:,3], lag) for ts in tick_data])
    mu = np.mean(roc, 1)
    cv = np.cov(roc)
    return roc, mu, cv
    

def train_fixed_return(cv, mu, target_return=1.0, alpha=0.01, iprint=False):
    n_components = len(mu)
    assert target_return <= mu.max()
    avr_cv = cv.diagonal().mean()
    H = cv - np.identity(n_components) * avr_cv * alpha
    f = np.zeros(n_components)
    lb = np.zeros(n_components)
    ub = np.ones(n_components)
    A = -mu
    b = -target_return
    Aeq = np.ones(n_components)
    beq = 1.0
    qp = QP(H=H, f=f, lb=lb, ub=ub, A=A, b=b, Aeq=Aeq, beq=beq)
    sol = qp.solve("cvxopt_qp", iprint=iprint)
    return sol


def train_scan_return(cv, mu, n_separate=100, bmax=None, bmin=None, 
                      alpha=0.01, iprint=False):
    if bmin == None:
        bmin = 0.0
    if (bmax == None) or (bmax > np.max(mu)):
        bmax = np.max(mu)

    target_returns = np.linspace(bmin, bmax, n_separate)

    solutions = []
    for target_return in target_returns:
        try:
            sol = train_fixed_return(cv, mu, target_return, alpha, iprint)
            solutions.append(sol)
        except:
            pass

    return solutions


def getBestSolution(mu, solutions, risks=None, returns=None):
    if (risks == None) or (returns == None):
        risks = []
        returns = []
        for sol in solutions:
            risks.append(np.sqrt(sol.ff[0]))
            returns.append(np.dot(mu, sol.xf))

    n_solutions = len(solutions)
    intercepts = []
    for i in xrange(n_solutions - 1):
        slope = (returns[i + 1] - returns[i]) / (risks[i + 1] - risks[i])
        intercept = slope * risks[i] - returns[i]
        intercepts.append(intercept)

    return np.argmin(np.abs(intercepts)), intercepts


def accumulateROC(roc):
    return np.cumprod(roc/100.0 + 1.0)


def _filteIrrelaventTicks(sol, round_precision=2):
    rounded_weights = sol.xf.round(round_precision)
    mask = rounded_weights > 0.0
    return mask, rounded_weights[mask]

def getRelaventTicks(sol, tick_ids, round_precision=2):
    mask, relavent_weights = _filteIrrelaventTicks(sol, round_precision)
    tick_ids = np.asanyarray(tick_ids)
    relavent_ids = tick_ids[mask]
    return relavent_ids, relavent_weights

def normalizeWeight(weights):
    w = weights.round(2)
    return w / w.sum()

def simulateReturn(sol, roc, round_precision=2, tick_ids=None, dates=None):
    mask, relavent_weights = _filteIrrelaventTicks(sol, round_precision)
    tick_ids = np.asanyarray(tick_ids)
    relavent_roc = roc[mask]
    
    if not tick_ids == None:
        relavent_ids = tick_ids[mask]
        try:
            from . import nikkei225
            relavent_names = nikkei225.getNameFromID(relavent_ids)
            for (name, weights) in zip(relavent_names, relavent_weights):
                print "%s %2d%%" %(name, weights * 100)
        except:
            pass
    
    simulated_return = accumulateROC(np.dot(relavent_weights, relavent_roc))

    if not dates == None:
        try:
            import matplotlib.pyplot as plt
            plt.plot(dates, simulated_return, "x-", label="portfolio")
            if not tick_ids == None:
                for tick_id, roc in zip(relavent_ids, relavent_roc):
                    sim_ret = accumulateROC(roc)
                    plt.plot(dates, sim_ret, "-", label=str(tick_id))
            plt.xlabel("date")
            plt.ylabel("simulated_return")
            plt.legend(loc="upper left")
            plt.show()
        except:
            pass

    return simulated_return


