'''
Created on 2012/03/02

@author: du
'''

import numpy as np
from openopt import QP


def train_fixed_return(cv, mu, target_return=1.0, iprint=False):
    n_components = len(mu)
    H = cv
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


def train_scan_return(cv, mu, n_separate=100,
                        bmax=None, bmin=None, iprint=False):
    if bmin == None:
        bmin = 0.0
    if (bmax == None) or (bmax > np.max(mu)):
        bmax = np.max(mu)

    target_returns = np.linspace(bmin, bmax, n_separate)

    solutions = []
    for target_return in target_returns:
        try:
            sol = train_fixed_return(cv, mu, target_return, iprint)
            solutions.append(sol)
        except:
            pass

    return solutions


def getRiskFreeAsset(mu, solutions, risks=None, returns=None):
    if (risks == None) or (returns == None):
        risks = []
        returns = []
        for sol in solutions:
            risks.append(np.sqrt(sol.ff[0]))
            returns.append(np.dot(mu, sol.xf))

    n_solutions = len(solutions)
    intercepts = []
    for i in xrange(n_solutions - 1):
        slope = (returns[i + 1] - returns[i]) / (risks[i + 1] / risks[i])
        intercept = slope * risks[i] - returns[i]
        intercepts.append(intercept)

    return np.argmin(np.abs(intercept)), intercepts
