import numpy as np
import pandas as pd
import scipy.stats as st
from  math import gamma


alpha=1.07
a=0.77
b=4.2
r=0.22
xi=14
txi=24
Ti=40
pi=.5
lambda_i=2

def draw_lambda(alpha,txi,xi,Ti,pi,r,lambda_i):
    num_1=pi/(txi+alpha)**(xi+r)
    den_1=pi/(txi+alpha)**(xi+r) + (1-pi)/(Ti+alpha)**(xi+r)
    gamma_1=st.gamma.pdf(xi+r,1/(txi+alpha))
    
    
    num_2=(1-pi)/(Ti+alpha)**(xi+r)
    den_2=pi/(txi+alpha)**(xi+r) +(1-pi)/(Ti+alpha)**(xi+r)
    gamma_2=st.gamma.pdf(xi+r,1/(Ti+alpha))
    
    
    
    pi_lambda=(num_1/den_1)*gamma_1*lambda_i + (num_2)/(den_2)*gamma_2*lambda_i

    return pi_lambda

print draw_lambda(alpha,txi,xi,Ti,pi,r,lambda_i)


def draw_roe(a,b,xi,lambda_i,Ti,txi,pi):
    num_1=a
    den_1=a+(b+xi-1)*np.exp(-lambda_i*(Ti-txi))
    beta_1=(gamma(a+1)*gamma(b+xi-1))/gamma(a+b+xi)
    
    num_2=(b+xi-1)*np.exp(-lambda_i*(Ti-txi))
    den_2=a+(b+xi-1)*np.exp(-lambda_i*(Ti-txi))
    beta_2=(gamma(a)*gamma(b+xi))/gamma(a+b+xi)

    pi_beta=(num_1/den_1)*beta_1*pi+(num_2/den_2)*beta_2*pi
    return pi_beta

print draw_roe(a,b,xi,lambda_i,Ti,txi,pi)
