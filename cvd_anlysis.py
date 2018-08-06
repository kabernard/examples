# !/usr/bin/env python3

"""
### OBJECTIVE ###
The objective of this code is to facilitate exploratory analysis via linear regression and visualizations using data
collected by CMS and CDC to provide a picture of the public health burden of cardiovascular diseases and associated risk
factors in the United States. Specifically, this code aims to provide guidance as to which markets would benefit the
most from CVD interventions. A simple methodology was employed that consisted of conducting an OLS regression for a
range of CVD risk factors and obtaining a dataframe of the slopes for each risk factor from 2004 to 2013.

Negative values for the slope represent an overall decrease in CVD risk factors over time while positive values
represent an increase in these factors and could suggest that these markets may provide value for patient outcomes and
cost savings.

The results of the analysis show that most states have decreased their CVD risk factors. However, Minnesota, Rhode
Island, and Pennsylvania have positive or near-zero slope values for many indicators. Thus, it may be beneficial to
investigate devoting resources which could reduce CDV in these markets.

### NOTES ###

CMS compiles claims data for Medicare and Medicaid patients across a variety of categories and years. This includes
Inpatient and Outpatient claims, Master Beneficiary Summary Files, and many other files. Indicators from this data
source have been computed by personnel in CDC's Division for Heart Disease and Stroke Prevention (DHDSP). This is one of
the datasets provided by the National Cardiovascular Disease Surveillance System. The system is designed to integrate
multiple indicators from many data sources to provide a comprehensive picture of the public health burden of CVDs and
associated risk factors in the United States. The data are organized by location (national and state) and indicator.
The data can be plotted as trends and stratified by sex and race/ethnicity.

### WEBSITES ###
https://www.healthdata.gov/dataset/center-medicare-medicaid-services-cms-medicare-claims-data

### INDICATOR CODES ###
'MD101': 'Prevalence of major cardiovascular disease hospitalizations among all hospitalizations, US Medicare FFS beneficiaries (65+)',
'MD501': 'Prevalence of heart failure hospitalizations among all hospitalizations, US Medicare FFS beneficiaries (65+)',
'MD201': 'Prevalence of all heart disease hospitalizations among all hospitalizations, US Medicare FFS beneficiaries (65+)',
'MD401': 'Prevalence of heart attack hospitalizations among all hospitalizations, US Medicare FFS beneficiaries (65+)',
'MD301': 'Prevalence of coronary heart disease hospitalizations among all hospitalizations, US Medicare FFS beneficiaries (65+)',
'MDP001': 'Rate of hospitalizations among older adults with heart failure as the principal diagnosis (among FFS Medicare beneficiaries (65+))',
'MD601': 'Prevalence of cerebrovascular disease hospitalizations among all hospitalizations, US Medicare FFS beneficiaries (65+)',
'MDP002': 'Rate of hospitalizations among adults aged 65 to 74 years with heart failure as the principal diagnosis (among FFS Medicare beneficiaries (65+))',
'MDP003': 'Rate of hospitalizations among adults aged 75 to 84 years with heart failure as the principal diagnosis (among FFS Medicare beneficiaries (65+))',
'MDP004': 'Rate of hospitalizations among adults aged 85 years and older with heart failure as the principal diagnosis (among FFS Medicare beneficiaries (65+))'
"""

__author__ = 'Kwame Bernard'
__copyright__ = ''
__license__ = ''
__credits__ = ''
__version__ = "0.1a1"
__maintainer__ = "Kwame Bernard"
__email__ = "kwame.bernard@mac.com"
__status__ = "Prototype"  # "Prototype", "Development", "Production"


# IMPORT STATEMENTS

import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import linear_model as lm


# UNUSED IMPORT STATEMENTS

# import sys
# import geopandas

### LOGGING STATEMENTS ###

# logging package levels:
# DEBUG: Detailed information, typically of interest only when diagnosing problems.
# INFO: Confirmation that things are working as expected.
# WARNING: An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
# ERROR: Due to a more serious problem, the software has not been able to perform some function.
# CRITICAL: A serious error, indicating that the program itself may be unable to continue running.

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')


# VARIABLES

file_dir = '/cvd_project/data/'
file_name = 'Center_for_Medicare___Medicaid_Services__CMS____Medicare_Claims_data.csv'
file_path = file_dir + file_name


# INGEST DATA

dat = pd.read_csv(file_path)
dat = dat.drop(columns=['PriorityArea1', 'PriorityArea2', 'PriorityArea3', 'PriorityArea4', 'Data_Value_Type',
                        'Data_Value_TypeID', 'Category', 'CategoryId', 'Data_Value_Footnote_Symbol',
                        'Data_Value_Footnote', 'Data_Value_TypeID', 'DataSource'])


# TRANSFORM DATA

count_total = len(dat['Data_Value'])
count_null = len(dat['Data_Value']) - dat['Data_Value'].count()
null_fraction = count_null / count_total

dat = dat.dropna(subset=['Data_Value'])
dat_percent = dat[dat['Data_Value_Unit'] == 'Percent (%)']
dat_per1000 = dat[dat['Data_Value_Unit'] == 'Rate per 1,000']

counts_dict = {'count_total': count_total,
               'count_null': count_null,
               'nan_percent': null_fraction}

# get overall breakout data by percent and incidence per 1000
dat_percent_ovr = dat_percent[dat_percent['BreakOutId'] == 'OVR01']
dat_per1000_ovr = dat_per1000[dat_per1000['BreakOutId'] == 'OVR01']

# create dicts for indicator and breakout codes
ind_dict = dict(zip(dat['IndicatorID'].unique(), dat['Indicator'].unique()))
bo_dict = dict(zip(dat['BreakOutId'].unique(), dat['Break_Out'].unique()))


# BOX AND WHISKER PLOTS

def bw_plot(data_percent, data_per1000, bo_title):
    plt.figure(1)

    plt.subplot(3, 1, 1)
    plt.title(bo_title + '\n' + 'Box And Whisker Plot of Data by Percent %')
    plt.boxplot(data_percent, 0, 'b', 0)
    plt.xlabel('Percent %')
    plt.yticks([])

    plt.subplot(3, 1, 3)
    plt.title(bo_title + '\n' + 'Box And Whisker Plot of Data by Rate per 1000')
    plt.boxplot(data_per1000, 0, 'b', 0)
    plt.xlabel('Rate per 1000')
    plt.yticks([])

    # plt.show()
    plt.savefig('bwplot_{0}.png'.format(bo_title))
    plt.close()

# create box and whisker plots for the Overall breakout to identify outliers in the data
# bw_plot(dat_percent['Data_Value'], dat_per1000['Data_Value'], bo_dict['OVR01']) #debug
bw_plot(dat_percent_ovr['Data_Value'], dat_per1000_ovr['Data_Value'], bo_dict['OVR01'])


# HISTOGRAMS

def hist_plot(data_percent, data_per1000, bo_title):
    plt.figure(1)

    plt.subplot(3, 1, 1)
    plt.title(bo_title + '\n' + 'Histogram of Data by Percent %')
    data_percent['Data_Value'].hist(bins=50)

    plt.subplot(3, 1, 3)
    plt.title(bo_title + '\n' + 'Histogram of Data by Rate per 1000')
    data_per1000['Data_Value'].hist(bins=50)

    # plt.show()
    plt.savefig('hist_{0}.png'.format(bo_title))
    plt.close()

# create histogram plot for the Overall breakout to identify distribution and outliers
hist_plot(dat_percent_ovr, dat_per1000_ovr, bo_dict['OVR01'])


# REGRESSION AND SLOPE COEFFICIENTS

coef_df = pd.DataFrame(columns=['LocationAbbr', 'coef'])
tmp_df = pd.DataFrame(columns=['LocationAbbr', 'coef'])

reg = lm.LinearRegression() # create linear regression object

for ind in dat['IndicatorID'].unique():
    tmp_df = pd.DataFrame(columns=['LocationAbbr', 'coef'])

    for state in dat['LocationAbbr'].unique():
        dat_ovr = dat[(dat['IndicatorID'] == ind)
                      & (dat['BreakOutId'] == 'OVR01')
                      & (dat['LocationAbbr'] == state)]

        x = np.array(dat_ovr['Year'])
        y = np.array(dat_ovr['Data_Value'])

        x = x.reshape(len(x), 1)
        y = y.reshape(len(x), 1)

        reg.fit(x, y) # perform ols regression
        y_pred = reg.predict(x) # create data to show regression line
        tmp_df = tmp_df.append({'LocationAbbr': str(state), 'coef': float(reg.coef_)}, ignore_index=True)

        if ind == 'MD101':
            # plot data and regression lines for MD101 indicators
            plt.title('Overall Regression: {0} - {1}'.format(ind, state))
            plt.scatter(x, y, color='black')
            plt.plot(x, y_pred, color='blue', linewidth=1)
            plt.xlabel('Year')
            plt.ylabel('Percentage (or Incidence per 1000)')
            # plt.show()
            plt.savefig('reg_{0}_{1}.png'.format(ind, state))
            plt.close()

    if len(coef_df) == 0: coef_df = tmp_df
    else: coef_df = pd.merge(coef_df, tmp_df, on='LocationAbbr')



# rename the columns of the dataframe
i = 1
for key in ind_dict.keys():
    coef_df.columns.values[i] = key
    i += 1

coef_df.to_csv('coef.csv')  # export data frame of ols regression slopes as csv file for further analysis


# ANALYSIS

# find top five states with the least improvement in CVD risk factors for each indicator condition
for key in ind_dict.keys():
    tmp = coef_df[['LocationAbbr', key]]
    print(ind_dict[key])
    print(tmp.sort_values(by=key, ascending=False).head(5))
    print()


# DEBUG FUNCTION

def debug():
    print('DEBUG FUNCTION')

    # EXAMPLE STATS
    print('Count: {0},\n'
          'Null Count: {1},\n'
          'Null Fraction: {2}'.format(count_total, count_null, null_fraction))

debug()
