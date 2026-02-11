import os
import requests
import datetime
import base64
import hashlib
import pandas as pd
import numpy as np
from typing import Any
import logging
import warnings
warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


# function for sk
def compute_sha224(row):
    concatenated_values = ''.join(str(val) for val in row)
    return hashlib.sha224(concatenated_values.encode()).hexdigest()


def transfrom_user(user_df):
    '''
    transform worker data from db for payload ready
    '''
    try:
        filtered_users = [
        'workerId','externalWorkerId','userId','dateOfBirth','fullName','firstName','lastName','middleName','preferredName','shirtSize',
        'email','phoneNumber','onboardingComplete','hireDate',
        'terminationDate','employmentType','payPeriodPreferenceOptions','onboardingStatus','position.current.employeeId',"legalWorkAddress.current.employeeId",'position.current.title',
        'position.current.payRate.amount','position.current.payRate.currency','position.current.payType',
        'position.current.eligibleForOvertime','position.current.companyId','legalWorkAddress.current.companyId',
        'position.current.startDate',"legalWorkAddress.current.startDate",'position.current.employmentStatus','position.current.wageType','homeAddress.current.line1',
        'homeAddress.current.line2','homeAddress.current.city','homeAddress.current.state','homeAddress.current.postalCode',
        'approvalGroup.id','approvalGroup.name','legalWorkAddress.current.name','lifecycleStatus','position.current.createdAt'
        ]
        user = user_df
        for col in filtered_users:
            if col not in user.columns.tolist():
                user[col] = ''
        # retrieve needed columns
        user = user[filtered_users]

        # rename columns
        columns = {
            "workerId": "worker_id",
            "externalWorkerId": "external_id",
            "userId": "emp_id",
            "position.current.employeeId": "everee_id",
            "legalWorkAddress.current.employeeId": "contractor_everee_id",
            "approvalGroup.id": "approval_grp_id",
            "dateOfBirth": "dob",
            "fullName": "full_name",
            "firstName": "first_name",
            "lastName": "last_name",
            "middleName": "middle_name",
            'preferredName': "preferred_name",
            "shirtSize": "shirt_size",
            "email": "email",
            "phoneNumber": "phone",
            "homeAddress.current.line1": "address_1",
            "homeAddress.current.line2": "address_2",
            "homeAddress.current.city": "city",
            "homeAddress.current.state": "state",
            "homeAddress.current.postalCode": "zip",
            "onboardingComplete": "onboarding_complete",
            "hireDate": "hire_dt",
            "terminationDate": "termination_dt",
            "employmentType": "emp_type",
            "payPeriodPreferenceOptions": "pay_freq",
            "onboardingStatus": 'onboarding_status',
            "position.current.title": "title",
            "position.current.companyId": "company_id",
            "legalWorkAddress.current.companyId": 'contractor_company_id',
            "position.current.payRate.amount": "pay_rate",
            "position.current.payRate.currency": "currency",
            "position.current.payType": "pay_type",
            "position.current.eligibleForOvertime": "ot_eligible",
            "position.current.startDate": "start_dt",
            "legalWorkAddress.current.startDate": "contractor_start_dt",
            "position.current.employmentStatus": "emp_status",
            "position.current.wageType": "wage_type",
            "approvalGroup.name": "approval_grp_name",
            "legalWorkAddress.current.name": "cur_wrk_loc",
            "lifecycleStatus": "status",
            "position.current.createdAt": "created_at"
                }

        # rename columns
        user = user.rename(columns=columns)

        # fill emp_id with contractor_emp_id if emp_id is null or empty
        user['everee_id'] = user['everee_id'].fillna(user['contractor_everee_id'])
        user['company_id'] = user['company_id'].fillna(user['contractor_company_id'])
        user['start_dt'] = user['start_dt'].fillna(user['contractor_start_dt'])
        user.drop(columns=['contractor_company_id','contractor_everee_id','contractor_start_dt'], inplace=True)

        # pay freq
        payPeriodPreferenceOptions = user['pay_freq'].values[0]
        payPeriodPreferenceOptions = [item['localizedTitle'] for item in payPeriodPreferenceOptions if item.get('selected')][0]
        user['pay_freq'] = payPeriodPreferenceOptions
        user = user.replace({np.nan: ''})
        user = user.apply(lambda x: x.strip() if isinstance(x, str) else x)

        # List all columns you want to clean
        numeric_cols = ['emp_id', 'everee_id', 'company_id', 'approval_grp_id']
        bool_cols = ['onboarding_complete', 'ot_eligible']

        for col in numeric_cols:
            if col in user.columns:
                user[col] = (pd.to_numeric(user[col], errors='coerce').round().astype('Int64'))

        user = user.where(pd.notnull(user), None)

        user.loc[user['pay_type'] == 'HOURLY', 'pay_freq'] = 'Weekly payroll'
        user.loc[user['pay_type'] == 'SALARY', 'pay_freq'] = 'Semi-Monthly payroll'

        # Optionally, set pay_type to NA if not HOURLY or SALARY
        user.loc[~user['pay_type'].isin(['HOURLY', 'SALARY']), 'pay_type'] = pd.NA

        # set status to separated if termination date is not empty
        if user['termination_dt'].values.any():
            user['status'] = 'SEPARATED'
        # Add a new column with the SHA-224 hash
        user['curr_flg'] = 'Y'
        user['everee_sk'] = user.astype(str).apply(compute_sha224, axis=1)

    except Exception as ex:
        logger.exception(ex)
    return user


def everee_api_request(worker_id, api_token, tenant_id):
    '''
    everee api request
    '''
    try:
        # everee api url
        url = f"https://api-prod.everee.com/api/v2/workers/{worker_id}"
        # encoded api token
        encoded_token = base64.b64encode(api_token.encode('utf8')).decode()
        # headers details for url
        headers = {
            "accept": "application/json",
            "x-everee-tenant-id": tenant_id,
            "Authorization": f"Basic {encoded_token}"
        }

        response = requests.get(url, headers=headers)
        res_json = response.json()
        res_pd = pd.json_normalize(res_json)
    except Exception as ex:
        logger.exception(ex)
    return res_pd
