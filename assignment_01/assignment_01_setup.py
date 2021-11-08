"""This script sets up a sample dataset for assignment 01
   using real OmniCom data complying with company data privacy policy.

This purpose of the assignment is to assess the candidate’s ability to:

1. Clean-up data not in standard format
2. Process data into standard summary statistics

see README and INSTRUCTIONS for details
"""
# ----------------------------------------------------------------------------------
# external package dependencies
# ---------------------------------------------------------------------------------
import sys
import os
import json
import random
import datetime as dt
import numpy as np
import pandas as pd
# ----------------------------------------------------------------------------------
# local dependencies
#----------------------------------------------------------------------------------
import omnicom as oc
# ----------------------------------------------------------------------------------
# static variables
#----------------------------------------------------------------------------------
QRY_FILENAME = 'assignment_01.sql'
SERVER = 'omnicom'
LOCAL_DIR = ""
# ----------------------------------------------------------------------------------
# dynamic variables
#----------------------------------------------------------------------------------
TABLES = {}
FIELDS_TO_REMOVE = {
    'process_audit': [
        'uuid_',
        'userId',
        'companyId',
        'userName',
        'type_',
        'doneBy',
        'action',
        'field1',
        'field2',
        'field3',
        'field4',
        'field5',
        'data2',
        'spPEProcessId',
        'statusTypeId',
        'spPEProcessStageId',
        'status',
        'nodeId',
        'userIdSupervisor',
        'userIdAgent',
        'spPEClosedStageId',
        'groupId'
    ],
    'unpacked': [
        'msource_1',
        'minisite_url_2',
        'firstName',
        'lastName',
        'full_number_intPhone',
        'intPhone',
        'parentVisitorId',
        'parentUrl',
        'agree_to_terms_of_use_8',
        'country_13',
        'upload_resume_14',
        'userId',
        'data1'
    ]
}
#---------------------------------------------------------------------------------------
# local env
#---------------------------------------------------------------------------------------
""" in case of diagnostics on local env add the following lines:

ROOT_DIR = os.getcwd()
LOCAL_DIR = ROOT_DIR + "\\assignment_01\\"
"""

LOCAL_DIR = ""
# ----------------------------------------------------------------------------------
# setup
#----------------------------------------------------------------------------------


def load():
    oc.load_config()
# ----------------------------------------------------------------------------------
# main
#----------------------------------------------------------------------------------


def setup():
    load()
    #00 connect to database
    oc.db_connect(SERVER)

    #01 load the source table and name it 'process_audit'
    s_table_name = 'process_audit'
    TABLES[s_table_name] = oc.query_from_sql_file(LOCAL_DIR + 'assignment_01.sql', SERVER)

    #02.01 export manually inspect data
    TABLES[s_table_name].to_csv(LOCAL_DIR + s_table_name + '.csv', index=False)

    #02.02 remove redundant and confidential fields
    keep_fields = [f for f in TABLES[s_table_name].columns if
                   f not in FIELDS_TO_REMOVE[s_table_name]]
    TABLES[s_table_name] = TABLES[s_table_name][keep_fields]

    #03 unpack data1 export to csv and manually inspect fields to remove
    unpacked = form_submissions_unpack(TABLES[s_table_name]['data1'].values)
    TABLES['unpacked'] = pd.merge(TABLES[s_table_name], unpacked, how='left', on='storageId')
    TABLES['unpacked'].to_csv(LOCAL_DIR + 'unpacked.csv', index=False)

    keep_fields = [f for f in TABLES['unpacked'].columns if
                   f not in FIELDS_TO_REMOVE['unpacked']]
    TABLES['unpacked'] = TABLES['unpacked'][keep_fields]

    #04 anonymize student id using email address field, then remove email address
    TABLES['unpacked']['VisitorId'] = TABLES['unpacked']['emailAddress'].astype('category').cat.codes
    del TABLES['unpacked']['emailAddress']

    #05 simulate campaign spending
    TABLES['campaigns'] = campaign_spending(TABLES['unpacked'])
    TABLES['campaigns'].to_csv(LOCAL_DIR + 'campaigns.csv', index=False)

    #06 corrupt data
    TABLES['corrupt'] = corrupt(TABLES['unpacked'])
    TABLES['corrupt'].to_csv(LOCAL_DIR + 'corrupt.csv', index=False)

    #07 repack data
    key_field = 'storageId'
    pack_field = 'data1'
    fields_to_pack = list(set(
        TABLES['corrupt'].columns).difference(
        set(TABLES[s_table_name].columns))) + [key_field]
    TABLES['marketing_sales'] = pack_fields(
        TABLES['corrupt'], fields_to_pack, key_field, pack_field)
    TABLES['marketing_sales'].to_csv(LOCAL_DIR + 'marketing_sales.csv', index=False)


def campaign_spending(leads):
    def spending_from_leads(ld, c):
        return c * (1 + 0.25 * (-1 + 2 * random.random())) * ld

    #get campaign start date and lead counts
    campaigns_by_start_date = leads[
        ['createDate', 'utm_campaign_8']].groupby(
        'utm_campaign_8').min()['createDate']
    campaigns_by_lead_count = leads[
        ['VisitorId', 'utm_campaign_8']].groupby(
        'utm_campaign_8').count()['VisitorId']

    #combine to create campaign table
    campaigns = pd.DataFrame({'start_date': campaigns_by_start_date,
                              'lead_count': campaigns_by_lead_count})
    campaigns.index.name = 'utm_campaign_8'

    #divide into early and late campaigns
    mid_year = dt.datetime(year=2021, month=7, day=1)
    early_campaigns = campaigns[campaigns.start_date < mid_year].copy()
    late_campaigns = campaigns[campaigns.start_date >= mid_year].copy()

    #apply different coefficients to early and late campaigns, then combine together
    coeff = [4, 8]
    early_campaigns['amount_spent'] = early_campaigns['lead_count'].apply(
        lambda x: spending_from_leads(x, coeff[0]))
    late_campaigns['amount_spent'] = late_campaigns['lead_count'].apply(
        lambda x: spending_from_leads(x, coeff[1]))
    campaign_table = pd.concat([early_campaigns, late_campaigns], axis=0)

    #drop nonsense campaign ids
    campaign_table = campaign_table.drop(['Agent', 'placeholder', ''])

    #remove lead counts
    del campaign_table['lead_count']

    #reset index to include campaign id in the list of fields
    campaign_table.reset_index(inplace=True)
    return campaign_table


def corrupt(clean):
    corrupt = clean.copy()

    #01 drop records in March
    blackout_start = dt.datetime(year=2021, month=3, day=1)
    blackout_end = dt.datetime(year=2021, month=3, day=31)
    corrupt = corrupt[
        (corrupt.createDate < blackout_start) |
        (corrupt.createDate > blackout_end)].copy()
    corrupt.reset_index(inplace=True)
    del corrupt['index']

    #02 replace some VisitorIds with 0
    n_blank_visitor_id = 5
    blank_visitor_row_ids = random.sample(range(0, len(corrupt)-1),
                                          n_blank_visitor_id)
    corrupt.loc[blank_visitor_row_ids, 'VisitorId'] = 0

    #03 campaign name add invalid characters spaces " "
    n_space_rows = 4000
    excluded_campaigns = ['Agent', '', 'placeholder']
    valid_campaigns = corrupt[~corrupt.utm_campaign_8.isin(
        excluded_campaigns
    )].copy()
    valid_campaigns.reset_index(inplace=True)
    space_row_ids = random.sample(range(0, len(valid_campaigns) - 1),
                                          n_space_rows)
    valid_campaigns.loc[
        space_row_ids,
        'utm_campaign_8'] = valid_campaigns.loc[
                                    space_row_ids,
                                    'utm_campaign_8'] + ' '
    valid_campaigns.set_index('index', inplace=True)
    corrupt.update(valid_campaigns)

    #04 add '' in create and modified date
    n_blank_dates = 500
    blank_date_row_ids = random.sample(range(0, len(corrupt) - 1),
                                  n_blank_dates)
    corrupt.loc[blank_date_row_ids, 'createDate'] = ''
    corrupt.loc[blank_date_row_ids, 'modifiedDate'] = ''
    return corrupt


def pack_fields(unpacked, fields_to_pack, key_field, pack_field):
    packed = unpacked.copy()
    to_pack = packed[fields_to_pack].copy()
    records = [str(dict(zip(to_pack.columns, x)))
               for x in to_pack.to_records(index=False)]
    fields_to_keep = [f for f in packed.columns
                      if not f in fields_to_pack] + [key_field]
    packed = packed[fields_to_keep].copy()
    packed[pack_field] = pd.Series(records)
    return packed


def form_submissions_unpack(form_submission_values):
    """ unpacks the contents from the form submissions in data1 field
        as additional columns in the table
    """
    unpacked_list = [unpack_form_contents(x) for x in form_submission_values]
    unpacked = pd.concat(unpacked_list)
    return unpacked


def unpack_form_contents(form_contents_str):
    """ returns a single dataframe row with field header from json as string
        which contains a list of {id, value} pairs, where id = field name
    """
    form_details = json.loads(form_contents_str)
    storage_id = int(form_details['formStorageId'])
    contents_json = json.loads(form_details['content'])['step1']
    key_value = pd.DataFrame.from_records(contents_json)
    form_contents_df = pd.DataFrame(
        data=[[storage_id] + list(key_value.value.values)],
        columns=['storageId'] + list(key_value.id.values))
    return form_contents_df

# ----------------------------------------------------------------------------------
# console
#----------------------------------------------------------------------------------


if __name__ == "__main__":
    setup()

# ----------------------------------------------------------------------------------
# END
#----------------------------------------------------------------------------------


