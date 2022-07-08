# config must be valid python dictionary
{
    # okta service account
    'OKTA_UID': r'mdijkhof',
    'OKTA_PWD': r'seinfeld66P$',

    # teradata
    'SQL_UID': r'mdijkhof',
    'SQL_PWD': r'seinfeld66P%',
    'SQL_ADMINS': [
        r'mdijkhof',r'ub_intl_sales_ops',r'ub_intl_sales_ops_2',r'ub_intl_sales_ops_3'
    ],  # list of accounts which will be granted all privileges to all tables created with megatron

    # cerebro & hive
    'HIVE_UID': r'XXXXXXXXX',
    'HIVE_DB': r'',  # hive schema_name to use when connecting & creating tables. will fall back to default ({HIVE_UID}_db) if this is empty
    'HIVE_HDFS_LOCATION': r'',  # path to hiveDB.db file, e.g. hdfs://cerebro-namenode/user/grp_gdoop_ima/ima_hiveDB.db. will fall back to default (hdfs://cerebro-namenode/user/{HIVE_UID}/{HIVE_UID}_hiveDB.db) if this is empty
    'HIVE_PKEY': r'',  # private keyfile path. it needs to match HIVE_UID. will fall back to '~/.ssh/id_rsa' if this is empty
    'HIVE_PKEY_PWD': r'',  # private keyfile password (fill in if needed, otherwise leave empty)

    # salesforce staging (only needed for testing)
    'SFDC_UID__sandbox': r'XXXXXXXXX@groupon.com.staging',
    'SFDC_PWD__sandbox': r'XXXXXXXXX',
    'SFDC_TOKEN__sandbox': r'XXXXXXXXX',

    # salesforce production
    'SFDC_UID': r'mdijkhof@groupon.com',
    'SFDC_PWD': r'seinfeld66P$',
    'SFDC_TOKEN': r'XXXXXXXXX',
    'SFDC_CLIENT_ID': r'XXXXXXXXXXXXX',
    'SFDC_CLIENT_SECRET': r'XXXXXXXXXXXXXXXXXXX',

    # all logs & dumpfiles will be saved in this gdrive folder
    'GDRIVE_LOG_FOLDER_ID': r'12vYeKS6x28-3TEKd2W5xppI-y6B-5yVT',  # just id, without 'https://drive.google.com/drive/folders/'

    # admins will receive all emails sent with mailing module
    'ADMINS': [
        r'mdijkhof@groupon.com',
    ],

    # additional config for mailing module (email formatting)
    'EMAIL_SIGNATURE': r'Mark Dijkhof',
    'EMAIL_DISCLAIMER': r"""
    <font size="2" color="#999999">
    This is an automatically generated email.
    If you have any questions, please reply to
    <a href="mailto:mdijkhof@groupon.com">
    <font color="#999999">mdijkhof@groupon.com</font></a>
    </font>
    """,
    'NO_REPLY_DISCLAIMER': r"""
    <font size="2" color="#999999">
    This is an automatically generated email, please do not reply!
    </font>
    """,
}
© 2022 GitHub, Inc.
Help
Support
API