import os
import datetime
import logging
import pyodbc
import gzip
from collections import namedtuple
import utils

config = utils.get_config()
VT_HOST = config['vertica']['host']
VT_USER = config['vertica']['user']
VT_PASSWORD = config['vertica']['password']
VT_VISITS_TABLE = config['vertica']['visits_table']
VT_HITS_TABLE = config['vertica']['hits_table']
VT_DATABASE = config['vertica']['database']

logger = logging.getLogger('logs_api')


def get_message(name: str) -> str:
    """Returns errors and warning string"""
    if name == 'connect_error':
        return 'Unable to connect to Vertica:\n\tHOST={server},\n\tDATABASE={db},\n\tUSER={user}' \
            .format(server=VT_HOST, db=VT_DATABASE, user=VT_USER)
    elif name == 'close_warning':
        return 'Unable to close the connection to Vertica:\n\tHOST={server},\n\tDATABASE={db},\n\tUSER={user}' \
            .format(server=VT_HOST, db=VT_DATABASE, user=VT_USER)
    else:
        raise ValueError('Wrong argument: ' + name)


def disconnect(handler):
    try:
        handler.con.close()
    except Exception as e:
        logger.warning(get_message('close_warning'))


def get_handler():
    connection_string = 'Driver=Vertica;Servername={server};Port=5433;Database={db};UserName={user};Password={psw}' \
        .format(server=VT_HOST, db=VT_DATABASE, user=VT_USER, psw=VT_PASSWORD)
    try:
        con = pyodbc.connect(connection_string)
        cursor = con.cursor()
    except Exception as e:
        logger.critical(get_message('connect_error'))
        raise e
    DbHandler = namedtuple('DbHandler', 'cursor con')
    return DbHandler(cursor=cursor, con=con)


def get_data(handler, query: str) -> list:
    """Returns Vertica response"""
    logger.debug(query)
    try:
        handler.cursor.execute(query)
        rows = handler.cursor.fetchall()
    except Exception as e:
        logger.critical(get_message('connect_error'))
        disconnect(handler)
        raise e
    return rows


def upload(user_req, handler, content: bytes, part):
    """Uploads data to table in Vertica"""
    dump_file = os.path.join(user_req.dump_path, 'content.tsv.gz')

    rejected_file = os.path.join(user_req.dump_path, 'rejected_{counter}_{start}_{end}_{part}.txt'
                                 .format(counter=user_req.counter_id,
                                         start=user_req.start_date_str, end=user_req.end_date_str,
                                         part=part))

    exceptions_file = os.path.join(user_req.dump_path, 'exceptions_{counter}_{start}_{end}_{part}.txt'
                                   .format(counter=user_req.counter_id,
                                           start=user_req.start_date_str, end=user_req.end_date_str,
                                           part=part))

    table = get_source_table_name(user_req.source)

    with gzip.open(dump_file, 'w') as data_dump:
        data_dump.write(content)

    query = """
            COPY {table}
            FROM LOCAL '{file}'
            GZIP DELIMITER E'\t'
            SKIP 1
            REJECTED DATA '{rejected}'
            EXCEPTIONS '{exceptions}';
        """.format(table=table, file=dump_file, rejected=rejected_file, exceptions=exceptions_file)

    try:
        handler.cursor.execute(query)
    except Exception as e:
        logger.critical("Unable to COPY FROM LOCAL FILE '{file}' TO TABLE {table}"
                        .format(file=dump_file, table=table))
        disconnect(handler)
        raise e

    # remove data dump
    try:
        os.remove(dump_file)
    except Exception as e:
        logger.warning('Unable to remove file: {dump}'.format(dump=dump_file))
    # remove rejected data and exceptions files if empty
    try:
        statinfo = os.stat(rejected_file)
        if statinfo.st_size == 0:
            logger.info("0 rejects occured: REMOVE '{rejects}'".format(rejects=rejected_file))
            os.remove(rejected_file)
    except Exception as e:
        logger.warning('Unable to remove file: {rejects}'.format(rejects=rejected_file))
    try:
        statinfo = os.stat(exceptions_file)
        if statinfo.st_size == 0:
            logger.info("0 exceptions occured: REMOVE '{exceptions}'".format(exceptions=exceptions_file))
            os.remove(exceptions_file)
    except Exception as e:
        logger.warning('Unable to remove file: {exceptions}'.format(exceptions=exceptions_file))

    # (1) remove dump
    # (2) remove rejected and exceptions if size = 0


def get_source_table_name(source) -> str:
    """Returns table name in database"""
    if source == 'hits':
        return VT_HITS_TABLE
    if source == 'visits':
        return VT_VISITS_TABLE


def get_tables(handler) -> list:
    """Returns list of tables in a database"""
    rows = get_data(handler, """SELECT table_schema, table_name from TABLES""")
    result = []
    for r in rows:
        result.append('.'.join(r).lower())
    return result


def is_table_present(handler, source) -> bool:
    """Returns whether table for data is already present in database"""
    return get_source_table_name(source).lower() in get_tables(handler)


def get_vt_field_name(field_name: str) -> str:
    """Converts Logs API parameter name to Vertica column name"""
    prefixes = ['ym:s:', 'ym:pv:']
    for prefix in prefixes:
        field_name = field_name.replace(prefix, '')
    return utils.camel_to_snake(field_name)


def drop_table(handler, source):
    """Drops table in Vertica"""
    table_name = get_source_table_name(source)

    query = 'DROP TABLE IF EXISTS {table};'.format(table=table_name)
    try:
        handler.cursor.execute(query)
    except Exception as e:
        logger.critical('Unable to DROP table ' + table_name)
        raise e


def create_table(handler, source, fields):
    """Creates table in Vertica for hits/visits with particular fields"""
    tmpl = '''
        CREATE TABLE {table_name} (
            {fields}
        ) ORDER BY {order_clause}
          SEGMENTED BY HASH({segmentation_clause}) ALL NODES;
    '''
    field_tmpl = '{name} {type}'
    field_statements = []

    table_name = get_source_table_name(source)

    vt_field_types = utils.get_fields_config('vertica')
    vt_fields = list(map(get_vt_field_name, fields))

    order_clause = ', '.join(vt_fields[:5])
    segmentation_clause = ', '.join(vt_fields[:3])

    for i in range(len(fields)):
        field_statements.append(field_tmpl.format(name=vt_fields[i],
                                                  type=vt_field_types[fields[i]]))

    query = tmpl.format(table_name=table_name,
                        order_clause=order_clause,
                        segmentation_clause=segmentation_clause,
                        fields=',\n'.join(field_statements))

    try:
        handler.cursor.execute(query)
        logger.info('Destination table is created: {name}'.format(name=table_name))
    except Exception as e:
        logger.critical('Unable to CREATE table ' + table_name)
        disconnect(handler)
        raise e


def save_data(user_req, data, part=None):
    """Inserts data into Vertica table"""
    handler = get_handler()

    if not is_table_present(handler, user_req.source):
        create_table(handler, user_req.source, user_req.fields)

    upload(user_req, handler, data, part)
    disconnect(handler)


def is_data_present(user_request) -> bool:
    """Returns whether there is a records in database for particular date range and source"""
    handler = get_handler()

    if not is_table_present(handler, user_request.source):
        return False

    table_name = get_source_table_name(user_request.source)
    query = '''
        SELECT count(*) cnt
        FROM {table}
        WHERE date between '{start_date}' AND '{end_date}';
    '''.format(table=table_name, start_date=user_request.start_date_str, end_date=user_request.end_date_str)

    rows = get_data(handler, query)
    disconnect(handler)

    return rows[0][0] > 0


def data_missing_time_spans(user_request) -> tuple:
    """Returns tuple of date spans of the form (start_date, end_date) for the given request parameters
        (user_request.counter_id, user_request.start_date_str, user_request.end_date_str)"""
    handler = get_handler()

    if not is_table_present(handler, user_request.source):
        # print('data_missing_time_spans', 'not is_table_present')
        return tuple([(user_request.start_date_str, user_request.end_date_str)])

    table_name = get_source_table_name(user_request.source)
    query = '''
        SELECT
            date,
            count(*) cnt
        FROM {table}
        WHERE date between '{start_date}' AND '{end_date}'
            AND counter_id = {counter}
        GROUP BY 1
        ORDER BY 1;
    '''.format(table=table_name, start_date=user_request.start_date_str,
               end_date=user_request.end_date_str, counter=user_request.counter_id)

    rows = get_data(handler, query)
    disconnect(handler)

    if len(rows) == 0:
        # print('data_missing_time_spans', 'len(rows) == 0')
        return tuple([(user_request.start_date_str, user_request.end_date_str)])
    else:
        # find missing dates
        dates = [d[0] for d in rows]
        # print('data_missing_time_spans', 'OTHER', 'dates', dates)
        start_date = datetime.datetime.strptime(user_request.start_date_str, utils.DATE_FORMAT)
        end_date = datetime.datetime.strptime(user_request.end_date_str, utils.DATE_FORMAT)
        required = [datetime.datetime.date(start_date + datetime.timedelta(days=i)) for i in range((end_date - start_date).days + 1)]
        # print('data_missing_time_spans', 'OTHER', 'required', required)
        missing = [d for d in required if d not in dates]
        # print('data_missing_time_spans', 'OTHER', 'missing', missing)

        # convert list of individual dates to list of date spans [(start_i, end_i)]
        spans, span = [], []
        for i in range(len(missing)):
            if len(span) == 0:
                span.append(missing[i])
            if missing[i] - datetime.timedelta(days=1) <= span[-1]:
                span.append(missing[i])
            else:
                spans.append(('{:%Y-%m-%d}'.format(span[0]), '{:%Y-%m-%d}'.format(span[-1])))
                span = []
            if i + 1 == len(missing):
                if len(span) == 0:
                    span.append(missing[i])
                spans.append(('{:%Y-%m-%d}'.format(span[0]), '{:%Y-%m-%d}'.format(span[-1])))
                span = []
        print('data_missing_time_spans', 'OTHER', 'spans', spans)
        return tuple(spans)


def clean_data(source):
    """Analyze table statistics"""
    handler = get_handler()

    if is_table_present(handler, source):
        table = get_source_table_name(source)
        try:
            handler.cursor.execute("""SELECT ANALYZE_STATISTICS('{table}');""".format(table=table))
        except Exception as e:
            logger.warning('Unable to analyze statistics for {table}.'.format(table=table))

    disconnect(handler)