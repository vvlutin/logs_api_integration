import time
import datetime
import sys
import logging
from collections import namedtuple
import utils
import logs_api
import clickhouse
import vertica


def setup_logging(conf):
    global logger
    logger = logging.getLogger('logs_api')
    logging.basicConfig(stream=sys.stdout,
                        level=conf['log_level'],
                        format='%(asctime)s %(processName)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', )


def get_date_period(opt):
    """Get date limits tuple from opt"""
    if opt.mode is None:
        start_date_str = opt.start_date
        end_date_str = opt.end_date
    else:
        if opt.mode == 'regular':
            start_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
                .strftime(utils.DATE_FORMAT)
            end_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
                .strftime(utils.DATE_FORMAT)
        elif opt.mode == 'regular_early':
            start_date_str = (datetime.datetime.today() - datetime.timedelta(1)) \
                .strftime(utils.DATE_FORMAT)
            end_date_str = (datetime.datetime.today() - datetime.timedelta(1)) \
                .strftime(utils.DATE_FORMAT)
        elif opt.mode == 'history':
            start_date_str = utils.get_counter_creation_date(
                config['counter_id'],
                config['token']
            )
            end_date_str = (datetime.datetime.today() - datetime.timedelta(2)) \
                .strftime(utils.DATE_FORMAT)
        else:
            raise ValueError("Wrong 'mode' parameter: mode = {mode}".format(mode=opt.mode))
    return start_date_str, end_date_str


def build_user_request(conf, opt, counter=None, span=None):
    """Create user request as a named tuple"""
    if span is None or len(span) == 0:
        start_date_str, end_date_str = get_date_period(opt)
    else:
        start_date_str, end_date_str = span[0], span[1]
    source = opt.source

    # Validate that fields are present in conf
    assert '{source}_fields'.format(source=source) in conf, \
        'Fields must be specified in conf'
    fields = conf['{source}_fields'.format(source=source)]

    # Creating data structure (immutable tuple) with initial user request
    UserRequest = namedtuple(
        "UserRequest",
        "app_id token counter_id start_date_str end_date_str source fields retries retries_delay dump_path"
    )

    user_req = UserRequest(
        app_id=conf['app_id'],
        token=conf['token'],
        counter_id=(counter or conf['counter_id']),
        start_date_str=start_date_str,
        end_date_str=end_date_str,
        source=source,
        fields=tuple(fields),
        retries=conf['retries'],
        retries_delay=conf['retries_delay'],
        dump_path=conf['dump_path']
    )

    utils.validate_user_request(user_req)  # unnecessary check
    return user_req


def integrate_with_logs_api(user_req, dest):
    """Attempt fetching data from Logs API and saving to dest (clickhouse, vertica)"""
    for i in range(user_req.retries):
        time.sleep(i * user_req.retries_delay)
        try:
            # Creating API requests
            api_requests = logs_api.get_api_requests(user_req)

            for api_request in api_requests:
                logger.info('### CREATING TASK for counter_id = {counter}, start = {start}, end = {end}'
                            .format(counter=user_request.counter_id,
                                    start=user_request.start_date_str, end=user_request.end_date_str))
                logs_api.create_task(api_request)
                delay = 20
                while api_request.status != 'processed':
                    logger.info('### DELAY %d secs' % delay)
                    time.sleep(delay)
                    logger.info('### CHECKING STATUS')
                    api_request = logs_api.update_status(api_request)
                    logger.info('API Request status: ' + api_request.status)

                logger.info('### SAVING DATA')
                for part in range(api_request.size):
                    logger.info('Part #' + str(part))
                    logs_api.save_data(api_request, part, dest)

                logger.info('### CLEANING DATA')
                logs_api.clean_data(api_request)
        except Exception as e:
            logger.critical('Iteration #{i} failed'.format(i=i + 1))
            if i == user_req.retries - 1:
                raise e


if __name__ == '__main__':

    start_time = time.time()

    config = utils.get_config()
    setup_logging(config)
    options = utils.get_cli_options()

    # choose from available destinations
    if (options.dest is None) or (options.dest == 'clickhouse'):
        destination = clickhouse
    elif options.dest == 'vertica':
        destination = vertica
    else:
        raise ValueError('Wrong argument: dest = ' + options.dest)

    user_request = build_user_request(config, options)

    # choose counter [from config | from cli options | all avaibalbe counters]
    if options.counter is None:
        counters = (config['counter_id'],)
    elif options.counter == 'all':
        counters = logs_api.get_active_counters(user_request)
    elif options.counter != 'all':
        counters = (options.counter,)
    else:
        raise ValueError('Wrong argument: counter = ' + options.counter)

    for cntr in counters:
        user_request = build_user_request(config, options, counter=cntr)

        # If data for specified period is already in database, script is skipped
        missing_time_spans = destination.data_missing_time_spans(user_request)
        logger.info('Required timespans for counter_id = {counter}, start = {start}, end = {end}: {ts}'
                    .format(counter=user_request.counter_id,
                            start=user_request.start_date_str, end=user_request.end_date_str,
                            ts=missing_time_spans))

        if len(missing_time_spans) == 0:
            logger.info('### DATA IS PRESENT FOR counter={counter}, start_date={start}, end_date={end}'
                        .format(counter=cntr, start=user_request.start_date_str, end=user_request.end_date_str))

        for timespan in missing_time_spans:
            user_request = build_user_request(config, options, counter=cntr, span=timespan)
            logger.info('User request: {user_request}'.format(user_request=user_request))
            integrate_with_logs_api(user_request, destination)

    destination.clean_data(user_request.source)

    end_time = time.time()
    logger.info('### TOTAL TIME: %d minutes %d seconds' % (
        (end_time - start_time) / 60,
        (end_time - start_time) % 60
    ))

