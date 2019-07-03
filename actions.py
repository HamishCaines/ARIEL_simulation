#################################################################
# This is the main wheelhouse for the simulator                 #
# Contains the main operations needed to run the simulator      #
# Calls auxilliary files for large operations involving         #
# querying databases or mathematical functions                  #
#                                                               #
# Hamish Caines 07-2019                                         #
#################################################################


class Database:
    """
    Database object, contains database and cursor used to make changes.
    Functions within operate on the database as part of the simulation
    """
    def __init__(self, database, telescopes, mode, threshold):
        """
        Constructor that connects to, and stores information about the simulation
        :param database: name of database file for the simulation: str
        :param telescopes: location of telescope file: str
        :param mode: Operating mode to be used in the simulation: str
        :param threshold: Accuracy threshold for the simulation in minutes: int
        """
        import sqlite3
        self.db = sqlite3.connect(database)
        self.cursor = self.db.cursor()
        self.names = self.read_target_names()
        self.telescope_file = telescopes  # store location
        self.telescope_setup = telescopes.split('/')[-1].split('.')[0]  # store file name
        self.telescope_data = []
        self.mode = mode
        self.threshold = threshold

    def update(self, table, column, row, value):
        """
        Update single value for single row in SQL table
        :param table: Table name: str
        :param column: Column name: str
        :param row: Row to update: str
        :param value: New value: int or float
        """
        self.cursor.execute('UPDATE '+table+' SET '+column+' = '+str(value)+' WHERE Name = \''+row+'\'')

    def read_target_names(self):
        """
        Read all target names from database
        :return: List of target names
        """
        rows = self.cursor.execute('SELECT Name FROM TARGET_DATA').fetchall()
        names = []
        # extract strings from tuples
        for row in rows:
            names.append(row[0])
        return names

    def run_queries(self):
        """
        Query databases to obtain known observations and missing data for each target
        Queries ETD for observations, and exoplanets.org for missing data
        """
        import query_tools
        total = len(self.names)
        count = 1
        print('Starting queries for '+str(total)+'...')
        for name in self.names:
            try:
                float(name[:-1])  # check for real target, fake star names only contain numbers
                print('Target ' + name + ' is not real (' + str(count) + '/' + str(total) + ')')
            except ValueError:  # catch error for real targets
                print('Querying ' + name + ' ('+str(count)+'/'+str(total)+')')
                target = query_tools.Target(name)  # initialise Target object
                target.ETD_query()  # obtain observation data
                # check for missing data and query exoplanets.org to fill gaps
                if list(target.__dict__.values()).__contains__(None):
                    target.EXO_query()
                    target.EXO_used = True
                target.write_query_data(self.cursor, self.db)  # write results
            count += 1

        print('Initial data obtained, now fitting periods...')
        self.initial_period_fit()  # perform initial period fit for initial observation available
        self.initial_prop_to_ariel()  # calculate uncertainties at ARIEL launch based on current observation data

    def initial_period_fit(self):
        """
        Perform initial period fit for each target based on initial observation data
        :return:
        """
        import data_tools
        total = len(self.names)
        count = 1
        for target in self.names:
            obs = data_tools.read_obs_data(self.cursor, target)  # load target data
            try:
                print('Fitting ' + target + ' ('+str(count)+'/'+str(total)+')')
                # perform fit
                fit_period, fit_period_err, latest_tmid, latest_tmid_err, latest_epoch = data_tools.period_fit(obs)
                # store values in table
                self.update('TARGET_DATA', 'FitPeriod', target, fit_period)
                self.update('TARGET_DATA', 'FitPeriodErr', target, fit_period_err)
                self.update('TARGET_DATA', 'CurrentPeriod', target, fit_period)
                self.update('TARGET_DATA', 'CurrentPeriodErr', target, fit_period_err)
                self.update('TARGET_DATA', 'TruePeriod', target, fit_period)
                self.update('TARGET_DATA', 'TruePeriodErr', target, fit_period_err)
                self.update('TARGET_DATA', 'TrueEpoch', target, latest_epoch)
                self.update('TARGET_DATA', 'TrueLastObs', target, latest_tmid)
                self.update('TARGET_DATA', 'TrueLastObsErr', target, latest_tmid_err)

                # check for missing error data on last observation and store latest value with error available
                if self.cursor.execute('SELECT LastObsErr FROM TARGET_DATA WHERE Name = \''+target+'\'').fetchall()[0]\
                        is None:
                    self.update('TARGET_DATA', 'LastObs', target, latest_tmid)
                    self.update('TARGET_DATA', 'LastObsErr', target, latest_tmid_err)
                    self.update('TARGET_DATA', 'LastEpoch', target, latest_epoch)

                # check for new latest observation
                elif latest_tmid > self.cursor.execute('SELECT LastObs FROM TARGET_DATA WHERE '
                                                       'Name = \''+target+'\'').fetchall()[0][0]:
                    self.update('TARGET_DATA', 'LastObs', target, latest_tmid)
                    self.update('TARGET_DATA', 'LastObsErr', target, latest_tmid_err)
                    self.update('TARGET_DATA', 'LastEpoch', target, latest_epoch)

            except Warning:  # thrown by period_fit
                print('Fit for ' + target + ' failed, has '+str(len(obs))+' observations')
                self.cursor.execute(
                    'UPDATE TARGET_DATA SET CurrentPeriod = PeriodStart, CurrentPeriodErr = PeriodStartErr WHERE '
                    'FitPeriod IS NULL')
                self.cursor.execute(
                    'UPDATE TARGET_DATA SET TruePeriod = PeriodStart, TruePeriodErr = PeriodStartErr WHERE FitPeriod IS'
                    ' NULL')
                self.cursor.execute('UPDATE TARGET_DATA SET TrueEpoch = LastEpoch WHERE TrueEpoch IS NULL')

            finally:
                count += 1
                self.db.commit()

    def initial_prop_to_ariel(self):
        """
        Calculate timing uncertainty at ARIEL launch for each target based on current observation data available
        """
        import data_tools

        # obtain required data for all targets
        self.cursor.execute(
            'SELECT Name, CurrentPeriod, CurrentPeriodErr, LastObs, LastObsErr, Duration FROM TARGET_DATA '
            'WHERE CurrentPeriodErr NOT NULL AND LastObsErr NOT NULL')
        rows = self.cursor.fetchall()
        # for each target
        for row in rows:
            name = row[0]
            try:
                err_tot, percent, loss = data_tools.prop_forwards(row)  # calculate propagated error
                # store values in table
                self.update('TARGET_DATA', 'ErrAtAriel', name, err_tot)
                self.update('TARGET_DATA', 'PercentLoss', name, percent)
                self.update('TARGET_DATA', 'LossAtAriel', name, loss)
                self.update('TARGET_DATA', 'ErrAtArielStart', name, err_tot)
                self.update('TARGET_DATA', 'PercentLossStart', name, percent)
                self.update('TARGET_DATA', 'LossAtArielStart', name, loss)
            except Warning:
                pass

    def transit_forecast(self, start, end):
        """
        Forecast exoplanet transits visible from somewhere on earth within the window specified by arguments
        :param start: Start of forecast window: datetime
        :param end: End of forecast window: datetime
        """
        import observation_tools
        import julian

        #  make table for transit data if not exists
        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS DEEP_TRANSITS(Center DATETIME, Name VARCHAR(20), Ingress DATETIME, Egress DATET'
            'IME, Duration TIME, RA DECIMAL(9,7), Dec DECIMAL(9,7), PercentLoss REAL, Epoch REAL, ErrAtAriel DECIMAL(16'
            ',8))')

        start_jd = julian.to_jd(start, fmt='jd') - 2400000  # convert start to JD format in table
        # select targets with deep transits and discovery dates in the past, AND require further observation
        rows = self.cursor.execute('SELECT Name FROM TARGET_DATA WHERE Depth > 10.0 AND "' + str(
            start_jd) + '" > LastObs AND (ErrAtAriel*24*60 > ' + str(
            self.threshold) + ' OR ErrAtAriel IS NULL)').fetchall()

        # obtain deep names and reset names in database
        deep_names = []
        for row in rows:
            deep_names.append(row[0])
        self.names = deep_names

        # for each deep target
        for name in self.names:
            data = observation_tools.read_data(self.cursor, name)
            transits = observation_tools.transit_forecast(data, name, start, end)  # forecast transits for target

            for transit in transits:
                # add new transit to the table
                string = 'INSERT INTO DEEP_TRANSITS (Center, Name, Ingress, Egress, Duration, RA, Dec, PercentLoss, ' \
                         'Epoch, ErrAtAriel) VALUES ( \'' + str(transit.center) + '\', \'' + transit.name + '\', \'' \
                         + str(transit.ingress) + '\', \'' + str(transit.egress) + '\', \'' + str(
                            transit.duration) + '\', ' + str(transit.ra) + ', ' + str(transit.dec) + ', '
                # check for missing error values and modify string accordingly
                if transit.error is None:
                    string += 'NULL, ' + str(transit.epoch) + ', NULL)'
                else:
                    string += str(transit.loss) + ', ' + str(transit.epoch) + ', ' + str(transit.error) + ')'
                self.cursor.execute(string)

        self.db.commit()

    def load_telescope_data(self):
        """
        Create Telescope objects for telescopes in database and store in Database object
        """
        import observation_tools
        self.cursor.execute('SELECT * FROM TELESCOPES')  # query database
        rows = self.cursor.fetchall()
        telescopes = []
        # for each telescope
        for row in rows:
            new_telescope = observation_tools.Telescope()
            new_telescope.gen_from_database(row)
            telescopes.append(new_telescope)

        self.telescope_data = telescopes

    def obtain_upcoming_transits(self, start_date, interval):
        """
        Obtain transits in the given interval and check visibility from specific telescopes
        :param start_date: Start of window: datetime
        :param interval: Length of window: timedelta
        :return: List of Transit objects for transits visible from a telescope in the given window
        """
        import observation_tools
        # find transits in window
        end_date = start_date + interval
        self.cursor.execute('SELECT * FROM DEEP_TRANSITS WHERE Center BETWEEN "' + str(start_date.date()) + ' 00:00:00"'
                            ' AND "' + str(end_date.date()) + ' 00:00:00"')
        rows = self.cursor.fetchall()
        self.load_telescope_data()  # store telescopes in object
        transits = []
        # for each transit
        for row in rows:
            new_transit = observation_tools.Transit()
            new_transit.gen_from_database(row)
            # check visibility from telescopes available
            if new_transit.check_visibility_telescopes(self.telescope_data):
                # check for missing propagated error values and set to high values
                if new_transit.error is None:
                    new_transit.loss = 1000
                    new_transit.error = 1000
                # check if target is constrained
                if new_transit.error*24*60 > self.threshold:
                    print(new_transit.name + ' is needed, current error: ' + str(new_transit.error * 24 * 60))
                    transits.append(new_transit)  # add to list

        transits.sort(key=lambda x: x.loss, reverse=True)  # sort transits by error, highest error at top of list

        return transits

    def make_schedules(self, start_date, interval, mode):
        """
        Schedule observations for each telescope according to the operating mode specified, giving priority to
        targets with the highest error at ARIEL
        :param start_date: Start of window: datetime
        :param interval: Length of window: timdelta
        :param mode: Operating mode: str
        """
        import numpy as np

        transits = self.obtain_upcoming_transits(start_date, interval)

        # check each telescope against all transits
        for telescope in self.telescope_data:
            matching_transits = []
            for transit in transits:
                if telescope.name in transit.telescope:
                    matching_transits.append(transit)

            # set limit to number of observations according to operating mode
            if mode == 'unlimited':
                limit = np.inf
            elif 'perweek' in mode:
                limit = int(mode.split('perweek')[0])
            else:
                print('NO VALID MODE SPECIFIED')
                raise IOError

            self.schedule(matching_transits, limit, telescope.name)  # make schedule

    def schedule(self, transits, limit, telescope):
        """
        Make schedule for a given telescope given a set of transits and a limit on the number of observations
        available in the interval
        :param transits: List of Transit objects for transits in the given window visible from the telescope
        :param limit: Maximum number of observations allowed in the interval
        :param telescope: Name of telescope being scheduled
        """
        from datetime import datetime, timedelta
        from sqlite3 import IntegrityError

        new_transits = 0  # set limit counter
        while new_transits < limit:  # loop while limit not reached
            # iterate through transits
            for transit in transits:
                # check already scheduled observations
                scheduled = self.cursor.execute('SELECT RunStart, RunEnd FROM "'+telescope+'"').fetchall()
                # set start and finish times including continuum observation time
                continuum = timedelta(minutes=45)
                new_start = transit.ingress - continuum
                new_end = transit.egress + continuum
                duration = new_end - new_start  # find duration
                # check for empty schedule and schedule transit if empty
                if len(scheduled) == 0:
                    try:
                        self.cursor.execute(
                            'INSERT INTO ' + telescope + ' VALUES ("' + transit.name + '", ' + str(transit.ra) +
                            ', ' + str(transit.dec) + ', "' + str(transit.center) + '", "' + str(
                                new_start) + '", "' + str(new_end) + '", "' +
                            str(duration) + '", ' + str(transit.epoch) + ')')
                        new_transits += 1  # increment number of scheduled observations

                    except IntegrityError:
                        pass
                # if schedule is not empty, need to check for space in schedule
                else:
                    space = True
                    for single in scheduled:  # check for each scheduled observations
                        # obtain start and end times
                        try:
                            start_time = datetime.strptime(single[0], '%Y-%m-%d %H:%M:%S.%f')
                            end_time = datetime.strptime(single[1], '%Y-%m-%d %H:%M:%S.%f')
                        except ValueError:
                            start_time = datetime.strptime(single[0], '%Y-%m-%d %H:%M:%S')
                            end_time = datetime.strptime(single[1], '%Y-%m-%d %H:%M:%S')

                        if start_time < new_start < end_time:  # new observation starts before current one ends
                            space = False
                        if start_time < new_end < end_time:  # new observation ends after current one starts
                            space = False
                        if new_start < start_time and end_time < new_end:  # new observation surrounds current one
                            space = False

                    if space:  # schedule if space is available
                        try:
                            self.cursor.execute(
                                'INSERT INTO ' + telescope + ' VALUES ("' + transit.name + '", ' + str(
                                    transit.ra) +
                                ', ' + str(transit.dec) + ', "' + str(transit.center) + '", "' + str(
                                    new_start) + '", "' + str(new_end) + '", "' +
                                str(duration) + '", ' + str(transit.epoch) + ')')
                            new_transits += 1
                        except IntegrityError:
                            pass

            self.db.commit()
            # break when limit reached
            break

    def simulate_observations(self, start_date, interval):
        """
        Simulate the observation of transits scheduled for each telescope, generate data, and add to database
        :param start_date: Start of window: datetime
        :param interval: Length of window: timedelta
        """
        import observation_tools
        end_date = start_date + interval
        # observe for each telescope
        for telescope in self.telescope_data:
            # obtain schedule
            self.cursor.execute('SELECT * FROM "' + telescope.name + '" WHERE ObsCenter BETWEEN "' + str(
                start_date.date()) + ' 00:00:00" AND "' + str(end_date.date()) + ' 00:00:00"')
            observations = self.cursor.fetchall()

            # "observe" each transit
            for observation in observations:
                success = observation_tools.flip_unfair_coin()  # determine success of observation at random

                if success:
                    print('Observed ' + observation[0] + ' from ' + telescope.name + ' at ' + str(observation[3]))

                    # obtain current data for target
                    self.cursor.execute(
                        'SELECT LastObs, LastObsErr, LastEpoch, CurrentPeriod, TrueLastObs, TruePeriod, TrueEpoch FROM '
                        'TARGET_DATA WHERE Name = "' + observation[0] + '"')
                    data = self.cursor.fetchall()[0]
                    new_epoch = observation[-1]  # obtain epoch for new observation
                    last_tmid, last_tmid_err, last_epoch, period, true_t0, true_period, true_epoch = \
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6]

                    # generate new data based on target information
                    new_tmid, new_tmid_err = observation_tools.generate_results(last_tmid, last_tmid_err, last_epoch,
                                                                                new_epoch, period)
                    # obtain true ephemeris based on expected period
                    if true_t0 is not None:
                        true_center = observation_tools.find_true_t0(true_t0, true_period, true_epoch, new_epoch)
                    else:
                        true_center = 'NULL'
                    # add new observation to the database
                    self.add_new_observation(observation[0], new_epoch, new_tmid, new_tmid_err,
                                             telescope.name, true_center)
                    self.db.commit()

    def add_new_observation(self, name, epoch, tmid, tmid_err, telescope, true_center):
        """
        Add a new observation to the database and trigger recalculation period and error at ARIEL launch
        :param name: Name of target: str
        :param epoch: Epoch of observation: int
        :param tmid: Observed transit center: float
        :param tmid_err: Error in observed transit center: float
        :param telescope: Name of telescope used: str
        :param true_center: Expected ephemeris based on expected period: float
        :return:
        """
        import observation_tools
        from sqlite3 import IntegrityError

        # find new observation ID
        ids_raw = self.cursor.execute('SELECT ObID FROM "' + name + '"').fetchall()
        new_id = observation_tools.find_highest_id(ids_raw)

        # insert into database
        try:
            self.cursor.execute('INSERT INTO "' + name + '" (ObID, Epoch, ObsCenter, ObsCenterErr, TrueCenter, Source) '
                                                         'VALUES (' + str(new_id) + ', ' + str(epoch) + ', '
                                + str(tmid) + ', ' + str(tmid_err) + ', ' + str(true_center)+', "'+telescope + '")')
            self.cursor.execute('UPDATE TARGET_DATA SET NoOfObs = NoOfObs + 1 WHERE Name = "'+name+'"')
            self.db.commit()
        except IntegrityError:
            pass

        self.recalculate(name)  # trigger recalculation based on new data

    def recalculate(self, name):
        """
        Recalculate period and error at ARIEL launch based on data currently in database
        :param name: Target name to be recalculated: str
        """
        import data_tools
        import julian
        import datetime

        ######################
        # PERIOD FIT SECTION #
        ######################

        # obtain current observations
        observations = data_tools.read_obs_data(self.cursor, name)
        # fit new period and update table values
        try:
            new_period, new_period_err, tmid_max, tmid_max_err, last_epoch = data_tools.period_fit(observations)
            self.update('TARGET_DATA', 'CurrentPeriod', name, new_period)
            self.update('TARGET_DATA', 'CurrentPeriodErr', name, new_period_err)
            self.update('TARGET_DATA', 'LastObs', name, tmid_max)
            self.update('TARGET_DATA', 'LastObsErr', name, tmid_max_err)
            self.update('TARGET_DATA', 'LastEpoch', name, last_epoch)
            #self.update('TARGET_DATA', 'TrueLastObs', name, true_center)
        except Warning:
            # insufficient observations for period fit, 3 required
            if observations is None:
                print('Fit for '+name+'failed, has no observations')
            else:
                if len(observations) != 1:
                    print('Fit for '+name+' failed, has '+str(len(observations))+' observations')
                else:
                    print('Fit for ' + name + ' failed, has ' + str(len(observations)) + ' observation')
        self.db.commit()

        ######################
        # ERROR PROP SECTION #
        ######################

        # obtain current data for target after period fit
        self.cursor.execute(
            'SELECT Name, CurrentPeriod, CurrentPeriodErr, LastObs, LastObsErr, Duration FROM TARGET_DATA '
            'WHERE CurrentPeriodErr NOT NULL AND LastObsErr NOT NULL AND Name = \'' + name + '\'')
        data = self.cursor.fetchall()
        if len(data) != 0:  # check for failed SELECT
            data_row = data[0]
            # check if ARIEL launch has been reached
            if data_row[3] < (julian.to_jd(datetime.datetime(year=2030, month=6, day=12, hour=0, minute=0, second=0),
                                           fmt='jd') - 2400000):
                # repropagate and store results in table
                err_tot, percent, loss = data_tools.prop_forwards(data_row)
                self.update('TARGET_DATA', 'ErrAtAriel', name, err_tot)
                self.update('TARGET_DATA', 'PercentLoss', name, percent)
                self.update('TARGET_DATA', 'LossAtAriel', name, loss)

    def find_earliest_date(self):
        """
        Find date of earliest transit in database
        :return:
        """
        import datetime
        self.cursor.execute('SELECT Center FROM DEEP_TRANSITS')  # find all transits
        rows = self.cursor.fetchall()
        lowest = min(rows)  # find earliest date
        return datetime.datetime.strptime(lowest[0], '%Y-%m-%d %H:%M:%S.%f')  # convert to datetime and return

    def store_results(self, count, total):
        """
        Store running total data in results table
        :param count: Number of constrained targets: int
        :param total: Total targets in simulation: int
        """
        # create results table
        self.cursor.execute('CREATE TABLE IF NOT EXISTS RESULTS (Network VARCHAR(25), Mode VARCHAR(25), Accuracy REAL, '
                            'Constrained REAL, Total REAL, Percent REAL)')
        percent = count/total*100
        # store values
        self.cursor.execute('INSERT INTO RESULTS VALUES ("'+self.telescope_setup+'", "'+self.mode+'", '
                            ''+str(self.threshold)+', '+str(count)+', '+str(total)+', '+str(percent)+')')
        self.db.commit()

    def check_constrained(self, current):
        """
        Check number of constrained targets at the current date
        :param current: Current date in simulation: datetime
        :return count: Number of constrained targets: int
        :return total: Number of targets in simulation: int
        """
        import julian
        current_jd = julian.to_jd(current, fmt='jd') - 2400000  # convert to JD format used in table
        # find all deep targets
        total = len(self.cursor.execute(
            'SELECT Name FROM TARGET_DATA WHERE LastObs < ' + str(current_jd) + ' AND Depth > 10.0').fetchall())
        # find all constrained deep targets
        count = len(self.cursor.execute('SELECT Name FROM TARGET_DATA WHERE LastObs < '+str(current_jd)+' '
                                        'AND Depth > 10.0 AND ErrAtAriel*24*60 < '+str(self.threshold)).fetchall())
        print('Constrained '+str(count)+'/'+str(total)+' targets on '+str(current.date()))
        return count, total






