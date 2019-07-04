#################################################################
# Code for handling the forecasting, visibility and observation #
# of upcoming transits                                          #
# Called by actions.py                                          #
#                                                               #
# Hamish Caines 07-2019                                         #
#################################################################


def flip_unfair_coin():
    """
    Determines the success of an observation by flipping a weighted coin
    :return: Success: boolean
    """
    import random
    chance = 0.6
    return True if random.random() < chance else False
    #return True


def generate_results(last_tmid, last_tmid_err, last_epoch, new_epoch, period):
    """
    Generates results from a simulated observation based on target data
    :param last_tmid: Transit center of latest observation: float
    :param last_tmid_err: Error in transit center of latest observation: float
    :param last_epoch: Epoch of latest observation: int
    :param new_epoch: Epoch of new observation: int
    :param period: Current period of the target: days
    :return: New transit center w/error: float
    """
    from random import gauss
    # calculate expected tmid from known period, last observation and epochs passed
    epoch_dif = new_epoch - last_epoch
    new_tmid_exp = last_tmid + period*epoch_dif
    #if last_tmid_err is not None:
    #    new_tmid = gauss(new_tmid_exp, last_tmid_err)
    #    new_tmid_err = abs(gauss(0, last_tmid_err))
    #else:
    #    new_tmid = new_tmid_exp
    #    new_tmid_err = 'NULL'
    # generate new values from expected tmid and nominal uncertainty
    new_tmid = gauss(new_tmid_exp, 0.5/24/60)
    new_tmid_err = abs(gauss(0.5, 0.01)/24/60)
    return new_tmid, new_tmid_err


def find_true_t0(true_t0, true_period, last_true_epoch, epoch):
    epoch_dif = epoch - last_true_epoch
    new_true_t0 = true_t0 + epoch_dif*true_period
    #TODO: Add errors?
    return new_true_t0


class Transit:
    """
    Transit object, contains data for a single transit
    Functions within check the visibility of the transit from specific locations
    """
    def __init__(self):
        """
        Null constructor, populated by gen_from_database
        """
        self.center = None
        self.name = None
        self.ingress = None
        self.egress = None
        self.duration = None
        self.ra = None
        self.dec = None
        self.loss = None
        self.telescope = []
        self.epoch = None
        self.error = None

    def gen_from_database(self, row):
        """
        Creates Transit object for a single row of data
        :param row: Array of values to be stored in Transit object
        """
        import datetime
        try:
            self.center = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            self.center = datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
        self.name = row[1]
        try:
            self.ingress = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            self.ingress = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
        try:
            self.egress = datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            self.egress = datetime.datetime.strptime(row[3], '%Y-%m-%d %H:%M:%S')
        time_split = row[4].split(':')
        self.duration = datetime.timedelta(hours=float(time_split[0]), minutes=float(time_split[1]), seconds=float(time_split[2]))
        self.ra = row[5]
        self.dec = row[6]
        self.loss = row[7]
        self.epoch = row[8]
        self.error = row[9]

    def check_visibility_telescopes(self, telescopes):
        """
        Checks the visibility of a transit from specific telescopes and stores acceptable ones in Transit object
        :param telescopes: List of Telescope objects transit will be checked against
        :return: Boolean for visibility
        """
        import mini_staralt
        from datetime import timedelta
        accept = False

        while not accept:
            # loop for telescopes
            for telescope in telescopes:
                # check is transit is during night at site
                sun_down = False
                sunset, sunrise = mini_staralt.sun_set_rise(
                    self.center.replace(hour=0, second=0, minute=0, microsecond=0), lon=telescope.lon, lat=telescope.lat, sundown=-12)
                # check sunrise/set against in/egress
                if self.ingress > sunset:
                    if self.egress < sunrise:
                        sun_down = True
                # if calculation was made for wrong day, step back and recalculate
                elif sunset > self.center:
                    sunset, sunrise = mini_staralt.sun_set_rise(
                        self.center.replace(hour=0, second=0, minute=0, microsecond=0) - timedelta(days=1),
                        lon=telescope.lon, lat=telescope.lat, sundown=-12)
                    # check sunrise/set against in/egress
                    if self.ingress > sunset:
                        if self.egress < sunrise:
                            sun_down = True

                if sun_down:  # continue only if transit is during night
                    try:
                        # calculate target rise/set times
                        target_rise, target_set = mini_staralt.target_rise_set(
                            self.center.replace(hour=0, minute=0, second=0, microsecond=0),
                            ra=self.ra, dec=self.dec, mintargetalt=20, lon=telescope.lon, lat=telescope.lat)
                        # check target rise/set against in/egress
                        if self.ingress > target_rise:
                            if self.egress < target_set:
                                self.telescope.append(telescope.name)  # add telescope name to viable list
                                accept = True

                        # if calculation made for wrong day, step back and recalculate
                        elif target_rise > self.center:
                            target_rise, target_set = mini_staralt.target_rise_set(
                                self.center.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
                                ra=self.ra, dec=self.dec, mintargetalt=20, lon=telescope.lon, lat=telescope.lat)
                            # check target rise/set against in/egress
                            if self.ingress > target_rise:
                                if self.egress < target_set:
                                    self.telescope.append(telescope.name)  # add telescope to viable list
                                    accept = True
                    except mini_staralt.NeverVisibleError:
                        pass
                    except mini_staralt.AlwaysVisibleError:
                        # target is always visible
                        accept = True
                        self.telescope.append(telescope.name)  # add telescope to viable list

            break

        return accept


class Telescope:
    """
    Telescope object, contains physical information about the location of the telescope
    """
    def __init__(self):
        """
        Null constructor, populated by gen_from_database
        """
        self.name = None
        self.lat = None
        self.lon = None
        self.alt = None
        self.aperture = None

    def __str__(self):
        return self.name+' Lat: '+str(self.lat)+' Lon: '+str(self.lon)

    def gen_from_database(self, row):
        """
        Populates Telescope object from data row
        :param row: array of values to be stored
        """
        self.name = row[0]
        self.lat = row[1]
        self.lon = row[2]
        self.alt = row[3]
        self.aperture = row[4]


def read_data(cursor, target):
    """
    Reads target data required to make transit forecast
    :param cursor: Cursor connected to database
    :param target: Name of target to be queried
    :return: data obtained form database
    """
    cursor.execute('SELECT LastObs, Duration, CurrentPeriod, RA, Dec, PercentLoss, LastEpoch, ErrAtAriel FROM '
                   'TARGET_DATA WHERE Name = \'' + target + '\'')
    data = cursor.fetchall()[0]
    return data


def transit_forecast(data, name, start, end):
    """
    Forecast transits visible from somewhere on Earth within the given window for a given target
    :param data: Data from read_data, to be stored in Transit2 object
    :param name: Name of target
    :param start: Start of window: datetime
    :param end: End of window: datetime
    :return: List of Transit2 objects for transits visible from somewhere on Earth
    """
    from datetime import timedelta
    import julian

    # extract data
    current_dt = start
    target_dt = end
    current_ephemeris = julian.from_jd(data[0]+2400000, fmt='jd')  # convert dt to JD
    duration = timedelta(minutes=data[1])
    period = timedelta(days=data[2])
    ra = data[3]
    dec = data[4]
    loss = data[5]
    epoch = data[6]
    error = data[7]

    transits = []
    # loop for all transits by the target in the window
    while current_ephemeris < target_dt:
        if current_ephemeris > current_dt:
            # create new Transit2 object
            candidate = Transit2(current_ephemeris, duration, ra, dec, period, loss, name, epoch, error)
            if candidate.check_visibility_general():  # check visibility
                transits.append(candidate)
        current_ephemeris += period  # increment date
        epoch += 1  # increment epochs

    return transits


class Transit2:
    """
    Transit2 object, need to merge with Transit eventually
    Functions within check the visibility of the Transit2 from somewhere on Earth
    """
    def __init__(self, center, duration, ra, dec, period, percentloss, name, epoch, error):
        """
        Constructor for Transit2 object, stores information inside
        :param center:
        :param duration:
        :param ra:
        :param dec:
        :param period:
        :param percentloss:
        :param name:
        :param epoch:
        :param error:
        """
        self.date = center.replace(hour=0, minute=0, second=0, microsecond=0)
        self.name = name
        self.center = center
        self.ingress = center - duration / 2
        self.egress = center + duration / 2
        self.duration = duration
        self.ra = ra
        self.dec = dec
        self.period = period
        self.loss = percentloss
        self.epoch = epoch
        self.error = error

    def check_visibility_general(self):
        """
        Checks visibility of a Transit2 object from somewhere on Earth
        :return:
        """
        import mini_staralt
        import datetime

        # latitude and longitude combinations that cover entire globe
        latitudes = [45, 0, -45]
        longitudes = [0, 60, 120, 150, 180, 240, 300]
        accept = False

        while not accept:
            # loop through all combinations of lat and lon
            for lat in latitudes:
                for lon in longitudes:
                    # check for night at coordinate
                    sun_down = False
                    sunset, sunrise = mini_staralt.sun_set_rise(
                        self.center.replace(hour=0, second=0, minute=0, microsecond=0), lon=lon, lat=lat, sundown=-12)
                    # check sunrise/set against in/egress
                    if self.ingress > sunset:
                        if self.egress < sunrise:
                            sun_down = True
                    # if calculation made for wrong day, step back and recalculate
                    elif sunset > self.center:
                        sunset, sunrise = mini_staralt.sun_set_rise(
                            self.center.replace(hour=0, second=0, minute=0, microsecond=0) - datetime.timedelta(
                                days=1),
                            lon=lon, lat=lat, sundown=-12)
                        # check sunrise/set against in/egress
                        if self.ingress > sunset:
                            if self.egress < sunrise:
                                sun_down = True

                    if sun_down:  # continue only if night at location
                        try:
                            target_rise, target_set = mini_staralt.target_rise_set(
                                self.center.replace(hour=0, minute=0, second=0, microsecond=0),
                                ra=self.ra, dec=self.dec, mintargetalt=20, lon=lon, lat=lat)
                            # check target rise/set against in/egress
                            if self.ingress > target_rise:
                                if self.egress < target_set:
                                    accept = True
                            # if calculation made for wrong day, step back and recalculate
                            elif target_rise > self.center:
                                target_rise, target_set = mini_staralt.target_rise_set(
                                    self.center.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1),
                                    ra=self.ra, dec=self.dec, mintargetalt=20, lon=lon, lat=lat)
                                # check target rise/set against in/egress
                                if self.ingress > target_rise:
                                    if self.egress < target_set:
                                        accept = True

                        except mini_staralt.NeverVisibleError:
                            pass
                        except mini_staralt.AlwaysVisibleError:
                            # target is always visible
                            accept = True

            break
        return accept


def find_highest_id(ids_raw):
    """
    Find highest observation ID from list of current IDs and returns the next ID to use
    :param ids_raw: List of current IDs
    :return: New ID to be used
    """
    # extract IDs from tuples
    ids = []
    for id in ids_raw:
        ids.append(id[0])
    try:
        # check for null value
        highest_id = max(ids)
        # out observations start at 10000, if highest ID is < 10000, start with 10000
        if highest_id < 10000:
            new_id = 10000
        # increment from highest ID
        else:
            new_id = highest_id + 1

    except ValueError:
        new_id = 10000
    return new_id

