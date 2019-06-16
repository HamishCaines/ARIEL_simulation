class Transit:
    def __init__(self):
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
        import mini_staralt
        from datetime import timedelta
        accept = False

        while not accept:
            for telescope in telescopes:
                sun_down = False
                sunset, sunrise = mini_staralt.sun_set_rise(
                    self.center.replace(hour=0, second=0, minute=0, microsecond=0), lon=telescope.lon, lat=telescope.lat, sundown=-12)
                # print(sunset, sunrise, self.center, lon, lat)

                if self.ingress > sunset:
                    if self.egress < sunrise:
                        sun_down = True
                elif sunset > self.center:
                    sunset, sunrise = mini_staralt.sun_set_rise(
                        self.center.replace(hour=0, second=0, minute=0, microsecond=0) - timedelta(days=1),
                        lon=telescope.lon, lat=telescope.lat, sundown=-12)

                    if self.ingress > sunset:

                        if self.egress < sunrise:
                            sun_down = True

                if sun_down:

                        # print(sunset, target_rise, target_set, sunrise)
                    try:
                        target_rise, target_set = mini_staralt.target_rise_set(
                            self.center.replace(hour=0, minute=0, second=0, microsecond=0),
                            ra=self.ra, dec=self.dec, mintargetalt=20, lon=telescope.lon, lat=telescope.lat)
                        if self.name == 'XO-6b':
                            print('YES1')

                        if self.ingress > target_rise:
                            if self.egress < target_set:
                                #print(target_rise, self.ingress, self.egress, target_set, self.name, self.ra, self.dec)
                                self.telescope.append(telescope.name)

                                accept = True

                                #if self.telescope is not None:
                                #    self.telescope = [self.telescope, telescope.name]
                                #elif self.telescope is None:
                                #    self.telescope = telescope.name
                            #print(telescope.name, target_rise, self.ingress, self.egress, target_set, accept, self.name)
                        elif target_rise > self.center:
                            target_rise, target_set = mini_staralt.target_rise_set(
                                self.center.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1),
                                ra=self.ra, dec=self.dec, mintargetalt=20, lon=telescope.lon, lat=telescope.lat)

                            if self.ingress > target_rise:
                                if self.egress < target_set:
                                    self.telescope.append(telescope.name)
                                    if self.name == 'XO-6b':
                                        print('YES2')
                                    accept = True
                            #print(telescope.name, target_rise, self.ingress, self.egress, target_set, accept, self.name)
                    except mini_staralt.NeverVisibleError:
                        # print('no')
                        pass
                    except mini_staralt.AlwaysVisibleError:
                        # print('yes')
                        accept = True
                        self.telescope.append(telescope.name)

            break

        return accept


class Telescope:
    def __init__(self):
        self.name = None
        self.lat = None
        self.lon = None
        self.alt = None
        self.aperture = None

    def __str__(self):
        return self.name+' Lat: '+str(self.lat)+' Lon: '+str(self.lon)

    def gen_from_database(self, row):
        self.name = row[0]
        self.lat = row[1]
        self.lon = row[2]
        self.alt = row[3]
        self.aperture = row[4]

