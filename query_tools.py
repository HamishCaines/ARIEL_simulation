def obtain_targets(cursor):
    cursor.execute('SELECT Name FROM TARGET_DATA')
    rows = cursor.fetchall()
    targets = []
    for row in rows:
        targets.append(row[0])
    return targets


class Target:
    def __init__(self, name):
        self.name = name
        self.star = name[:-1]
        self.planet = name[-1]
        self.ETD_obs = []
        #self.query_period = None
        #self.query_period_err = None
        #self.query_epoch = None
        self.query_depth = None
        self.query_depth_err = None
        #self.query_duration = None
        #self.query_duration_err = None
        self.query_lastob = None
        self.query_lastob_err = None
        self.query_last_epoch = None

        self.depth_source = ''
        #self.duration_source = None

        #self.EXO_all_data = {}
        self.EXO_used = False

    def __str__(self):
        return self.star+self.planet

    def ETD_query(self):
        import re
        import requests
        import numpy as np
        url_base = 'http://var2.astro.cz/ETD/etd.php?'
        url = url_base + 'STARNAME=' + self.star + '&PLANET=' + self.planet

        web_html = str(requests.get(url).content)

        obs_data = re.findall(
            "<tr valign=\\\\\\'top\\\\\\'><td>(\d+)<\/td><td class=\\\\\\'right\\\\\\'><b>([\d\.]+)<br\/><\/b>"
            "([\d\s\.\+\/-]+)<\/td><td>(\d+)<\/td><td>([+|\-\d\.\s]+)<\/td><td>([\d\.\s]+)\+\/-([\d\.\s]+)<\/td><td>"
            "([\d\.]+) ([\d\s\.\+\/-]+)<\/td><td>([\w]+)<\/td><td><b><a href=\\\\\\'etd-data\.php\?id=[\d]+\\\\\\' "
            "target=\\\\\\'[\w]+\\\\\\' title=\\\\\\'get data\\\\\\'>(\d)",
            web_html)

        epoch_data = re.findall("size=\\\\\\'7\\\\\\\' value=\\\\\\'([\d\.]+)\\\\\\'>[\S\s]+size=\\\\\\'12\\\\\\' value"
                                "=\\\\\\'([\d\.]+)", web_html)

        #try:
        #    self.query_period = epoch_data[0][0]
        #except IndexError:
        #    self.query_period = None
        #try:
        #    self.query_epoch = epoch_data[0][1]
        #except IndexError:
        #    self.query_epoch = None

        depth_count = 0
        depth_total = 0
        depth_err_total = 0
        #duration_count = 0
        #duration_total = 0
        #duration_err_total = 0
        max_tmid = 0
        max_tmid_err = 0
        max_epoch = 0
        for obs in obs_data:
            new_ob = Observation()
            new_ob.gen_from_ETD(obs)

            if new_ob.quality >= 3:
                self.ETD_obs.append(new_ob)

                if new_ob.depth_err is not None:
                    depth_total += new_ob.depth
                    depth_err_total += new_ob.depth_err * new_ob.depth_err
                    depth_count += 1
                #if new_ob.duration_err is not None:
                    #duration_total += new_ob.duration
                    #duration_err_total += new_ob.duration_err * new_ob.duration_err
                    #duration_count += 1

                if new_ob.tmid > max_tmid:
                    max_tmid = new_ob.tmid
                    max_tmid_err = new_ob.tmid_err
                    max_epoch = new_ob.epoch
        if max_tmid != 0:
            self.query_lastob = max_tmid
            self.query_lastob_err = max_tmid_err
            self.query_last_epoch = max_epoch

        try:
            self.query_depth = depth_total / depth_count
            self.query_depth_err = np.sqrt(depth_err_total) / depth_count
            self.depth_source = 'ETD'
            # print('test ' + str(self.ETD_depth) + ' ' + str(self.ETD_depth_err))
        except ZeroDivisionError:
            pass

        #try:
        #    self.query_duration = duration_total / duration_count
        #    self.query_duration_err = np.sqrt(duration_err_total) / duration_count
            # print('dur test '+str(self.ETD_duration)+' '+str(self.ETD_duration_err))
        #except ZeroDivisionError:
        #    pass

    def EXO_query(self):
        import requests
        import re
        url_base = 'http://exoplanets.org/detail/'
        if self.star[0].isdigit():
            number = True
            count = 0
            while number:
                count += 1
                number = self.star[count].isdigit()
            url = url_base + self.star[:count] + '_' + self.star[count:] + '_' + self.planet
        elif self.star[:4] == 'EPIC':
            url = url_base + self.star[:4] + '_' + self.star[4:] + '_' + self.planet
        elif 'GJ' in self.star:
            url = url_base + 'GJ' + '_' + self.star[2:] + '_' + self.planet
        elif 'HD' in self.star:
            url = url_base + 'HD' + '_' + self.star[2:] + '_' + self.planet
        elif 'LHS' in self.star:
            url = url_base + 'LHS' + '_' + self.star[3:] + '_' + self.planet
        elif 'NGTS' in self.star:
            url = url_base + 'NGTS' + '-' + self.star[4:] + '_' + self.planet
        elif 'PH' in self.star:
            url = url_base + 'PH-' + self.star[2:] + '_' + self.planet
        elif 'KPS' in self.star:
            url = url_base + 'KPS' + '-' + self.star[3:] + '_' + self.planet
        elif 'HIP' in self.star:
            url = url_base + 'HIP' + '_' + self.star[3:] + '_' + self.planet
        else:
            url = url_base + self.star + '_' + self.planet

        web_html = str(requests.get(url).content)
        invalid_check = re.findall("(Invalid Planet)", web_html)
        if invalid_check:
            print(url + ' is invalid')
        if not invalid_check:
            data = dict(re.findall("\"([\w]+)\":\[([\d\w\.\"\s\/\:\;\%]+)\]", web_html))

            try:
                if self.query_depth is None:
                    if data['DEPTH'] != 'null':
                        self.query_depth = float(data['DEPTH'])*1000  # convert from unitless to mmag
                        self.depth_source = 'EXO'
                        if data['DEPTHUPPER'] != 'null':
                            self.query_depth_err = float(data['DEPTHUPPER'])*1000  # convert from unitless to mmag

                if self.query_last_epoch is None:
                    if data['TT'] != 'null':
                        self.query_lastob = float(data['TT']) - 2400000
                        self.query_lastob_err = data['TTUPPER']
                        self.query_last_epoch = 0
            except KeyError:
                pass

            # Code below repeats for duration, not necessary as duration is in source data
            #try:
            #    if self.query_duration is None:
            #        if data['T14'] != 'null':
            #            self.query_duration = float(data['T14'])*24*60  # convert from days to minutes
            #            if data['T14UPPER'] != 'null':
            #                self.query_duration_err = float(data['T14UPPER'])*24*60
            #except KeyError:
            #    pass

                #print(self.name, self.query_depth, self.query_depth_err)

    def write_query_data(self, cursor, db):
        import sqlite3
        if self.query_depth is not None:
            cursor.execute('UPDATE TARGET_DATA SET Depth = '+str(self.query_depth)+' WHERE Name = \''+self.name+'\'')
            cursor.execute('UPDATE TARGET_DATA SET DepthSource = \''+self.depth_source+'\' WHERE Name = \''+self.name+'\'')
            if self.query_depth_err is not None:
                cursor.execute('UPDATE TARGET_DATA SET DepthErr = '+str(self.query_depth_err) + \
                         ' WHERE Name = \''+self.name+'\'')
        if self.query_last_epoch is not None:
            cursor.execute('UPDATE TARGET_DATA SET LastObs = '+str(self.query_lastob)+' WHERE Name = \''+self.name+'\'')
            cursor.execute(
                'UPDATE TARGET_DATA SET LastObsErr = ' + str(self.query_lastob_err) + ' WHERE Name = \'' + self.name + '\'')
            cursor.execute(
                'UPDATE TARGET_DATA SET LastEpoch = ' + str(self.query_last_epoch) + ' WHERE Name = \'' + self.name + '\'')

        if len(self.ETD_obs) != 0:
            cursor.execute('UPDATE TARGET_DATA SET NoOfObs = '+str(len(self.ETD_obs))+' WHERE Name = \''+self.name+'\'')
            for ob in self.ETD_obs:
                #try:
                #    cursor.execute('ALTER TABLE \''+self.name+'\' ADD Source VARCHAR(10)')
                #except sqlite3.OperationalError:
                #    pass
                try:
                    cursor.execute('INSERT INTO \''+self.name+'\' VALUES ('+ob.obnumber+', '+str(ob.epoch)+', '+str(ob.tmid)+\
                    ', '+str(ob.tmid_err)+', NULL, NULL, '+str(ob.depth)+', '+str(ob.depth_err)+', '+str(ob.duration)+', '+\
                    str(ob.duration_err)+', \'ETD\')')
                except sqlite3.IntegrityError:
                    print('Obs No. '+str(ob.obnumber)+' already exists')

        db.commit()


class Observation:
    def __init__(self):
        self.obnumber = 0
        self.epoch = 0
        self.tmid = 0
        self.tmid_err = 0
        self.depth = 0
        self.depth_err = 0
        self.duration = 0
        self.duration_err = 0
        self.quality = 0
        self.OminusC = 0

    def gen_from_ETD(self, match):
        self.obnumber = match[0]
        self.tmid = float(match[1])  # HJD
        try:
            self.tmid_err = float(match[2].split()[1])  # HJD
        except ValueError:
            self.tmid_err = 0
        except IndexError:
            self.tmid_err = 0
        self.epoch = int(match[3])
        self.OminusC = float(match[4])  # days
        try:
            self.duration = float(match[5])  # mins
        except ValueError:
            pass
        try:
            self.duration_err = float(match[6].split()[0])  # mins
        except IndexError:
            pass
        self.depth = float(match[7])  # mmag
        try:
            self.depth_err = float(match[8].split()[1])  # mmag
        except IndexError:
            pass
        try:
            self.quality = int(match[10])
        except IndexError:
            pass

