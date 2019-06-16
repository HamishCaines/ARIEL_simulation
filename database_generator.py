import sqlite3


def load_planet_data(filename):
    targets = []
    try:
        file = open(filename, 'r')
        line = file.readline().split('\n')[0].split(',')
        columns = line
        while True:
            line = file.readline().split('\n')[0].split(',')
            if line == ['']:
                break
            targets.append(line)
        return columns, targets

    except FileNotFoundError:
        raise FileNotFoundError


def obtain_types(values):
    types = []
    for value in values:
        if value != '':
            try:
                float(value)
                types.append('DECIMAL(16,8)')
            except ValueError:
                types.append('VARCHAR(20)')
        else:
            types.append('DECIMAL(16,8)')
    return types


def obtain_columns(headers):
    columns = []
    for header in headers:
        columns.append(header)
    return columns


def table_string_builder(columns, types, table_name):
    string = 'CREATE TABLE IF NOT EXISTS ' + table_name + '('
    for i in range(0, len(columns)):
        string += columns[i] + ' ' + types[i] + ', '
    string = string[:-2]
    string += ', UNIQUE (Name))'
    return string


def data_string_builder(columns, target, table_name):
    string = 'INSERT INTO ' + table_name + ' ('
    for column in columns:
        string += column + ', '
    string = string[:-2] + ') VALUES ('
    for value in target:
        if value == '':
            string += 'NULL, '
        elif value[0].isdigit():
            if not value.isalnum():
                string += '\"' + value + '\", '
            else:
                string += '\'' + value + '\', '
        else:
            string += '\'' + value + '\', '
    string = string[:-2] + ')'
    return string


def generate_sql_table_from_csv(filename, table_name, cursor):
    columns, targets = load_planet_data(filename)
    types = obtain_types(targets[0])
    clean_columns = obtain_columns(columns)
    #if filename == '../database/starting_data.csv':
    #    targets = targets[:6]
    table_string = table_string_builder(clean_columns, types, table_name)
    print(table_string)
    # cursor.execute('DROP TABLE '+table_name)
    cursor.execute(table_string)

    for target in targets:
        data_string = data_string_builder(clean_columns, target, table_name)
        #print(data_string)
        try:
            cursor.execute(data_string)
        except sqlite3.IntegrityError:
            pass


def main():
    database_name = 'clean_lessideal.db'
    start_file = '../database/starting_data.csv'
    telescope_file = '../database/telescopes_ideal.csv'

    tel_cols, telescopes = load_planet_data(telescope_file)

    db = sqlite3.connect(database_name)
    cursor = db.cursor()
    generate_sql_table_from_csv(start_file, 'TARGET_DATA', cursor)
    db.commit()
    generate_sql_table_from_csv(telescope_file, 'TELESCOPES', cursor)
    db.commit()
    for telescope in telescopes:
        cursor.execute('DROP TABLE IF EXISTS ' + telescope[0])
        cursor.execute('CREATE TABLE IF NOT EXISTS ' + telescope[
            0] + '(Target VARCHAR(25), RA DECIMAL(16,8), Dec DECIMAL(16,8), ObsCenter DATETIME, RunStart DATETIME, RunEnd DATETIME, RunDuration TIME, Epoch REAL, UNIQUE(RunStart))')
        print(telescope[0])

    cursor.execute('SELECT Name FROM TARGET_DATA')
    names = cursor.fetchall()
    for name in names:
        cursor.execute('CREATE TABLE IF NOT EXISTS \'' + name[
            0] + '\'(ObID VARCHAR(25), Epoch REAL, ObsCenter DECIMAL(16,8), ObsCenterErr DECIMAL(16,8), TrueCenter DECIMAL(16,8), TrueCenterErr DECIMAL(16,8), ObsDepth DECIMAL(16,8), ObsDepthErr DECIMAL(16,8), ObsDuration DECIMAL(16,8), ObsDurationErr DECIMAL(16,8), Source VARCHAR(25), UNIQUE(ObsCenter))')
    db.commit()

    import database_tools
    from datetime import datetime, timedelta
    database = database_tools.Database(database_name)
    database.run_queries()

    start_date = datetime.today()
    database.transit_forecast(start_date, start_date+timedelta(days=28))
    #database.make_schedules(start_date)

    print('Database initialised')


if __name__ == '__main__':
    main()
