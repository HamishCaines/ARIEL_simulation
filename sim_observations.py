import database_tools
import datetime

database_name = 'ARIEL_DATABASE.db'

database = database_tools.Database(database_name)

day = datetime.timedelta(days=1)
start = datetime.datetime.today() + day
end = start + day


database.simulate_observations(start, end)

