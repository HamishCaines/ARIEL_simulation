import database_tools

database_name = 'ARIEL_DATABASE.db'

database = database_tools.Database(database_name)

database.run_queries()

database.transit_forecast()

database.make_schedules()

print('Database initialised')








