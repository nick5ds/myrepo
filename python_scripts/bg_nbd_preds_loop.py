from bg_nbd_model_with_projections_and_elasticity import *
from datetime import datetime, timedelta

get_creds()




start = datetime.strptime("2016-11-01", "%Y-%m-%d")
end = datetime.strptime("2016-11-20", "%Y-%m-%d")
date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]
for date in date_generated:
    print date.strftime("%Y-%m-%d")
