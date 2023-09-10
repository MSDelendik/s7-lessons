from datetime import datetime, timedelta

def input_paths (date: str, depth: int):
    path_list = []
    #path = f"/user/msdelendik/data/events/date={date}/event_type=message"
    for i in range (depth):
        dt = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=i)
        dt = dt.date()
        path = f"/user/msdelendik/data/events/date={dt}/event_type=message"
        path_list.append(path)
    return path_list

        