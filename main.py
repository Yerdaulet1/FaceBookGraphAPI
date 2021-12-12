import aiocron, asyncio, time
from index import parse_data, pagination


@aiocron.crontab('30 6 */1 * *') # decorator runs the main function everyday at 6:30 AM
async def main():
    
    """
    We except the KeyError to handle the request error limit from the Facebook Graph API
    If a limit error occurs, we will wait 1 hour and for a time equal to the time when the requests were made
    While loop we use to early stop the pagination request
    We also collect user posts if the post was published in the last 30 days
    """
    
    Time = 0
    competitors = ["usernames"]

    for competitor in competitors:
        try:
            start_time = time.time()
            responce, params, is_end = await asyncio.create_task(parse_data(competitor))
            Time += (time.time() - start_time)
        except KeyError as e:
            time.sleep(60*60*1 + Time)
            responce, params, is_end = await asyncio.create_task(parse_data(competitor))
            Time=0
            
        while is_end < 1:

            try:
                start_time = time.time()
                responce, is_end = await asyncio.create_task(pagination(responce, competitor, params))
                Time += (time.time() - start_time)
            except KeyError as e:
                time.sleep(60*60*1 + Time)
                responce, is_end = await asyncio.create_task(pagination(responce, competitor, params))
                Time=0

asyncio.get_event_loop().run_forever()
