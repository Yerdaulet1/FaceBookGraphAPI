import requests, json, datetime, re, asyncio
from pymongo.errors import (DuplicateKeyError)
from pymongo import MongoClient
from dateutil import parser

cluster = MongoClient("mongodb+srv: ... ")
db = cluster['DatabaseName']
tag = re.compile(r"@(\w+)")
hashtag = re.compile(r"#(\w+)")


async def text_preprocess(text):
       
    text = text.replace("\n", "")
    tags = tag.findall(text)
    hashtags = hashtag.findall(text)
    return text, hashtags, tags


async def parse_data(competitor):
    
    params=dict()
    params['access_token'] = 'access_token'
    params['base'] = 'https://graph.facebook.com/'
    params['insta_acc_id'] = 'insta_acc_id'
    params['query'] = '{media{id,caption,comments_count,like_count,media_type,timestamp},followers_count,media_count}'
    params['parse_url'] = params['base'] + params['insta_acc_id'] + f"?fields=business_discovery.username({competitor}){params['query']}&access_token={params['access_token']}"
    responce = requests.get(url=params['parse_url']).json()
    is_end = await asyncio.create_task(save_responce(responce, competitor))
    return responce, params, is_end


async def pagination(responce, competitor, params):

    query = '{' + f"media.after({responce['business_discovery']['media']['paging']['cursors']['after']})" + '{id,caption,comments_count,like_count,media_type,timestamp},followers_count,media_count}'
    responce = requests.get(params['base'] + params['insta_acc_id'] + f"?fields=business_discovery.username({competitor}){query}&access_token={params['access_token']}").json()
    is_end = await asyncio.create_task(save_responce(responce, competitor))
    return responce, is_end


async def save_responce(responce, competitor):
    """
    if the _id exists in the database, handle the error, find one and update the document
    if the post creation time is more than 30 days, break the loop and return is_end as 1
    is_end means for the current user we have collected all the posts for today 
    We handle errors related to username changes, account closings, non-professional accounts, etc. in competitors list
    """
    
    final_day = datetime.datetime.today() - datetime.timedelta(days=30)
    try:
        posts = responce['business_discovery']['media']['data']
        for post in posts:
            is_end = 0
            if 0 > (parser.parse(post['timestamp']).replace(tzinfo=None) - final_day).days:
                is_end = 1
                break

            caption, hashtags, tags = await asyncio.create_task(text_preprocess(post['caption']))
            post['_id'] = post['id']
            del post['id']

            post['competitor'] = competitor
            post['followers_count'] = responce['business_discovery']['followers_count']
            post['media_count'] = responce['business_discovery']['media_count']
            post['caption'] = caption
            post['hashtags'] = hashtags
            post['tags'] = tags
            post['last_check'] = datetime.datetime.today()

            try:
                db.Media.insert_one(post)
            except DuplicateKeyError:
                doc = {
                    "caption": post['caption'],
                    "comments_count": post['comments_count'],
                    "like_count": post['like_count'],
                    "last_check": datetime.datetime.today(),
                    "followers_count": post['followers_count'],
                    "media_count": post['media_count']}
                db.Media.find_one_and_update({"_id": post["_id"]}, {"$set": doc})
        
    except KeyError:
        is_end = 1
   
    return is_end


