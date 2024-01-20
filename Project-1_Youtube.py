from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
from datetime import datetime
import dateutil.parser
from datetime import timezone
import streamlit as st

def connect_api():
    api_key='AIzaSyDtkTMSUtUqH2kZHFhEMToqVr7gL7WtuU0'
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube
youtube=connect_api()

#to get channel details
def channelDetails(channel_id):
    request = youtube.channels().list(                          
            part="snippet,contentDetails,statistics",
            id=channel_id)
    response = request.execute()
    for i in response['items']:
        data=dict(
            channel_name=i['snippet']['title'],
            channel_id=i['id'],
            channel_type=i['kind'],
            channel_view=i['statistics']['viewCount'],
            channel_des=i['snippet']['description'],
            channel_sub=i['statistics']['subscriberCount'],
            channel_video=i['statistics']['videoCount'])
    return data

#to get video ids
def videoidDetails(channel_id):
    video_ids=[]
    response = youtube.channels().list(id=channel_id,part="contentDetails").execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']    
    response1= youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=50).execute()
    for i in range(len(response1['items'])):
        video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    return video_ids

#to get video details
def videoDetails(video_ids):
    video_details=[]
    for video_id in video_ids:
        request1 = youtube.videos().list(part="snippet,contentDetails,statistics",id=video_id).execute()
        for i in request1['items']:
            data=dict(
                    video_id= i['id'],
                    channel_name=i['snippet']['channelTitle'],
                    video_name= i['snippet']['title'],
                    video_des= i['snippet']['description'],
                    video_viewcount= i['statistics']['viewCount'],
                    video_comment= i['statistics']['commentCount'],
                    video_likes= i['statistics']['likeCount'],
                    video_publisheddate= i['snippet']['publishedAt'],
                    video_duration= i['contentDetails']['duration'])
        video_details.append(data)
    return video_details

#to get comment details
def commentDetails(videoids):
    comment_details=[]
    try:
        for videoid in videoids:
            request2 = youtube.commentThreads().list(part="snippet,replies",videoId=videoid,maxResults=10).execute()
            for i in request2['items']:
                data=dict(
                    comment_id=i['id'],
                    video_id=i['snippet']['videoId'],
                    comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_date=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_details.append(data)
        return comment_details
    except:
        pass

import pymongo
connection=pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db=connection['ProjectOne_Youtube']
col=db['Youtube_Data']

def youtube_channel_data(id):
    channel_data=channelDetails(id)
    videoid_data=videoidDetails(id)
    video_data=videoDetails(videoid_data)
    comment_data=commentDetails(videoid_data)
    col=db['Youtube_Data']
    col.insert_one({"channel":channel_data,"video":video_data,"comment":comment_data})
    return "Data Uploaded in MongoDB"

import mysql.connector
connection=mysql.connector.connect(       
    host='localhost',
    user='root',
    password='12345678',
    database='ProjectOne_Youtube'
)
cursor=connection.cursor()

def ChannelTable():
    connection=mysql.connector.connect(       
    host='localhost',
    user='root',
    password='12345678',
    database='ProjectOne_Youtube')
    cursor=connection.cursor()
    drop_query='''drop table if exists Channels'''
    cursor.execute(drop_query)
    connection.commit()
    query="""create table Channels( channel_name varchar(255),
                                    channel_id varchar(255) primary key,
                                    channel_type varchar(255),
                                    channel_view bigint,
                                    channel_des text,
                                    channel_sub bigint,
                                    channel_video int
                                  )"""
    cursor.execute(query)
    channel_list=[]
    for data in col.find({},{"_id":0,"channel":1}):
        channel_list.append(data["channel"])
    df=pd.DataFrame(channel_list)
    for i,j in df.iterrows():
        query='''insert into Channels(channel_name,channel_id,channel_type,
                                      channel_view,channel_des,channel_sub,channel_video)
                                      values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(j['channel_name'],
                j['channel_id'],
                j['channel_type'],
                j['channel_view'],
                j['channel_des'],
                j['channel_sub'],
                j['channel_video'])
        cursor.execute(query,values)
        connection.commit()

def VideosTable():
    connection=mysql.connector.connect(       
    host='localhost',
    user='root',
    password='12345678',
    database='ProjectOne_Youtube')
    cursor=connection.cursor()
    drop_query='''drop table if exists Videos'''
    cursor.execute(drop_query)
    connection.commit()
    query="""create table Videos ( video_id varchar(255),
                                   channel_name varchar(255),
                                   video_name varchar(255),
                                   video_des text,
                                   video_viewcount bigint,
                                   video_comment int,
                                   video_likes bigint,
                                   video_publisheddate timestamp,
                                   video_duration TIME
                                )"""
    cursor.execute(query)
    video_list=[]
    for data in col.find({},{"_id":0,"video":1}):
        for i in range(len(data['video'])):
            video_list.append(data["video"][i])
    df=pd.DataFrame(video_list)
    for i,j in df.iterrows():
        query='''insert into Videos(video_id,channel_name,video_name,video_des,
                                      video_viewcount,video_comment,
                                      video_likes,video_publisheddate,video_duration)
                                      values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        d = datetime.fromisoformat(j['video_publisheddate']).astimezone(timezone.utc)
        j['video_publisheddate']=d.strftime('%Y-%m-%d %H:%M:%S')
        d1 = dateutil.parser.parse(j['video_duration'][2:])
        j['video_duration']=d1.strftime('%H:%M:%S')
        
        
        values=(j['video_id'],
                j['channel_name'],
                j['video_name'],
                j['video_des'],
                j['video_viewcount'],
                j['video_comment'],
                j['video_likes'],
                j['video_publisheddate'],
                j['video_duration'])
        cursor.execute(query,values)
        connection.commit()

def CommentTable():
    connection=mysql.connector.connect(       
    host='localhost',
    user='root',
    password='12345678',
    database='ProjectOne_Youtube')
    cursor=connection.cursor()
    drop_query='''drop table if exists Comments'''
    cursor.execute(drop_query)
    connection.commit()
    query="""create table Comments(
                                 comment_id varchar(255) ,
                                 video_id varchar(255),
                                 comment_text TEXT,
                                 comment_author varchar(255),
                                 comment_date DATETIME
                                  )"""
    cursor.execute(query)
    comment_list=[]
    for data in col.find({},{"_id":0,"comment":1}):
        for i in range(len(data['comment'])):
            comment_list.append(data["comment"][i])
    df=pd.DataFrame(comment_list)
    for i,j in df.iterrows():
        query='''insert into Comments(comment_id,video_id,comment_text,
                                      comment_author,comment_date)
                                      values(%s,%s,%s,%s,%s)'''
        
        d = datetime.fromisoformat(j['comment_date']).astimezone(timezone.utc)
        j['comment_date']=d.strftime('%Y-%m-%d %H:%M:%S')
        
        values=(j['comment_id'],
                j['video_id'],
                j['comment_text'],
                j['comment_author'],
                j['comment_date'])
        cursor.execute(query,values)
        connection.commit()   

def Table():
    ChannelTable()
    VideosTable()
    CommentTable()
    return "Tables Created in MYSQL"

def viewChannel():
    channel_list=[]
    for data in col.find({},{"_id":0,"channel":1}):
        channel_list.append(data["channel"])
    df1=st.dataframe(channel_list)
    return df1

def viewVideo():
    video_list=[]
    for data in col.find({},{"_id":0,"video":1}):
        for i in range(len(data['video'])):
            video_list.append(data["video"][i])
    df2=st.dataframe(video_list)
    return df2

def viewComment():
    comment_list=[]
    for data in col.find({},{"_id":0,"comment":1}):
        for i in range(len(data['comment'])):
            comment_list.append(data["comment"][i])
    df3=st.dataframe(comment_list)
    return df3

#streamlit code
with st.sidebar:
    st.title(":red[KEY TAKEAWAYS]")
    st.caption("Collect Data from Youtube API")
    st.caption("Migrate data to MONGODB")
    st.caption("Insert data to MYSQL")
    st.caption("Write Query to display details")
    
st.subheader(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")   
channel_id=st.text_input("Enter the channel id")
col1,col2=st.columns(2)
with col1:
    if st.button("Store data in MongoDB"):
        ch_id=[]
        #db=connection['ProjectOne_Youtube']
        #col=db['Youtube_Data']
        for data in col.find({},{"_id":0,"channel":1}):
            ch_id.append(data["channel"]['channel_id'])
        if channel_id in ch_id:
            st.success("Channel Details already exists")
        else:
            insert=youtube_channel_data(channel_id)
            st.success(insert)
with col2:
    if st.button("Migrate data to MYSQL"):
        table=Table()
        st.success(table)
st.text(" ")
st.text(" ")
st.text("Click the below buttons to display Channel Details/Video Details/Comments Details")
col1, col2, col3 = st.columns(3)
with col1:
   if st.button("Channel Details"):
    viewChannel()

with col2:
   if st.button("Video Details"):
    viewVideo()
    
with col3:
   if st.button("Comment Details"):
    viewComment()
    
st.text(" ")
st.text(" ")
#QUERY
connection=mysql.connector.connect(       
    host='localhost',
    user='root',
    password='12345678',
    database='ProjectOne_Youtube'
)
cursor=connection.cursor()
question=st.selectbox("Select one question to display",
                      ("1. What are the names of all the videos and their corresponding channels?",
                       "2. Which channels have the most number of videos, and how many videos do they have?",
                       "3. What are the top 10 most viewed videos and their respective channels?",
                       "4. How many comments were made on each video, and what are their corresponding video names?",
                       "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                       "6. What is the total number of likes for each video, and what are their corresponding video names?",
                       "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                       "8. What are the names of all the channels that have published videos in the year 2022?",
                       "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                       "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))
#10 QUERIES
connection=mysql.connector.connect(       
    host='localhost',
    user='root',
    password='12345678',
    database='ProjectOne_Youtube'
)
cursor=connection.cursor()
if (question=="1. What are the names of all the videos and their corresponding channels?"):
    query1="select video_name,channel_name from Videos"
    cursor.execute(query1)
    data1=cursor.fetchall()
    df1=pd.DataFrame(data1,columns=["Videos Name","Channel Name"])
    st.write(df1)
elif (question=="2. Which channels have the most number of videos, and how many videos do they have?"):
    query2="select channel_name,channel_video from Channels order by channel_video desc"
    cursor.execute(query2)
    data2=cursor.fetchall()
    df2=pd.DataFrame(data2,columns=["Channel Name","Videos Count"])
    st.write(df2)
elif (question== "3. What are the top 10 most viewed videos and their respective channels?"):
    query3="select video_viewcount,video_name, channel_name from Videos where video_viewcount is not null order by video_viewcount desc limit 10"
    cursor.execute(query3)
    data3=cursor.fetchall()
    df3=pd.DataFrame(data3,columns=["Video View Count","Video Name","Channel Name"])
    st.write(df3)
elif (question== "4. How many comments were made on each video, and what are their corresponding video names?"):
    query4="select video_comment,video_name from Videos where video_comment is not null"
    cursor.execute(query4)
    data4=cursor.fetchall()
    df4=pd.DataFrame(data4,columns=["Video Comment Count","Video Name"])
    st.write(df4)
elif (question== "5. Which videos have the highest number of likes, and what are their corresponding channel names?"):
    query5="select video_likes,video_name,channel_name from Videos where video_likes is not null order by video_likes desc"
    cursor.execute(query5)
    data5=cursor.fetchall()
    df5=pd.DataFrame(data5,columns=["Video Likes Count","Video Name","Channel Name"])
    st.write(df5)
elif (question== "6. What is the total number of likes for each video, and what are their corresponding video names?"):
    query6="select video_likes,video_name from Videos"
    cursor.execute(query6)
    data6=cursor.fetchall()
    df6=pd.DataFrame(data6,columns=["Video Likes Count","Video Name"])
    st.write(df6)
elif (question=="7. What is the total number of views for each channel, and what are their corresponding channel names?"):
    query7="select channel_name,channel_view from Channels"
    cursor.execute(query7)
    data7=cursor.fetchall()
    df7=pd.DataFrame(data7,columns=["Channel names","Channel Views"])
    st.write(df7)
elif (question== "8. What are the names of all the channels that have published videos in the year 2022?"):
    query8="select video_name,video_publisheddate,channel_name from Videos where extract(year from video_publisheddate)=2022"
    cursor.execute(query8)
    data8=cursor.fetchall()
    df8=pd.DataFrame(data8,columns=["Video Names","Published in 2022","Channel Names"])
    st.write(df8)
elif (question== "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?"):
    query9="select AVG(video_duration),channel_name from Videos group by channel_name"
    cursor.execute(query9)
    data9=cursor.fetchall()
    df9=pd.DataFrame(data9,columns=["AverageDuration","ChannelNames"])
    T9=[]
    for i,j in df9.iterrows():
        ch_title=j['ChannelNames']
        avg_duration=j['AverageDuration']
        avg=str(avg_duration)
        T9.append(dict(channel_name=ch_title,avg_duration=avg))
    df=pd.DataFrame(T9)
    st.write(df)
elif (question== "10. Which videos have the highest number of comments, and what are their corresponding channel names?"):
    query10="select video_name,video_comment,channel_name from Videos where video_comment is not null order by video_comment desc"
    cursor.execute(query10)
    data10=cursor.fetchall()
    df10=pd.DataFrame(data10,columns=["Video Names","Video Comment","Channel Names"])
    st.write(df10)
