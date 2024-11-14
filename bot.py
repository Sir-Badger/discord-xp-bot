import asyncio # asynchronous funcionality
import discord # basic discord functions
from discord.ext import commands # discord bot functions
import mysql.connector as mysql # mysql functions
import time # for time purposes
from apscheduler.schedulers.asyncio import AsyncIOScheduler # async scheduler
from apscheduler.triggers.cron import CronTrigger # timed trigger
import yaml
#import rp_word_counter as rp # word counting

with open('/'.join(__file__.split('/')[:-1])+'/conf.yaml', 'r') as file:
    conf = yaml.safe_load(file)

# discord related functions
async def notify(member_list=[], msg="You have been notified!"): # notify role/member of something
    mention_text="> "
    for m in member_list: # loop through members to @
        mention_text+=m.mention+" "

    if mention_text != "> ":
        pass
        #await notify_channel.send(f"{mention_text}\n{msg}") # mention
    else:
        pass
        #await notify_channel.send(f"{mention_text}{msg}") # mention

# main & startup function
print(" - building bot")
intents = discord.Intents.default()
intents.message_content = True
QuestBored = commands.Bot(command_prefix=conf['prefix'], intents=intents)

print(" - loading extensions")
QuestBored.load_extension("xp_system_extension")

@QuestBored.event
async def on_ready():
    print( "===\n"\
          f"Logged in as {QuestBored.user}\n"
          f" - prefix '{QuestBored.command_prefix}'\n"\
          f" - ping {QuestBored.latency * 1000} ms\n"\
           "===")

QuestBored.run(conf['token'])