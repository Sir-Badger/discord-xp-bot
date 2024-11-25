import asyncio # asynchronous funcionality
import discord # basic discord functions
from discord.ext import commands # discord bot functions
import mysql.connector as mysql # mysql functions
import time # for time purposes
from apscheduler.schedulers.asyncio import AsyncIOScheduler # async scheduler
from apscheduler.triggers.cron import CronTrigger # timed trigger
import yaml, os, subprocess, traceback

file_dir = '/'.join(__file__.split('/')[:-1]) # get abs location of file
with open(file_dir+'/conf.yaml', 'r') as file:
    conf: dict = yaml.safe_load(file)

# main & startup function
print(" - building bot")
intents = discord.Intents.default()
intents.message_content = True
QuestBored = commands.Bot(command_prefix=conf['prefix'], intents=intents)

print(" - loading extensions")
for extension in conf["extensions"]:
    QuestBored.load_extension(extension+"_extension")

# event listeners
@QuestBored.event
async def on_ready():
    print( "===\n"\
          f"Logged in as {QuestBored.user}\n"
          f" - prefix '{QuestBored.command_prefix}'\n"\
          f" - ping {QuestBored.latency * 1000} ms\n"\
           "===")

debug = False

@QuestBored.listen()
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    emb = discord.Embed(color=0x800000)
    
    if type(error) == commands.CheckFailure:
        emb.description = f"You're missing permissions to use that command.\nYou need the following permissions: `{', '.join(ctx.command.extras['required_permissions'])}`"
    elif isinstance(error, commands.UserInputError):
        emb.description = "User input error"
    else:
        emb.title = type(error).__name__
        emb.description = f"{str(error)}" if not debug else f"```\n{'\n'.join(traceback.format_exception(error))}```"
    
    await ctx.send(embed=emb)   

# extension management    
@QuestBored.group(
    invoke_without_command=True
)
async def extensions(ctx: commands.Context):
    available = set([f.removesuffix("_extension.py") for f in os.listdir(file_dir) if f.endswith("_extension.py")])
    active = set([e.removesuffix("_extension") for e in QuestBored.extensions])
    unused = available.difference(active)

    if len(unused) == 0: unused = ('-')
    if len(active) == 0: active = ('-')

    await ctx.send(f"### Active extensions:\n{', '.join(active)}\n### Available extensions:\n{', '.join(unused)}")

@extensions.command(

)
async def reload(ctx: commands.Context, ext: str = None):
    if not ext or ext+"_extension" not in QuestBored.extensions:
        raise Exception(f"`{ext}` is not a currently loaded extension.")
    
    m = await ctx.send(f"Reloading `{ext}` ...")
    print(f"Reloading `{ext}` extension")
    try:
        QuestBored.reload_extension(ext+"_extension")
        await m.edit(f"Successfully reloaded `{ext}`")
    except Exception as e:
        await m.edit(f"Something went wrong when reloading `{ext}`\n```\n{e}```")

@extensions.command(

)
async def load(ctx: commands.Context, ext: str = None):
    if not ext or ext not in [f.removesuffix("_extension.py") for f in os.listdir(file_dir) if f.endswith("_extension.py")]:
        raise Exception(f"`{ext}` is not an available extension.")
    elif ext+"_extension" in QuestBored.extensions:
        raise Exception(f"`{ext}` is already loaded.") 
    
    m = await ctx.send(f"Loading `{ext}` ...")
    print(f"Loading `{ext}` extension")
    try:
        QuestBored.load_extension(ext+"_extension")
        await m.edit(f"Successfully loaded `{ext}`")
    except Exception as e:
        await m.edit(f"Something went wrong when loading `{ext}`\n```\n{e}```")

@extensions.command(

)
async def unload(ctx: commands.Context, ext: str = None):
    if not ext or ext+"_extension" not in QuestBored.extensions:
        raise Exception(f"`{ext}` is not an active extension.")
    
    m = await ctx.send(f"Unloading `{ext}` ...")
    print(f"Unloading `{ext}` extension")
    try:
        QuestBored.unload_extension(ext+"_extension")
        await m.edit(f"Successfully unloaded `{ext}`")
    except Exception as e:
        await m.edit(f"Something went wrong when unloading `{ext}`\n```\n{e}```")

@extensions.command(

)
async def update(ctx: commands.Context):
    m = await ctx.send("Updating code ...")
    try:
        print("Updating code via subprocess 'git pull'")
        out = subprocess.run(cwd = file_dir,
                             args = ["git","pull"],
                             capture_output = True,
                             check = True,
                             text = True)
        
        for l in out.stdout.split("\n"):
            print("> "+l)
        
        await m.edit("Code updated")
    except Exception as e:
        await m.edit(f"Something went wrong when running `git pull` to update the code\n```\n{e}```")
        return
    
    m = await ctx.send("Reloading extensions ...")
    print("Reloading extensions")
    for ext in [e for e in QuestBored.extensions]:
        try:
            QuestBored.reload_extension(ext)
        except Exception as e:
            await m.edit(f"Something went wrong when reloading `{ext}`\n```\n{e}```")
            return
    await m.edit("Successfully reloaded all extensions")

QuestBored.run(conf['token'])