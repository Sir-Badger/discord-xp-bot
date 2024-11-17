import discord, asyncio, yaml, traceback, validators.url
from discord.ext import commands
import aiomysql as mysql
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from rp_word_counter import count
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

# extension functions
def setup(bot):
    # load conf
    with open('/'.join(__file__.split('/')[:-1])+'/conf.yaml', 'r') as file:
        config = yaml.safe_load(file)["xp_system"]

    bot.add_cog(xp_system_db_connection(bot, config))
    db = bot.get_cog("xp_system_db_connection")
    loop = asyncio.get_event_loop()
    loop.create_task(db._build_pool()) # connect to db
    loop.create_task(db._start_scheduler()) # start scheduler

    bot.add_cog(xp_system(bot, config))

def clean_url(url):
    if url[0] == "<" and url[-1] == ">": url = url[1:-1]
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))  # Parse and reformat query
    cleaned_query = urlencode(query)  # Re-encode query string
    return urlunparse(parsed._replace(query=cleaned_query))

# Util classes
class player_character():
    """A data class for handling character data stored in the database"""
    id: int
    name: str
    color: hex
    image_url: str
    total_xp: int
    roleplay_xp: int
    level: int
    level_notification: bool
    words_cached: int
    owner_id: int
    pool_id: int
    active_on_account: int

    def __init__(self, attributes:tuple):
        self.id, self.name, self.color, self.image_url, self.total_xp, \
        self.roleplay_xp, self.level, self.level_notification, \
        self.words_cached, self.owner_id, self.pool_id, self.active_on_account = attributes

        if type(self.color) == str: self.color = int(self.color, 16)

class notifyUserException(Exception):
    """Raised to notify a member in discord when something goes wrong (like them inputting a wrong argument)"""
    pass

# cogs
class xp_system_db_connection(commands.Cog):
    """A cog that handles interacting with a MySQL database asynchronously with a pool of connections."""

    def __init__(self, bot:discord.bot, configuration):
        self.bot = bot

        # connection handling
        self.stable_pool = asyncio.Event()
        self.crash_time: datetime = None
        self.waiting_executions = 0
        self.stagger_time = 0.25
        self.max_retries = 3

        # periodic reset
        if "periodic_cap" in configuration:
            self.reset_cron = configuration["periodic_cap"]["cron"]
            self.reset_tz = configuration["periodic_cap"]["timezone"]

        # db info
        self.credentials: dict = configuration["database"]["credentials"]
        self.max_characters_per_pool: int = configuration["max_characters_per_pool"]
        self.char_table: str = configuration["database"]["char_table"]
        self.acc_table: str  = configuration["database"]["acc_table"]
        self.min_level: int  = list(configuration["level_req"].keys())[0]
        self.max_level: int  = list(configuration["level_req"].keys())[-1]

    def cog_unload(self): # make sure to close the connection
        loop = asyncio.get_event_loop()
        loop.create_task(self._close_pool())
        
        self.scheduler.shutdown(wait=False)

        return super().cog_unload()
    
    # connection & err handling
    async def _build_pool(self):
        print(" - building connection pool")
        try:
            self._connection_pool = await mysql.create_pool(minsize=1, maxsize=5, **self.credentials)
            self.stable_pool.set()
        except mysql.InterfaceError as e:
            print(f"Something went wrong when trying to establish a connection to the db:\n{e}")
            return

    async def _rebuild_pool(self):
        print("rebuilding pool")
        try:
            await self._close_pool()
            await self._build_pool(self.credentials)
            print(f"rebuilt pool at {datetime.now(timezone.utc)} with {self.waiting_executions} waiting executions")
            self.stable_pool.set()

        except Exception as e:
            print(f"Something went wrong when trying to rebuild:\n{e}")
            pass

    async def _close_pool(self):
        print(" - closing connection pool")
        self._connection_pool.close()
        await self._connection_pool.wait_closed()

    # periodic reset
    async def _start_scheduler(self):
        print(" - starting periodic reset scheduler")
        self.scheduler = AsyncIOScheduler()
        self.scheduler.start()

        self.scheduler.add_job(
            self.periodic_reset,
            CronTrigger.from_crontab(
                self.reset_cron, self.reset_tz
            )
        )

    async def periodic_reset(self):
        print("Reseting periodic cap")
        await self.commit(f"UPDATE {self.char_table} SET roleplay_xp = 0;")


    # general db operations
    def _database_interaction(func):
        """a decorator that handles faulty connections and possible errors"""

        async def interaction(self, *args, **kwargs):
            if not self.stable_pool.is_set():
                wait_time_to_prevent_overload = self.waiting_executions * self.stagger_time
                self.waiting_executions += 1

                await self.stable_pool.wait() # wait for connection flag

                # stagger the requests a little in case execution requests piled up
                await asyncio.sleep(wait_time_to_prevent_overload)
                self.waiting_executions -= 1
            
            retries = 0
            while retries < self.max_retries:
                try:
                    return await func(self, *args, **kwargs)
                except mysql.OperationalError as e:
                    retries += 1
                    if retries >= self.max_retries: # retries fail
                        if not self.stable_pool.is_set(): # rebuild sequence not yet initiated
                            self.stable_pool.clear()
                            self.crash_time = datetime.now(timezone.utc)
                            print(f"Pool crashed at {self.crash_time} due to the following exception:\n\"{e}\"")
                            await self._rebuild_pool()
                        else: # if it is, wait until rebuilt
                            self.stable_pool.wait()

                        try: # try again
                            return await func(self, *args, **kwargs)
                        except Exception as er:
                            print(f"An exception occured after rebuilding pool:")
                            raise er
                    
                    await asyncio.sleep(2)
                except Exception as e:
                    raise e

        return interaction

    @_database_interaction
    async def fetch(self, statement:str) -> tuple|list[tuple]:
        """A function for executing a single SELECT statement and fetching the result.
        If the result is a single row it will return a tuple of that row.
        If the result is multiple rows, it will return a list containing each result."""
    
        async with self._connection_pool.acquire() as conn:
            cursor = await conn.cursor()
            await cursor.execute(statement)
            result = await cursor.fetchall()
            await cursor.close()

        return result[0] if len(result) == 1 else list(result)
       
    @_database_interaction
    async def commit(self, statement:str):
        """A function for executing one or more statements and commiting the resulting changes.
        For multiple statements, they must be seperated with ';' as per SQL syntax"""

        async with self._connection_pool.acquire() as conn:
            cursor = await conn.cursor()
            for s in statement.split(";"):
                if s: await cursor.execute(s)
            await cursor.close()
            await conn.commit()

    # specific operations
    async def merge_pools(self, pool_a:int, pool_b:int):
        # merge a into b
        if pool_a == pool_b:
            raise notifyUserException("Cannot merge the same pool.")
        
        c = await self.fetch(f"SELECT character_name FROM {self.char_table} WHERE pool_id = {pool_a} OR pool_id = {pool_b}")
        if type(c) == tuple: c = [c]
        if len(c) > self.max_characters_per_pool:
            raise notifyUserException("Merging these two pools would exceed the character limit")
        if len(c) != len(set(c)):
            raise notifyUserException("There are multiple characters with the same name in the pools you want to merge")

        await self.commit(f"UPDATE {self.acc_table} SET pool_id = {pool_b} WHERE pool_id = {pool_a};"\
                          f"UPDATE {self.char_table} SET pool_id = {pool_b} WHERE pool_id = {pool_a};")

    async def separate_pools(self, acc_a:int, acc_b:int):
        # sep a from b
        shared_pool = await self.get_pool_by_account(acc_a)

        if acc_a == acc_b or shared_pool != await self.get_pool_by_account(acc_b):
            raise notifyUserException("You need to specify two different accounts that share a pool.")

        if shared_pool == acc_a: # the pool belongs to a
            # shift all accounts in the pool other than a into pool_b
            # update all characters in shared pool accordingly
            # separate all characters owned by acc_a into pool_a
            statement = f"UPDATE {self.acc_table} SET pool_id = {acc_b} WHERE pool_id = {shared_pool} AND account_id != {acc_a};"\
                        f"UPDATE {self.char_table} SET pool_id = {acc_b} WHERE pool_id = {shared_pool};"\
                        f"UPDATE {self.char_table} SET pool_id = {acc_a} WHERE owner_id = {acc_a};"
        else: # the pool belongs to b, or some other account
            # move acc_a out of shared pool
            # move all characters owned by acc_a into that pool
            statement = f"UPDATE {self.acc_table} SET pool_id = {acc_a} WHERE account_id = {acc_a};"\
                        f"UPDATE {self.char_table} SET pool_id = {acc_a} WHERE owner_id = {acc_a};"

        await self.commit(statement)

    async def add_pool_for_account(self, account_id: int):
        await self.commit(f"INSERT INTO {self.acc_table} (account_id, pool_id) VALUES ({account_id}, {account_id})")

    async def get_pool_by_account(self, account_id: int) -> int:
        pool = await self.fetch(f"SELECT pool_id FROM {self.acc_table} WHERE account_id = {account_id}")
        if pool:
            return pool[0]
        else:
            await self.add_pool_for_account(account_id)
            return account_id

    async def get_available_characters(self, account_id: int) -> list[player_character]:
        pool_id = await self.get_pool_by_account(account_id)
        characters = await self.fetch(f"SELECT * FROM {self.char_table} WHERE pool_id = {pool_id}")

        if type(characters) == tuple: # if there's only one, wrap it in a list
            characters = [characters]

        return [player_character(c) for c in characters]
    
    async def get_active_character(self, account_id:int) -> player_character:
        characters = await self.get_available_characters(account_id)
        active_char = None
        
        if len(characters) == 0:
            return await self.add_character_to_db(account_id, "character")
        
        for c in characters:
            if c.active_on_account == account_id:
                if active_char:
                    raise notifyUserException("The account has more than one active character")
                else:
                    active_char = c

        if active_char:
            return active_char
        else:
            raise notifyUserException("The account doesn't have any active characters")

    async def get_character(self, account_id:int, name:str) -> player_character:
        pool_id = await self.get_pool_by_account(account_id)
        result = await self.fetch(f"SELECT * FROM {self.char_table} WHERE pool_id = {pool_id} AND character_name = '{name}';")
        if type(result) == tuple:
            return player_character(result)
        elif len(result) == 0:
            raise notifyUserException("The account doesn't have access to that character")

    async def switch_active_character(self, account_id: int, name: str):
        pool_id = await self.get_pool_by_account(account_id)
        char = await self.fetch(f"SELECT character_name FROM {self.char_table} WHERE pool_id = {pool_id} AND character_name = '{name}'")
        if char:
            await self.commit(f"UPDATE {self.char_table} SET active_on_account = 0 WHERE active_on_account = {account_id};" \
                        f"UPDATE {self.char_table} SET active_on_account = {account_id} WHERE pool_id = {pool_id} AND character_name = '{name}'")
        else:
            raise notifyUserException("The account doesn't have access to that character")

    async def set_properties_of_character(self, character_id: int, **kwargs):
        changes = {}

        # name - check lenght/if the name is already taken in the pool
        if "character_name" in kwargs:
            n = str(kwargs["character_name"])
            if len(n) <=32 and '"' not in n and "'" not in n:
                changes["character_name"] = f'\'{str(kwargs["character_name"]).lower()}\''
            else:
                raise notifyUserException("The specified name needs to be 32 characters or less, and cannot contain ' or \", or any characters that aren't UTF-8")
        if "character_color" in kwargs:
            if kwargs["character_color"].lower() in ("none","null"):
                changes["character_color"] = "NULL"
            else:
                try:
                    c = kwargs["character_color"].removeprefix("0x")
                    c = c.removeprefix("#")
                    int(c, 16)
                    changes["character_color"] = f'\'{c[-6:]}\''
                except ValueError:
                    raise notifyUserException("The specified color is not a valid hex code.")
        if "character_image" in kwargs:
            if kwargs["character_image"].lower() in ("none","null"):
                changes["character_image"] = "NULL"
            else:
                try:
                    url = clean_url(str(kwargs["character_image"]))
                except:
                    raise notifyUserException("Invalid url")
                
                if validators.url(url):
                    changes["character_image"] = f'\"{url}\"'
                else:
                    raise notifyUserException("Invalid url")
        if "total_xp" in kwargs:
            if type(kwargs["total_xp"]) == int and kwargs["total_xp"] >= 0:
                changes["total_xp"] = kwargs["total_xp"]
            else:
                raise notifyUserException(f"Invalid value for total_xp: {kwargs['total_xp']}")
        if "roleplay_xp" in kwargs:
            if type(kwargs["roleplay_xp"]) == int and kwargs["roleplay_xp"] >= 0:
                changes["roleplay_xp"] = kwargs["roleplay_xp"]
            else:
                raise notifyUserException(f"Invalid value for roleplay_xp: {kwargs['roleplay_xp']}")
        if "level" in kwargs:
            if type(kwargs["level"]) == int and kwargs["level"] >= self.min_level and kwargs["level"] <= self.max_level:
                changes["level"] = kwargs["level"]
            else:
                raise notifyUserException(f"Invalid value for level: {kwargs['level']}")
        if "words_cached" in kwargs:
            if type(kwargs["words_cached"]) == int and kwargs["words_cached"] >= 0:
                changes["words_cached"] = kwargs["words_cached"]
            else:
                raise notifyUserException(f"Invalid value for words_cached: {kwargs['words_cached']}")
        if "level_notification" in kwargs:
            if type(kwargs["level_notification"]) == int and kwargs["level_notification"] in (0,1):
                changes["level_notification"] = kwargs["level_notification"]
            else:
                raise notifyUserException(f"Invalid value for level_notification: {kwargs['level_notification']}")

        if "owner_id" in kwargs: changes["owner_id"] = int(kwargs["owner_id"])
        if "pool_id" in kwargs: changes["pool_id"] = int(kwargs["pool_id"])
        if "active_on_account" in kwargs: changes["active_on_account"] = int(kwargs["active_on_account"])

        if changes:
            statement = f"UPDATE {self.char_table} SET {', '.join([f'{c[0]} = {c[1]}' for c in changes.items()])} WHERE character_id = {character_id};"
            await self.commit(statement)
        else:
            raise notifyUserException("No valid changes given")

    async def add_character_to_db(self, account_id: int, name: str) -> player_character:
        pool_id = await self.get_pool_by_account(account_id)

        characters = await self.fetch(f"SELECT character_name FROM {self.char_table} WHERE pool_id = {pool_id};")
        if characters:
            if type(characters) == tuple: characters = [characters] # if there's only one entry
            if len(characters) >= self.max_characters_per_pool:
                raise notifyUserException("Reached character limit")
            if name.lower() in [c[0] for c in characters]:
                raise notifyUserException("A character with the given name already exists")
        if len(name) > 32:
            raise notifyUserException("The desired name is too long. It needs to be 32 characters long or shorter.")
        if not name: name = "character"

        await self.commit(f"UPDATE {self.char_table} SET active_on_account = 0 WHERE active_on_account = {account_id};" \
                        f"INSERT INTO {self.char_table}" \
                        "(character_name, owner_id, pool_id, active_on_account)" \
                        "VALUES"\
                        f"('{name.lower()}', {account_id}, {pool_id}, {account_id});")
        return player_character(await self.fetch(f"SELECT * FROM {self.char_table} WHERE active_on_account = {account_id};"))

class xp_system(commands.Cog):
    def __init__(self, bot:discord.bot, configuration):
        self.bot: commands.Bot = bot
        self.db: xp_system_db_connection = self.bot.get_cog("xp_system_db_connection")

        self.debug: bool = configuration["debug"]
        self.notification_channel = configuration["notification_channel"]
        self.confirmation_timeout: float = 5

        # permissions
        self.default_perms: list = configuration["permissions"]["default"]
        self.role_permissions = {}
        base_perms = set(configuration["permissions"]["basic_permissions"])
        for role, perms in configuration["permissions"]["roles"].items():
            perms = set(perms)
            unresolved_perms = perms.difference(base_perms) # non-base perms
            while unresolved_perms:
                for c in unresolved_perms:
                    perms.remove(c)
                    perms = perms.union(set(configuration["permissions"]["nested_permissions"][c]))

                unresolved_perms = perms.difference(base_perms)

            self.role_permissions[role] = list(perms) # save the corresponding base perms

        # xp system config
        self.rp_categories: list = configuration["rp_categories"]
        self.level_req: dict = configuration["level_req"]
        self.xp_rate: int|dict = 1 if not "xp_rate" in configuration else configuration["xp_rate"]
        self.periodic_cap: None|int|dict = None if not "periodic_cap" in configuration else configuration["periodic_cap"]["value"]

    # cog functionality
    async def cog_check(self, ctx: commands.Context) -> bool:
        try:
            required_perms = ctx.command.extras["required_permissions"]
            if type(required_perms) not in (list, tuple):
                required_perms = [required_perms]
            
            if not required_perms:
                return True
        except:
            return True # no perms attached to command
        
        permissions = self.default_perms # get permissions
        for r in [r.id for r in ctx.author.roles]:
            if r in self.role_permissions:
                permissions.extend(self.role_permissions[r])
        
        required_perms = set(required_perms)
        permissions = set(permissions)

        if required_perms.difference(permissions):
            return False
        else:
            return True

    async def cog_command_error(self, ctx: commands.Context, error: commands.CommandError):
        emb = discord.Embed(color=0x800000)

        if type(error.__cause__) == notifyUserException:
            emb.description = str(error.__cause__)
        elif type(error) == commands.CheckFailure:
            emb.description = f"You're missing permissions to use that command.\nYou need the following permissions: `{', '.join(ctx.command.extras['required_permissions'])}`"
        elif isinstance(error, commands.UserInputError):
            emb.description = "User input error"
        else:
            emb.title = type(error).__name__
            emb.description = f"{str(error)}" if not self.debug else f"```\n{traceback.format_exc()}```"
        
        await ctx.send(embed=emb)

    # internal functions
    def _proccess_msg_for_rp(self, character: player_character, message_content: str) -> tuple[int, int]:
        words = count(message_content)
        rate = self._get_max_cache(character)
        overflow = (words + character.words_cached) % rate # get overflow XP to be stored as leftover for next time
        xp = ( words + character.words_cached - overflow) / rate # determine actual xp
        
        return (int(xp), overflow)

    async def _get_member_and_char_from_args(self, ctx: commands.Context, args: tuple[str]) -> tuple[discord.Member, player_character]:
        if len(args) > 2:
            raise notifyUserException(f"Too many arguments given.")
        
        character, member = None, None

        for arg in args:
            try:
                arg_member = await commands.MemberConverter().convert(ctx, arg)
                if member:
                    raise notifyUserException("Expected only one member, got multiple.")
                else:
                    member = arg_member
            except commands.ArgumentParsingError as e:
                raise e
            except Exception as e:
                if character:
                    raise notifyUserException("Expected only one character, got multiple.")
                else:
                    character = arg

        if not member:
            member = ctx.author
        elif member.bot:
            raise notifyUserException("This is a bot user and as such does not have any characters")
                
        character = await self.db.get_character(member.id, character) if character else await self.db.get_active_character(member.id)

        return (member, character)

    def _get_xp_until_lvl_up(self, character: player_character) -> int | None:
        try:
            return self.level_req[character.level+1] - character.total_xp
        except:
            return None

    def _get_max_cache(self, character: player_character) -> int:
        return self.xp_rate if type(self.xp_rate) == int else self.xp_rate[character.level]

    def _get_rp_cap(self, character: player_character) -> int | None:
        if self.periodic_cap:
            return self.periodic_cap if type(self.periodic_cap) == int else self.periodic_cap[character.level]
        else:
            return None
    
    async def ask_confirmation(self, ctx: commands.Context, message: str = None) -> bool:
        emb = discord.Embed(
            color = discord.Color.dark_grey(),
            description = f"### Are you sure?\n{message if message else ''}",
        )
        emb.set_footer(text="Type 'y' or 'yes' to confirm, or anything else to cancel")

        are_you_sure_message = await ctx.send(embed=emb)

        try:
            response = await self.bot.wait_for("message", check=lambda m: m.author == ctx.author and m.channel == ctx.channel, timeout=self.confirmation_timeout)
        except TimeoutError:
            await ctx.send("Cancelling.")
            await are_you_sure_message.delete()
        else:
            await are_you_sure_message.delete()
            if response.content.lower() in ("y", "yes"):
                return True
            else:
                await ctx.send("Cancelling.")
            return False

    async def check_and_notify_level_up(self, character: player_character):
        xp_til_level = self._get_xp_until_lvl_up(character)
        if xp_til_level and xp_til_level <= 0 and character.level_notification:
            await self.db.set_properties_of_character(character.id, level_notification = 0)
            await self.notification_channel.send(f"> <@{character.active_on_account if character.active_on_account else character.owner_id}>\nYou have enough experience to level up to lvl **{character.level+1}**! :sparkles:")

    # discord functionality
    # listeners
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or (message.channel.category.id not in self.rp_categories) or message.content.startswith(self.bot.command_prefix):
            return

        try:
            character = await self.db.get_active_character(message.author.id)
            gained_xp, overflow = self._proccess_msg_for_rp(character, message.content)
            
            cap = self._get_rp_cap(character)
            if cap:
                gained_xp = min(gained_xp, cap - character.roleplay_xp)
                character.roleplay_xp = character.roleplay_xp + gained_xp           
            character.total_xp += gained_xp

            if gained_xp or overflow:
                await self.db.set_properties_of_character(
                    character.id,
                    total_xp = character.total_xp,
                    roleplay_xp = character.roleplay_xp,
                    words_cached = overflow
                )
            
            await self.check_and_notify_level_up(character)

        except notifyUserException: # no active character
            pass

    @commands.Cog.listener()
    async def on_ready(self):
        self.notification_channel = self.bot.get_channel(self.notification_channel)

    # basic commands
    @commands.command(
            extras={"required_permissions":[]}
    )
    async def stats(self, ctx: commands.Context, *args):
        member, character = await self._get_member_and_char_from_args(ctx, args)
        
        emb = discord.Embed()

        emb.title = f"{character.name.capitalize()}'s stats"
        emb.color = character.color if character.color else member.color
        emb.set_thumbnail(url=character.image_url if character.image_url else member.display_avatar.url)

        emb.description  = f"**Level:** `{character.level}` {':crown:' if character.level >= self.db.max_level else ''}\n" \
                           f"**Total xp:** `{character.total_xp}`\n"
        remaining_xp = self._get_xp_until_lvl_up(character)
        emb.description += f"({remaining_xp} xp remaining until level {character.level+1})\n" if character.level < self.db.max_level else ""
        max_word_cache = self._get_max_cache(character)
        emb.description += f"**Words cached:** `{character.words_cached}/{max_word_cache}`\n" if max_word_cache > 1 else ""
        roleplay_cap = self._get_rp_cap(character)
        emb.description += f"**Roleplay xp this period:** `{character.roleplay_xp}/{roleplay_cap}`\n" if roleplay_cap else ""

        # rank
        ranked = await self.db.fetch(f"SELECT character_name, total_xp, owner_id, character_id FROM {self.db.char_table} ORDER BY total_xp DESC;")
        if type(ranked) == tuple: ranked = [ranked]
        rank = [r[3] for r in ranked].index(character.id) + 1
        rank_above = "" if rank == 1 else f"> {rank-1}. {ranked[rank-2][0].capitalize()} - {ranked[rank-2][1]} xp (<@{ranked[rank-2][2]}>)\n"
        rank_text = f"> {rank}. **{character.name.capitalize()} - {character.total_xp} xp** (<@{member.id}>)"
        rank_below = "" if rank == len(ranked) else f"\n> {rank+1}. {ranked[rank][0].capitalize()} - {ranked[rank][1]} xp (<@{ranked[rank][2]}>)"
        emb.add_field(name = f"Rank {rank}/{len(ranked)}",
                      inline = False,
                      value = rank_above+rank_text+rank_below)
        
        # list of accounts the character is available to
        accs = await self.db.fetch(f"SELECT account_id FROM {self.db.acc_table} WHERE pool_id = {character.pool_id};")
        if type(accs) == tuple: accs = [accs]
        emb.add_field(name=f" ",
                      inline = False,
                      value=f"This character is available to: {', '.join([f'<@{a[0]}>' for a in accs])}\n"\
                            f"It's owned by <@{character.owner_id}>"
        )

        await ctx.send(embed=emb)

    @commands.command(
            extras={"required_permissions":[]}
    )
    async def top(self, ctx: commands.Context):
        ranked = await self.db.fetch(f"SELECT character_name, total_xp, owner_id FROM {self.db.char_table} ORDER BY total_xp DESC;")
        if type(ranked) == tuple: ranked = [ranked]
        
        emb = discord.Embed(title = "Top 10 characters by xp",
                            color = ctx.author.color)
        emb.set_thumbnail(url = "https://images-ext-1.discordapp.net/external/mGTL2XzYxMsQa3yZDqwbLaAWUjuqjDhZjhKbn_eX9Gw/https/images.emojiterra.com/twitter/v14.0/512px/1f3c6.png?format=webp&quality=lossless&width=412&height=412")

        top_ten = [f"**{r+1}.** {ranked[r][0].capitalize()} - {ranked[r][1]} xp (<@{ranked[r][2]}>)" for r in range(min(len(ranked), 10))]

        emb.description = "\n".join(top_ten)

        characters = await self.db.get_available_characters(ctx.author.id)
        characters = [f"**{ranked.index((c.name, c.total_xp, c.owner_id))+1}.** {c.name.capitalize()} - {c.total_xp} xp" for c in characters]
        characters.sort()

        emb.add_field(name= "Your ranks:", value="\n".join(characters))

        await ctx.send(embed=emb)

    @commands.command(
            extras={"required_permissions":[]}
    )
    async def level_up(self, ctx: commands.Context):
        character = await self.db.get_active_character(ctx.author.id)
        xp_remaining = self._get_xp_until_lvl_up(character)

        emb = discord.Embed(color = character.color if character.color else ctx.author.color)

        if not xp_remaining: # max lvl
            emb.title = "Already at max level"
            emb.description = "You cannot level up, because you are already at the maximum possible level. Here's some cake :birthday:"
        elif xp_remaining <= 0: # enough to lvl
            character.level += 1
            await self.db.set_properties_of_character(character.id,
                                                      level=character.level,
                                                      level_notification=1)
            emb.title = f"Leveled up to {character.level}!"
            emb.description = "Congrats!" if character.level == self.db.max_level else f"{self._get_xp_until_lvl_up(character)} xp remaining until level {character.level+1}!"
        else: # not enough to lvl
            emb.title = f"Cannot level up"
            emb.description = f"You don't have enough xp to level up yet\n({self._get_xp_until_lvl_up(character)} xp remaining)"

        await ctx.send(embed=emb)
    
    @commands.command(
            name="add",
            extras={"required_permissions":["manage_xp"]}
    )
    async def _add(self, ctx: commands.Context, amount: int, *args):
        member, character = await self._get_member_and_char_from_args(ctx, args)

        prev_total = character.total_xp
        character.total_xp = character.total_xp + amount if character.total_xp + amount > 0 else 0

        await self.db.set_properties_of_character(character.id, total_xp=character.total_xp)

        await self.check_and_notify_level_up(character)

        emb = discord.Embed(title = "Modifying xp",
                            color = character.color if character.color else member.color,
                            description = f"{'Added' if amount >= 0 else 'Removed'} {abs(character.total_xp - prev_total)} xp {'to' if amount >= 0 else 'from'} {character.name.capitalize()} (<@{member.id}>)\n({prev_total} -> {character.total_xp})")
        await ctx.send(embed=emb)

    # character management
    @commands.group(
            invoke_without_command=True
    )
    async def char(self, ctx: commands.Context, name: str = None):
        if name:
            await self.db.switch_active_character(ctx.author.id, name)
            await ctx.send(f"Switched active character to {name.capitalize()}")
        else:
            try:
                active_character = await self.db.get_active_character(ctx.author.id)
            except notifyUserException:
                active_character = None

            available_characters = await self.db.get_available_characters(ctx.author.id)

            await ctx.send(f"Current active character: {active_character.name.capitalize() if active_character else '-'}\n\n"\
                           f"Available characters: {', '.join([c.name.capitalize() for c in available_characters])}")

    @char.command(
            extras={"required_permissions":["manage_characters_self"]}
    )
    async def add(self, ctx: commands.Context, name: str, member: discord.Member = None):
        if not member:
            member = ctx.author
        elif member != ctx.author:
            ctx.command.extras["required_permissions"].append("manage_characters_others")
            if not await self.cog_check(ctx): raise commands.CheckFailure()
            
        await self.db.add_character_to_db(member.id, name.lower())
        await ctx.send(f"Added character '{name.capitalize()}' and set it as active on {'your account' if member == ctx.author else member.mention}")

    @char.command(
            extras={"required_permissions":["manage_characters_self"]}
    )
    async def edit(self, ctx: commands.Context, *args):
        characters = await self.db.get_available_characters(ctx.author.id)
        character = await self.db.get_active_character(ctx.author.id)

        name, color, url = None, None, None
        changes = {}

        if len(args) > 3:
            raise notifyUserException("Too many arguments")
        
        for arg in args:
            if arg.startswith("-n=") or arg.startswith("-name="):
                name = arg[3:] if arg.startswith("-n=") else arg[6:]
                for c in characters:
                    if name == c.name: raise notifyUserException("You already have a character with that name")
            if arg.startswith("-c=") or arg.startswith("-color="):
                color = arg[3:] if arg.startswith("-c=") else arg[7:]
            if arg.startswith("-i=") or arg.startswith("-image="):
                url = arg[3:] if arg.startswith("-i=") else arg[7:]

        if name: changes["character_name"] = name.lower()

        if color: changes["character_color"] = color
        if url: changes["character_image"] = url
        
        await self.db.set_properties_of_character(character.id,
                                                **changes)

        await ctx.send("Updated active character")

    @char.command(
            extras={"required_permissions":["manage_characters_self"]}
    )
    async def delete(self, ctx: commands.Context, *args):
        member, character = await self._get_member_and_char_from_args(ctx, args)
        if member != ctx.author:
            ctx.command.extras["required_permissions"].append("manage_characters_others")
            if not await self.cog_check(ctx): raise commands.CheckFailure()

        if await self.ask_confirmation(ctx, f"You are about to delete '{character.name.capitalize()}'"):
            await self.db.commit(f"DELETE FROM {self.db.char_table} WHERE character_id = {character.id};")
            await ctx.send(f"Deleted '{character.name.capitalize()}'")

    @char.command(
            extras={"required_permissions":["manage_characters_self","manage_characters_others"]}
    )
    async def move(self, ctx: commands.Context, *args):
        if len(args) > 3 or len(args) < 2:
            raise notifyUserException("Incorrect number of arguments, 3 expected")

        src, character = await self._get_member_and_char_from_args(ctx, args[:-1])
        dest = await commands.MemberConverter().convert(ctx, args[-1])

        dest_pool = await self.db.get_pool_by_account(dest.id)

        if await self.ask_confirmation(ctx, f"You are about to move '{character.name.capitalize()}' from <@{src.id}> to <@{dest.id}>"):
            await self.db.set_properties_of_character(character.id,
                                                    owner_id = dest.id,
                                                    pool_id = dest_pool,
                                                    active_on_account = 0)
            
            await ctx.send(f"Moved '{character.name.capitalize()}' from <@{src.id}> to <@{dest.id}>")

    # pool management
    @commands.group(
            invoke_without_command=True
    )
    async def pool(self, ctx: commands.Context, member:discord.Member = None):
        if not member:
            member = ctx.author
        elif member.bot:
            raise notifyUserException("This is a bot user and as such does not have any characters")
        
        characters = await self.db.get_available_characters(member.id)
        pool_id = await self.db.get_pool_by_account(member.id)
        
        members = await self.db.fetch(f"SELECT account_id FROM {self.db.acc_table} WHERE pool_id = {pool_id}")
        if type(members) == tuple: members = [members]

        whose_pool_str = 'your' if member == ctx.author else member.display_name+"'s"
        await ctx.send(f"Characters in {whose_pool_str} pool:\n{', '.join([c.name.capitalize() for c in characters])}\n\n" \
                       f"Accounts sharing {whose_pool_str} pool:\n{', '.join([f'<@{m[0]}>' for m in members])}",
                       allowed_mentions=discord.AllowedMentions.none())

    @pool.command(
            extras={"required_permissions":["manage_pools_self"]}
    )
    async def merge(self, ctx: commands.Context, m1:discord.Member, m2:discord.Member = None):
        if m1.bot or (m2 and m2.bot):
            raise notifyUserException("Cannot merge pools with a bot")
        if m2:
            a, b = await self.db.get_pool_by_account(m1.id), await self.db.get_pool_by_account(m2.id)
        else:
            a, b = await self.db.get_pool_by_account(ctx.author.id), await self.db.get_pool_by_account(m1.id)
        
        if await self.ask_confirmation(ctx, f"You are about to merge <@{a}>'s and <@{b}>'s pools"):
            await self.db.merge_pools(a, b)

            await ctx.send(f"Merged <@{a}>'s and <@{b}>'s pools")

    @pool.command(
            extras={"required_permissions":["manage_pools_self"]}
    )
    async def separate(self, ctx: commands.Context, m1:discord.Member, m2:discord.Member = None):
        if m1.bot or (m2 and m2.bot):
            raise notifyUserException("Cannot separate pools of a bot")
        if m2:
            a, b = m1.id, m2.id
        else:
            a, b = ctx.author.id, m1.id
        
        if a != ctx.author.id or b != ctx.author.id:
            ctx.command.extras["required_permissions"].append("manage_pools_others")
            if not await self.cog_check(ctx): raise commands.CheckFailure()

        if await self.ask_confirmation(ctx, f"You are about to seperate <@{a}>'s characters from <@{b}>'s pool"):
            await self.db.separate_pools(a, b)

            await ctx.send(f"Seperated <@{a}>'s characters from <@{b}>'s pool")

    # debug commands
    @commands.group(
            invoke_without_command=True
    )
    async def debug(self, ctx: commands.Context):
        await ctx.send(f"Debug: {self.debug}")

    @debug.command(
            extras={"required_permissions":["debug"]}
    )
    async def reload(self, ctx: commands.Context):
        print("Reloading XP system")
        await ctx.send("Reloading XP system...")
        self.bot.reload_extension("xp_system_extension")

    @debug.command(
            extras={"required_permissions":["debug"]}
    )
    async def period_reset(self, ctx: commands.Context):
        await self.db.periodic_reset()
        await ctx.send("Reset periodic cap")

    @debug.command(
            extras={"required_permissions":["debug"]}
    )
    async def count(self, ctx: commands.Context, start, end):
        await ctx.send("not implemented yet")

    @debug.command(
            extras={"required_permissions":["debug"]}
    )
    async def toggle(self, ctx: commands.Context):
        self.debug = False if self.debug else True
        await ctx.send(f"Set debug mode to {self.debug}")
