import requests
import logging
import json
import re
import html
import asyncio
import hashlib
import os
from typing import Final
from urllib.parse import quote_plus
import time
from datetime import datetime, timedelta
from datetime import time as dt_time
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import aiohttp
# PyMongo, motor error handling အတွက် import
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure, ConnectionFailure 
from motor.motor_asyncio import AsyncIOMotorClient
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes
from telegram.error import BadRequest

# --- Apply nest_asyncio for environments like Pydroid ---
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass # Not needed if not in Pydroid/similar environment

load_dotenv('bot.env')

# --- Global Configurations and Constants ---
# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB Configuration
username = os.getenv('DB_USERNAME')
password = os.getenv('DB_PASSWORD')
if not username or not password:
    raise ValueError("DB_USERNAME and DB_PASSWORD must be set in bot.env")
encoded_username = quote_plus(str(username))
encoded_password = quote_plus(str(password))

MONGO_URI = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster123.emqrsdp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster123"

# 🚨 FIX: MongoDB connection ကို Global Try/Except ထဲ ထည့်သွင်းခြင်း
try:
    client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client['smilebot']
    users_collection = db['user']  # user data
    order_collection = db['order']  # order data for mlbb and magic chess
    client.admin.command('ping') # Connection ကို စမ်းသပ်ခြင်း
    logger.info("Successfully connected to MongoDB.")
except (ServerSelectionTimeoutError, ConnectionFailure) as e:
    logger.error(f"MongoDB Connection Failed at startup: {e}")
    # Bot စတင်သည့်အခါ ချိတ်ဆက်မှုမအောင်မြင်ပါက Bot ကို ဆက် run စေပြီး Error message ပြပါမည်။
    client = None
    db = None
    users_collection = None
    order_collection = None


# Telegram Bot Token
BOT_TOKEN = os.getenv('BOTKEY')
if not BOT_TOKEN:
    raise ValueError("BOTKEY must be set in bot.env")

# Smile.one API Keys
SMILE_ONE_BASE_URL_PH: Final = "https://www.smile.one/ph"
SMILE_ONE_BASE_URL_BR: Final = "https://www.smile.one/br"
UID = os.getenv('UID')
EMAIL = os.getenv('EMAIL')
KEY = os.getenv('KEY')
if not all([UID, EMAIL, KEY]):
    logger.warning("One or more Smile.one API credentials (UID, EMAIL, KEY) are not set in bot.env. Smile.one API calls may fail.")

DEFAULT_PRODUCT_ID: Final = "213" # This is likely for MLBB, keep for existing functions

# Admin IDs (Telegram User IDs)
admins = [6619783517 ,6765997852, 5671920054]

# Global flag to control bot's operational status
is_bot_paused = False 

# Dictionary to store the state for each user (e.g., waiting for sec value)
user_data_state = {}

# --- Region-Country Mapping Dictionary ---
region_to_country = {
    "MM": "Myanmar", "TH": "Thailand", "JP": "Japan", "KR": "South Korea",
    "US": "United States", "FR": "France", "DE": "Germany", "IN": "India",
    "BR": "Brazil", "AU": "Australia", "CN": "China", "RU": "Russia",
    "CA": "Canada", "GB": "United Kingdom", "ES": "Spain", "IT": "Italy",
    "MX": "Mexico", "AR": "Argentina", "CL": "Chile", "CO": "Colombia",
    "ZA": "South Africa", "EG": "Egypt", "NG": "Nigeria", "KE": "Kenya",
    "PK": "Pakistan", "BD": "Bangladesh", "VN": "Vietnam", "PH": "Philippines",
    "ID": "Indonesia", "MY": "Malaysia", "SG": "Singapore", "NZ": "New Zealand",
    "SA": "Saudi Arabia", "AE": "UAE", "TR": "Turkey",
}

# --- Product Info ---
magic_chess_product_info_ph = {
    "55":{"id":"23918","rate":48.95},"165":{"id":"23919","rate":145.04},"275":{"id":"23920","rate":241.08},"565":{"id":"23921","rate":488.04},"5":{"id":"23906","rate":4.90},"11":{"id":"23907","rate":9.31},"22":{"id":"23908","rate":18.62},"33":{"id":["23907","23908"],"rate":27.93,"rates_per_component":{"23907":9.31,"23908":18.62}},"44":{"id":["23908","23908"],"rate":37.24,"rate_per_component":18.62},"56":{"id":"23909","rate":46.55},"112":{"id":"23910","rate":93.10},"223":{"id":"23911","rate":186.20},"336":{"id":"23912","rate":279.30},"570":{"id":"23913","rate":465.50},"1163":{"id":"23914","rate":931.00},"2398":{"id":"23915","rate":1862.00},"6042":{"id":"23916","rate":4655.00},"wdp":{"id":"23922","rate":98.00},
}
magic_chess_product_info_br = {"55":{"id":"23837","rate":40.00},"165":{"id":"23838","rate":120.00},"275":{"id":"23839","rate":200.00},"565":{"id":"23840","rate":400.00},"86":{"id":"23825","rate":62.50},"172":{"id":"23826","rate":125.00},"257":{"id":"23827","rate":187.00},"344":{"id":"23828","rate":250.00},"516":{"id":"23829","rate":375.00},"706":{"id":"23830","rate":500.00},"1346":{"id":"23831","rate":937.50},"1825":{"id":"23832","rate":1250.00},"2195":{"id":"23833","rate":1500.00},"3688":{"id":"23834","rate":2500.00},"5532":{"id":"23835","rate":3750.00},"9288":{"id":"23836","rate":6250.00},"wp":{"id":"23841","rate":99.90},"wp2":{"id":["23841","23841"],"rate":199.80,"rate_per_component":99.90},"wp3":{"id":["23841","23841","23841"],"rate":299.70,"rate_per_component":99.90},"wp4":{"id":["23841","23841","23841","23841"],"rate":399.60,"rate_per_component":99.90},"wp5":{"id":["23841","23841","23841","23841","23841"],"rate":499.50,"rate_per_component":99.90},"LBB":{"id":"25585","rate":41.40},"BFD":{"id":"25586","rate":41.40},"PB":{"id":"25587","rate":41.40},}
mlbb_product_info_ph = {
    "11": {"id": "212", "rate": 9.50}, "22": {"id": "213", "rate": 19.00},"33": {"id": ["212", "213"], "rate": 28.50, "rates_per_component": {"212": 9.50, "213": 19.0}}, "44": {"id": ["213", "213"], "rate": 38.00,"rates_per_component": "19.00"},"56": {"id": "214", "rate": 47.50}, "112": {"id": "215", "rate": 95.00}, "223": {"id": "216", "rate": 190.00}, "336": {"id": "217", "rate": 285.00}, "570": {"id": "218", "rate": 475.00}, "1163": {"id": "219", "rate": 950.00}, "2398": {"id": "220", "rate": 1900.00}, "6042": {"id": "221", "rate": 4750.00}, "wdp": {"id": "16641", "rate": 95.00},
}
mlbb_product_info_br = {
    "svp":{"id":"22594","rate":39.00},"55":{"id":"22590","rate":39.00},"165":{"id":"22591","rate":116.90},"275":{"id":"22592","rate":187.50},"565":{"id":"22593","rate":385.00},"wp":{"id":"16642","rate":76.00},"wp2":{"id":["16642","16642"],"rate":152.00,"rate_per_component":76.00},"wp3":{"id":["16642","16642","16642"],"rate":228.00,"rate_per_component":76.00},"wp4":{"id":["16642","16642","16642","16642"],"rate":304.00,"rate_per_component":76.00},"wp5":{"id":["16642","16642","16642","16642","16642"],"rate":380.00,"rate_per_component":76.00},"wp10":{"id":["16642","16642","16642","16642","16642","16642","16642","16642","16642","16642"],"rate":760.00,"rate_per_component":76.00},"tlp":{"id":"33","rate":402.50},"86":{"id":"13","rate":61.50},"172":{"id":"23","rate":122.00},"257":{"id":"25","rate":177.50},"706":{"id":"26","rate":480.00},"2195":{"id":"27","rate":1453.00},"3688":{"id":"28","rate":2424.00},"5532":{"id":"29","rate":3660.00},"9288":{"id":"30","rate":6079.00},"343":{"id":["13","25"],"rate":239.00,"rates_per_component":{"13":61.50,"25":177.50}},"344":{"id":["23","23"],"rate":244.00,"rate_per_component":122.00},"429":{"id":["23","25"],"rate":299.50,"rates_per_component":{"23":122.00,"25":177.50}},"514":{"id":["25","25"],"rate":355.00,"rate_per_component":177.50},"600":{"id":["25","25","13"],"rate":416.50,"rates_per_component":{"25":177.50,"13":61.50}},"792":{"id":["26","13"],"rate":541.50,"rates_per_component":{"26":480.00,"13":61.50}},"878":{"id":["26","23"],"rate":602.00,"rates_per_component":{"26":480.00,"23":122.00}},"963":{"id":["26","25"],"rate":657.50,"rates_per_component":{"26":480.00,"25":177.50}},"1049":{"id":["26","25","13"],"rate":719.00,"rates_per_component":{"26":480.00,"25":177.50,"13":61.50}},"1135":{"id":["26","25","23"],"rate":779.50,"rates_per_component":{"26":480.00,"25":177.50,"23":122.00}},"1220":{"id":["26","25","25"],"rate":835.00,"rate_per_component":177.50},"1412":{"id":["26","26"],"rate":960.00,"rate_per_component":480.00},"1584":{"id":["26","26","23"],"rate":1082.00,"rates_per_component":{"26":480.00,"23":122.00}},"1755":{"id":["26","26","25","13"],"rate":1199.00,"rates_per_component":{"26":480.00,"25":177.50,"13":61.50}},"2901":{"id":["27","26"],"rate":1933.00,"rates_per_component":{"27":1453.00,"26":480.00}},"4390":{"id":["27","27"],"rate":2906.00,"rate_per_component":1453.00},"11483":{"id":["30","27"],"rate":7532.00,"rates_per_component":{"30":6079.00,"27":1453.00}},"86wp":{"id":["13","16642"],"rate":137.50,"rates_per_component":{"13":61.50,"16642":76.00}},"172wp":{"id":["23","16642"],"rate":198.00,"rates_per_component":{"23":122.00,"16642":76.00}},"86wp2":{"id":["13","16642","16642"],"rate":213.50,"rates_per_component":{"13":61.50,"16642":76.00}},"257wp":{"id":["25","16642"],"rate":253.50,"rates_per_component":{"25":177.50,"16642":76.00}},"B12":{"id":["22590","22591"],"rate":155.90,"rates_per_component":{"22590":39.00,"22591":116.90}},"B123":{"id":["22590","22591","22592"],"rate":343.40,"rates_per_component":{"22590":39.00,"22591":116.90,"22592":187.50}},"B23":{"id":["22591","22592"],"rate":304.40,"rates_per_component":{"22591":116.90,"22592":187.50}},"B1234":{"id":["22590","22591","22592","22593"],"rate":728.40,"rates_per_component":{"22590":39.00,"22591":116.90,"22592":187.50,"22593":385.00}},
}

# BIGO Live အတွက် ဈေးနှုန်းစာရင်း
BIGO_product_info_br = {
    "20": {"id": "16081", "rate": 22.80},
    "50": {"id": "16082", "rate": 56.70},
    "100": {"id": "16083", "rate": 115.30},
    "200": {"id": "16084", "rate": 232.50},
    "500": {"id": "16085", "rate": 575.50},
    "1000": {"id": "18013", "rate": 1135.80},
    "2000": {"id": "16086", "rate": 2240.60},
    "5000": {"id": "16087", "rate": 5683.80},
    "10000": {"id": "16088", "rate": 11198.0},
}

############ Helper Functions Definitions ###############

def check_db_ready(update: Update = None) -> bool:
    """Checks if MongoDB connection objects are initialized."""
    global users_collection, order_collection
    if users_collection is None or order_collection is None:
        error_msg = "🛑 Database connection is currently unavailable. Please check the MongoDB connection URI and server IP access."
        if update and update.message:
            logger.error(error_msg)
        else:
            logger.error(error_msg)
        return False
    return True

async def safe_edit_message(message, new_text, **kwargs):
    """Edits a message safely, ignores 'Message is not modified' errors."""
    try:
        if message.text != new_text:
            await message.edit_text(new_text, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise

def is_admin(user_id: int) -> bool:
    """Checks if the user is an admin."""
    return user_id in admins

async def check_bot_status(update: Update) -> bool:
    """Checks if the bot is paused and sends a message if it is."""
    global is_bot_paused
    if is_bot_paused and not is_admin(update.message.from_user.id):
        await update.message.reply_text(
            "🛑 Bot is currently paused for maintenance. Please try again later."
        )
        return True
    return False

async def resolve_user_identifier(identifier: str):
    """Resolves an identifier (Telegram ID or username) to a user_id string and a display name."""
    if not check_db_ready(): return None, None
    
    identifier = identifier.strip('()')
    if identifier.isdigit():
        user = await users_collection.find_one({"user_id": identifier})
        if user:
            return user['user_id'], user.get('username', identifier)
        return identifier, identifier
    else:
        search_username = identifier.lstrip('@')
        user = await users_collection.find_one({"username": search_username})
        if user:
            return user['user_id'], identifier
        else:
            return None, identifier 

async def is_registered_user(user_id: str, update: Update = None) -> bool:
    """Checks if the user is fully registered (has date_joined) and optionally sends a message."""
    if not check_db_ready(update): return False

    try:
        user_doc = await users_collection.find_one({"user_id": user_id})
        if not user_doc or not user_doc.get('date_joined'):
            if update and update.message:
                await update.message.reply_text(
                    "You are not registered to use this command. Please ask an admin to register you.💁🏻", 
                    parse_mode='HTML'
                )
            return False
        return True
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during registration check for {user_id}: {e}")
        if update and update.message:
            await update.message.reply_text("Database connection failed during registration check. Please try again later.", parse_mode='HTML')
        return False


def calculate_sign(params):
    """Calculates the 'sign' parameter required for Smile.one API requests."""
    sorted_params = sorted(params.items())
    query_string = '&'.join([f"{k}={v}" for k, v in sorted_params])
    query_string += f"&{KEY}"
    hashed_string = hashlib.md5(hashlib.md5(
        query_string.encode()).hexdigest().encode()).hexdigest()
    return hashed_string

async def get_role_info(userid: str, zoneid: str, product_id: str = DEFAULT_PRODUCT_ID, product_type: str = 'mobilelegends'):
    """Fetches role information (in-game username) from Smile One."""
    endpoint = f"{SMILE_ONE_BASE_URL_PH}/smilecoin/api/getrole"
    current_time = int(time.time())
    params = {
        'uid': UID, 'email': EMAIL, 'userid': userid, 'zoneid': zoneid,
        'product': product_type, 'productid': product_id, 'time': current_time
    }
    params['sign'] = calculate_sign(params)

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(endpoint, data=params, headers={'Content-Type': 'application/x-www-form-urlencoded'}) as response:
                response.raise_for_status()
                data = await response.json()
                logger.info(f"Smile One getrole API response for {userid} ({zoneid}) Product Type {product_type}: {data}")
                return data if data.get('status') == 200 else None
        except aiohttp.ClientError as e:
            logger.error(f"Error fetching role info for {userid} ({zoneid}) Product Type {product_type}: {e}")
            return None

async def get_smile_one_balances():
    """Fetches Smile One account balances for various product types/regions."""
    endpoint_ph = f"{SMILE_ONE_BASE_URL_PH}/smilecoin/api/querypoints"
    endpoint_br = f"{SMILE_ONE_BASE_URL_BR}/smilecoin/api/querypoints"

    async def _get_points(endpoint, region, product_type=None):
        current_time = int(time.time())
        params = {'uid': UID, 'email': EMAIL, 'time': current_time}
        if product_type:
            params['product'] = product_type
        params['sign'] = calculate_sign(params)

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(endpoint, data=params) as response:
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as e:
                logger.error(f"Error fetching {region} points for {product_type if product_type else 'generic'}: {e}")
                return None

    ph_mlbb_resp = await _get_points(endpoint_ph, "PH", "mobilelegends")
    br_mlbb_resp = await _get_points(endpoint_br, "BR", "mobilelegends")

    return {
        'ph_mlbb': ph_mlbb_resp.get('smile_points', 'Unavailable') if ph_mlbb_resp else 'Unavailable',
        'br_mlbb': br_mlbb_resp.get('smile_points', 'Unavailable') if br_mlbb_resp else 'Unavailable',
    }

async def get_balance(user_id: str):
    """Fetches a user's PH and BR balances from the database."""
    if not check_db_ready(): return None
    try:
        user = await users_collection.find_one({"user_id": user_id})
        if user:
            return {'balance_ph': user.get('balance_ph', 0), 'balance_br': user.get('balance_br', 0)}
        return None
    except (ServerSelectionTimeoutError, OperationFailure):
        return None

async def update_balance(user_id: str, amount: float, balance_type: str):
    """Atomically updates the balance of the specified user."""
    if not check_db_ready(): return None
    try:
        query = {"user_id": user_id}
        if amount < 0:
            query[balance_type] = {"$gte": abs(amount)}
            
        result = await users_collection.find_one_and_update(
            query, {"$inc": {balance_type: amount}}, return_document=True)

        if result:
            return result.get(balance_type)
        else:
            logger.warning(f"Balance update failed for user {user_id}: Insufficient balance or user not found.")
            return None
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during balance update for {user_id}: {e}")
        return None

def split_message(text, max_length=4096):
    """Splits the message into chunks that fit within the Telegram message limit."""
    if len(text) <= max_length:
        return [text]
    messages = []
    current_chunk_lines = []
    current_chunk_length = 0
    lines = text.split('\n')
    for line in lines:
        if current_chunk_length + len(line) + 1 > max_length:
            if current_chunk_lines:
                messages.append("\n".join(current_chunk_lines))
            current_chunk_lines = [line]
            current_chunk_length = len(line) + 1
            if len(line) > max_length:
                for i in range(0, len(line), max_length):
                    messages.append(line[i:i + max_length])
                current_chunk_lines = []
                current_chunk_length = 0
        else:
            current_chunk_lines.append(line)
            current_chunk_length += len(line) + 1
    if current_chunk_lines:
        messages.append("\n".join(current_chunk_lines))
    return messages

async def create_order_and_log(userid: str, zoneid: str, product_id: str, base_url: str, product_type: str):
    """Sends an order creation request to Smile One API and logs the result."""
    endpoint = f"{base_url}/smilecoin/api/createorder"
    current_time = int(time.time())
    params = {
        'uid': UID, 'email': EMAIL, 'userid': userid, 'zoneid': zoneid,
        'product': product_type, 'productid': product_id, 'time': current_time
    }
    params['sign'] = calculate_sign(params)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(endpoint, data=params, headers={'Content-Type': 'application/x-www-form-urlencoded'}) as response:
                response.raise_for_status()
                data = await response.json()
                if data.get('status') == 200:
                    return {"order_id": data.get('order_id')}
                else:
                    return {"order_id": None, "reason": data.get('message', 'Unknown error')}
        except aiohttp.ClientError as e:
            logger.error(f"Error creating order via {base_url} ({product_type}): {e}")
            return {"order_id": None, "reason": str(e)}

############ Telegram Bot Command Handlers ###############

## General User Commands
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Sends a welcome message and explains how to use the bot."""
    if await check_bot_status(update): return
    if not check_db_ready(update): return

    user_chat_id = update.effective_chat.id
    user_id = str(update.message.from_user.id)
    username = update.message.from_user.username
    logger.info(f"Received .start from chat_id: {user_chat_id} (User ID: {user_id})")

    try:
        await users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"username": username},
             "$setOnInsert": {"balance_ph": 0, "balance_br": 0}},
            upsert=True
        )

        user = await users_collection.find_one({"user_id": user_id})

        if not user.get('date_joined'):
            await update.message.reply_text(
                ("<b>WELCOME TO ANGELSTROE Auto Recharge Bot </b>\n\n"
                 "You are not yet registered to use this bot's full features.\n"
                 "Please ask @angelstore20 for register to you. Your Telegram ID is: <code>{}</code>\n"
                 "You can share this ID with your admin for registration.").format(html.escape(user_id)),
                parse_mode="HTML"
            )
        else:
            balance_ph = user.get('balance_ph', 0)
            balance_br = user.get('balance_br', 0)
            await update.message.reply_text(
                ("<b>HI! DEAR,</b>\n"
                 "Your current balances:\n"
                 f"🇵🇭 PH Balance : ${balance_ph:.2f}\n"
                 f"🇧🇷 BR Balance : ${balance_br:.2f}\n\n"
                 "<b>PLEASE PRESS .help FOR HOW TO USE</b>"),
                parse_mode="HTML"
            )
    except (ServerSelectionTimeoutError, OperationFailure, ConnectionFailure) as e:
        logger.error(f"DB Error during start_command: {e}")
        await update.message.reply_text("Database connection failed. Please try again later.", parse_mode='HTML')


async def getid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the user's Telegram ID and username."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    username = update.message.from_user.username
    display_name = f"@{username}" if username else str(user_id)
    await update.message.reply_text(f"Your Telegram user is: <b>{html.escape(display_name)}</b> (ID: <code>{html.escape(user_id)}</code>)", parse_mode='HTML')

async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the user's current PH and BR balances."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return

    balances = await get_balance(user_id)
    if balances:
        await update.message.reply_text(
            (f"<code>Your Current Coin 🪙:\n\n"
             f"🇧🇷 BR Coins  : {balances.get('balance_br', 0):.2f} 🪙\n"
             f"🇵🇭 PH Coins  : {balances.get('balance_ph', 0):.2f} 🪙</code>"),
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text("Error: Could not retrieve your balance.", parse_mode='HTML')

# 🚨 ပြင်ဆင်ထားသော help_command (User Command Helper)
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays a list of available commands and contact info."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return

    help_message = (f"<b>Hello </b> {html.escape(str(update.message.from_user.username if update.message.from_user.username else 'User'))}💌\n"
                    "🙋🏻‍♂️Dear Users,\nNew Commands Have Been Added 💁🏻‍♂️\n\n"
                    "<b>--- General Commands ---</b>\n"
                    "<b>.help</b>    - Show this help message\n"
                    "<b>.use</b>     - How to use payment commands\n"
                    "<b>.bal</b>     - Check your current coin balance 💰\n"
                    "<b>.his</b> [today|week|month|&lt;id&gt; &lt;zoneid&gt;] - Get your orders history 📃\n"
                    "<b>.getid</b>   - Get your Telegram User ID 🆔\n"
                    "<b>.topup</b>   - Add Smile Coin to your wallet 🪙\n"
                    "<b>.role</b>    - Check player IGN for MLBB\n\n"
                    "<b>--- Price List & Game Info ---</b>\n"
                    "<b>.pricebr</b>   - Brazil MLBB Price List 🇧🇷\n"
                    "<b>.priceph</b>   - Philippines MLBB Price List 🇵🇭\n"
                    "<b>.mcpricebr</b> - Brazil Magic Chess Price List 🇧🇷\n"
                    "<b>.mcpriceph</b> - Philippines Magic Chess Price List 🇵🇭\n"
                    "<b>.bigopricebr</b> - Brazil BIGO Price List 🇧🇷\n"
                    "<b>.mcgg</b>      - Check Magic Chess IGN\n"
                    "<b>.pubg</b>      - Check PUBG IGN\n"
                    "<b>.hok</b>       - Check Honor of Kings IGN\n"
                    "<b>.dtf</b>       - Check DTF IGN\n\n"
                    "<b>If You Have Any Questions,</b>\n"
                    "Please Contact To @angelstore20 💬")
    await update.message.reply_text(help_message, parse_mode='HTML')

async def use_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provides instructions on how to use the bot."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return

    response_message = ("Welcome! Here's how you can use the bot:\n\n"
                        "For Brazil 🇧🇷:\n`.ml [id] [server id] [amount]`\n"
                        "E.G - `.ml 12345678 2222 wp`\n\n"
                        "For Philippines 🇵🇭:\n`.mlp [id] [server id] [amount]`\n"
                        "E.G - `.mlp 12345678 2222 11`\n\n"
                        "For Magic Chess Go Go (Philippines 🇵🇭):\n`.mcggp [id] [server id] [amount]`\n"
                        "E.G - `.mcggp 12345678 2222 55`\n\n"
                        "For Magic Chess Go Go (Brazil 🇧🇷):\n`.mcggb [id] [server id] [amount]`\n"
                        "E.G - `.mcggb 12345678 2222 55`\n\n"
                        "For Brazil 🇧🇷 .pricebr.\nFor Philippines 🇵🇭 .priceph.\n"
                        "FAILED ORDER 🚫 If it occurs, please notify the Admin.\n"
                        "For more details, contact @angelstore20.")
    await update.message.reply_text(response_message, parse_mode='HTML')

# --- START NEW ENHANCED HIS COMMAND ---

async def enhanced_get_user_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Retrieves and displays the requesting user's order history based on arguments:
    .his (no args) -> last 10 orders (default behavior)
    .his today|week|month
    .his <gameid> <zoneid>
    Admin only: .his <gameid>(<zoneid>) [DD.MM.YYYY] (handled by get_order_by_player_id_command)
    """
    if await check_bot_status(update): return
    sender_user_id = str(update.message.from_user.id)
    is_current_user_admin = is_admin(update.message.from_user.id)

    # 1. Registration Check (Only required for non-admin users)
    if not is_current_user_admin:
        if not await is_registered_user(sender_user_id, update): return
    if not check_db_ready(update): return

    # --- Argument Parsing ---
    args = [arg.lower() for arg in context.args]
    query_filter = {"sender_user_id": sender_user_id}
    limit_count = 10  # Default limit for no arguments
    report_title_suffix = "Last 10 Orders"

    # Time-based filtering (.his today|week|month)
    if len(args) == 1:
        keyword = args[0]
        now = datetime.now(ZoneInfo("Asia/Yangon"))

        if keyword == "today":
            report_title_suffix = f"Orders for Today ({now.strftime('%d.%m.%Y')})"
            query_filter["date"] = {"$regex": f".*{now.strftime('%d.%m.%Y')}$"}
            limit_count = 0  # No limit for time period queries
        elif keyword == "week":
            report_title_suffix = f"Orders for the Current Week"
            limit_count = 0
        elif keyword == "month":
            report_title_suffix = f"Orders for the Current Month"
            limit_count = 0
        
        # Admin combined ID check (pass to admin function)
        elif is_current_user_admin and '(' in args[0]:
            return await get_order_by_player_id_command(update, context)
            
        else:
            # Unrecognized single argument for non-admin
            if not is_current_user_admin:
                await update.message.reply_text("❌ Invalid usage. Use `.his today`, `.his week`, `.his month`, or `.his <gameid> <zoneid>`.", parse_mode='HTML')
                return

    # Game ID + Zone ID filtering (.his <gameid> <zoneid>)
    elif len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        game_id, zone_id = args[0], args[1]
        report_title_suffix = f"Orders for ID: {game_id} ({zone_id})"
        query_filter["user_id"] = game_id
        query_filter["zone_id"] = zone_id
        limit_count = 0 # No limit
    
    # Admin full query check (pass to admin function)
    elif is_current_user_admin and len(args) == 3:
        return await get_order_by_player_id_command(update, context)

    elif len(args) > 0:
        # Invalid arguments for user/admin
         await update.message.reply_text("❌ Invalid usage. Use `.his today`, `.his week`, `.his month`, or `.his <gameid> <zoneid>`.", parse_mode='HTML')
         return


    # --- Database Query ---
    loading_message = await update.message.reply_text("🔍 Searching order history...", parse_mode='HTML')
    
    try:
        transactions_cursor = order_collection.find(query_filter).sort([("date", -1)])
        
        # Check if we need to fetch all and filter locally for week/month (due to string date format)
        transactions_list = []
        if limit_count == 0 and len(args) == 1 and args[0] in ["week", "month"]:
            transactions_list = await transactions_cursor.to_list(length=None)
            
            # Local filtering for 'week' and 'month'
            filtered_list = []
            now = datetime.now(ZoneInfo("Asia/Yangon")).replace(hour=0, minute=0, second=0, microsecond=0)
            
            if args[0] == "week":
                start_date = now - timedelta(days=now.weekday())  # Start of the week (Monday)
            elif args[0] == "month":
                start_date = now.replace(day=1)

            for order in transactions_list:
                try:
                    # Parse the string date from the DB (e.g., '10:30:00AM 02.11.2025')
                    date_str = order.get('date', '').split(' ')[1]
                    order_date = datetime.strptime(date_str, '%d.%m.%Y').replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=ZoneInfo("Asia/Yangon"))
                    
                    if order_date >= start_date:
                        filtered_list.append(order)
                except Exception:
                    logger.warning(f"Could not parse date for order: {order.get('_id')}")
                    
            transactions_list = filtered_list
            
        else:
            # For 'today', specific ID query, or default (last 10)
            transactions_list = await transactions_cursor.to_list(length=limit_count if limit_count > 0 else None)


        # --- Response Generation ---
        sender_user_db = await users_collection.find_one({"user_id": sender_user_id})
        sender_display_name = f"@{sender_user_db['username']}" if sender_user_db and sender_user_db.get('username') else sender_user_id
        
        response_summary_parts = [f"==== Order History for <b>{html.escape(sender_display_name)}</b> ({report_title_suffix}) ====\n\n"]

        if not transactions_list:
            await safe_edit_message(loading_message, f"You have no recorded orders for the period/ID requested.", parse_mode='HTML')
            return

        for order in transactions_list:
            player_id = order.get('user_id', 'N/A')
            order_ids = order.get('order_ids', 'N/A')
            if isinstance(order_ids, list):
                order_ids_str = ', '.join(map(str, order_ids))
            else:
                order_ids_str = str(order_ids)
            
            actual_cost = float(order.get('total_cost', 0.0)) - float(order.get('refunded_amount', 0.0))

            response_summary_parts.append(
                (f"🆔 Game ID: <code>{html.escape(str(player_id))}</code>\n"
                f"🌍 Zone ID: {html.escape(str(order.get('zone_id', 'N/A')))}\n"
                f"💎 Pack: {html.escape(str(order.get('product_name', 'N/A')))} ({html.escape(order.get('region', 'N/A').upper())})\n"
                f"🆔 Order ID: <code>{html.escape(order_ids_str)}</code>\n"
                f"📅 Date: {html.escape(order.get('date', 'N/A'))}\n"
                f"💵 Cost: ${actual_cost:.2f} 🪙\n"
                f"🔄 Status: <b>{html.escape(str(order.get('status', 'N/A'))).upper()}</b>\n\n")
            )
        
        full_response_summary = "".join(response_summary_parts)
        await loading_message.delete()
        
        for msg in split_message(full_response_summary):
            await update.message.reply_text(msg, parse_mode='HTML')

    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during enhanced_get_user_orders: {e}")
        await safe_edit_message(loading_message, "Database connection failed while fetching orders. Please try again later.", parse_mode='HTML')
        
# --- END NEW ENHANCED HIS COMMAND ---

# The original get_user_orders is now replaced by enhanced_get_user_orders
# so it is removed from here to avoid duplication.

## Price List Commands
async def pricebr_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the Mobile Legends price list for Brazil."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return
    await update.message.reply_text(
        "<b>Pack List (FOR BR):</b>\n<b>🇧🇷 Brazil:</b>\nDOUBLE DIAMOND PACK\n- svp: 39.00🪙\n- 55: 39.00🪙\n- 165: 116.90🪙\n- 275: 187.50🪙\n- 565: 385.00🪙\n\nCOMBO PACKS\n- B12 (55+165) : 155.90🪙\n- B123 (55+165+275) : 343.40🪙\n- B23 (165+275) : 304.40🪙\n- B1234 (All Double Dia ): 728.40🪙\n\nNORMAL DIAMOND PACK\n- 86wp: 137.50🪙\n- 86wp2: 213.50🪙\n- 172wp: 198.00🪙\n- 257wp: 253.50🪙\n- wp: 76.00🪙\n- wp2: 152.00🪙\n- wp3: 228.00🪙\n- wp4: 304.00🪙\n- wp5: 380.00🪙\n- wp10: 760.00🪙\n- tlp: 402.50🪙\n- 86: 61.50🪙\n- 172: 122.00🪙\n- 257: 177.50🪙\n- 343: 239.00🪙\n- 344: 244.00🪙\n- 429: 299.50🪙\n- 514: 355.00🪙\n- 600: 416.50🪙\n- 706: 480.00🪙\n- 792: 541.50🪙\n- 878: 602.00🪙\n- 963: 657.50🪙\n- 1049: 719.00🪙\n- 1135: 779.50🪙\n- 1220: 835.00🪙\n- 1412: 960.00🪙\n- 1584: 1082.00🪙\n- 1755: 1199.00🪙\n- 2195: 1453.00🪙\n- 2901: 1940.00🪙\n- 3688: 2424.00🪙\n- 4390: 2906.00🪙\n- 5532: 3660.00🪙\n- 9288: 6079.00🪙\n- 11483: 7532.00🪙",
        parse_mode='HTML'
    )

async def priceph_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the Mobile Legends price list for Philippines."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return
    await update.message.reply_text(
        "<b>Pack List (FOR PH):</b>\n\n<b>🇵🇭 Philippines:</b>\n\n  - 11: 9.50🪙\n  - 22: 19.00 🪙\n- 33: 28.50🪙\n- 44: 38.00🪙\n  - 56: 47.50🪙\n  - 112: 95.00🪙\n  - 223: 190.00🪙\n  - 336: 285.00🪙\n  - 570: 475.00🪙\n  - 1163: 950.00🪙\n  - 2398: 1900.00🪙\n  - 6042: 4750.00🪙\n  - wdp: 95.00🪙",
        parse_mode='HTML'
    )
async def bigopricebr_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the BIGO price list for Brazil."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return
    price_list = "<b>BIGO Price List (FOR BR):</b>\n\n🇧🇷 <b>Brazil:</b>\n\n"
    for item, data in BIGO_product_info_br.items():
        price_list += f"  - {item}: {data['rate']:.2f}🪙\n"
    await update.message.reply_text(price_list, parse_mode='HTML')
    
async def mcpricebr_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the Magic Chess price list for Brazil."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return
    price_list = "<b>Magic Chess Price List (FOR BR):</b>\n\n🇧🇷 <b>Brazil:</b>\n\n"
    for item, data in magic_chess_product_info_br.items():
        price_list += f"  - {item}: {data['rate']:.2f}🪙\n"
    await update.message.reply_text(price_list, parse_mode='HTML')

async def mcpriceph_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays the Magic Chess price list for Philippines."""
    if await check_bot_status(update): return
    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return
    price_list = "<b>Magic Chess Price List (FOR PH):</b>\n\n🇵🇭 <b>Philippines:</b>\n\n"
    for item, data in magic_chess_product_info_ph.items():
        price_list += f"  - {item}: {data['rate']:.2f}🪙\n"
    await update.message.reply_text(price_list, parse_mode='HTML')

## Order Processing Commands
async def bulk_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, game_type: str, region_override: str = None):
    """Handles bulk order processing."""
    if await check_bot_status(update):
        return

    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return
    if not check_db_ready(update): return
    
    text_after_command = update.message.text.split(' ', 1)[-1]
    
    if game_type == 'bigo':
        pattern = re.findall(r'([a-zA-Z0-9]+)\s+([^\s]+)', text_after_command)
    else:
        pattern = re.findall(r'(\d+)\s*\(?(\d+)\)?\s*([^\s]+)', text_after_command)

    if not pattern:
        if game_type == 'bigo':
            usage_message = 'Usage: `.bigo <player_id> <amount>`.\nExample: `.bigo mi4604 20`'
        else:
            usage_message = f'Usage: `.{update.message.text.split()[0][1:]} <player_id> <zone_id> <product_name>`.\nExample: `.ml 12345678 2222 wp`'
        await update.message.reply_text(usage_message, parse_mode='Markdown')
        return

    order_requests = []
    failed_orders_details_for_report = []
    sender_user_id = str(update.message.from_user.id)

    if region_override:
        region = region_override
    elif game_type == 'magicchessgogo':
        region = 'ph' if "mcggp" in update.message.text else 'br'
    elif game_type == 'bigo':
        region = 'br'
    else:
        region = 'ph' if "mlp" in update.message.text else 'br'

    if game_type == 'bigo':
        parsed_data = [(p[0], None, p[1]) for p in pattern]
    else:
        parsed_data = pattern

    for user_id_str, zone_id, product_field in parsed_data:
        product_names = [p.strip() for p in product_field.split('+') if p.strip()]
        for product_name in product_names:
            
            if game_type == 'magicchessgogo':
                product_info_map = magic_chess_product_info_ph if region == 'ph' else magic_chess_product_info_br
            elif game_type == 'bigo':
                product_info_map = BIGO_product_info_br
            else:
                product_info_map = mlbb_product_info_ph if region == 'ph' else mlbb_product_info_br
            
            balance_type = 'balance_ph' if region == 'ph' else 'balance_br'
            base_url_for_region = SMILE_ONE_BASE_URL_PH if region == 'ph' else SMILE_ONE_BASE_URL_BR

            product = product_info_map.get(product_name)
            if not product:
                failed_orders_details_for_report.append({
                    "user_id": user_id_str, "zone_id": zone_id, "product_name": product_name,
                    "reason": f"Invalid Product Name: '{product_name}'", "is_pack_level_failure": True
                })
                continue

            order_requests.append({
                "user_id": user_id_str, "zone_id": zone_id, "product_name": product_name,
                "product_rate": product["rate"], "product_ids": product['id'] if isinstance(product['id'], list) else [product['id']],
                "region": region, "balance_type": balance_type, "base_url_for_region": base_url_for_region,
                "rate_per_component": product.get("rate_per_component"), "rates_per_component": product.get("rates_per_component")
            })

    if not order_requests:
        if failed_orders_details_for_report:
            error_msg = "No valid orders to process. Details of invalid requests:\n"
            for fail_detail in failed_orders_details_for_report:
                error_msg += f"- User ID: {html.escape(fail_detail['user_id'])}, Product: {html.escape(fail_detail['product_name'])}, Reason: {html.escape(fail_detail['reason'])}\n"
            await update.message.reply_text(error_msg, parse_mode='HTML')
        else:
            await update.message.reply_text("No valid orders to process. Please enter valid parameters.", parse_mode='HTML')
        return

    current_balance_dict = await get_balance(sender_user_id)
    if not current_balance_dict:
        await update.message.reply_text("DB Error: Could not retrieve current balance.", parse_mode='HTML')
        return
        
    balance_type = order_requests[0]['balance_type']
    initial_balance_before_all_deductions = current_balance_dict.get(balance_type, 0)
    total_cost_for_all_valid_orders = sum(order['product_rate'] for order in order_requests)

    if initial_balance_before_all_deductions < total_cost_for_all_valid_orders:
        await update.message.reply_text(
            (f"<code>Insufficient Coins\n\n"
             f"Assets   : {initial_balance_before_all_deductions:.2f} 🪙\n"
             f"Required : {total_cost_for_all_valid_orders:.2f} 🪙</code>"),
            parse_mode='HTML'
        )
        return

    transaction_documents_to_db = []

    for order in order_requests:
        balance_before_this_pack = (await get_balance(sender_user_id)).get(order['balance_type'], 0)
        
        new_balance_after_deduction = await update_balance(sender_user_id, -order['product_rate'], order['balance_type'])
        if new_balance_after_deduction is None:
            failed_orders_details_for_report.append({
                "user_id": order['user_id'], "product_name": order['product_name'],
                "reason": f"Balance deduction failed for {order['product_name']} (DB Error or Insufficient funds)", "is_pack_level_failure": True
            })
            continue

        order_ids_for_this_order_request, success_count, fail_count = [], 0, 0
        refund_amount_for_this_pack = 0.0

        for pid_idx, pid in enumerate(order['product_ids']):
            if pid_idx > 0:
                await asyncio.sleep(1)
            result = await create_order_and_log(order['user_id'], order['zone_id'], pid, order['base_url_for_region'], product_type=game_type)
            order_id = result.get("order_id")

            if not order_id:
                fail_count += 1
                reason_text = result.get("reason", "API Error")
                failed_orders_details_for_report.append({
                    "user_id": order['user_id'], "zone_id": order['zone_id'], "product_name": order['product_name'],
                    "failed_component_id": pid, "reason": html.escape(reason_text)
                })
                skip_refund_reasons = ["Há um problema com a conexão de rede. Por favor, tente novamente!", "Award failure","Server disconnected"]
                if not any(skip.lower() in reason_text.lower() for skip in skip_refund_reasons):
                    if order.get('rate_per_component'):
                        refund_amount_for_this_pack += order['rate_per_component']
                    elif order.get('rates_per_component') and isinstance(order['rates_per_component'], dict):
                        refund_amount_for_this_pack += order['rates_per_component'].get(pid, 0.0)
                    else:
                        refund_amount_for_this_pack += order['product_rate'] / len(order['product_ids'])
            else:
                success_count += 1
                order_ids_for_this_order_request.append(order_id)

        if refund_amount_for_this_pack > 0:
            await update_balance(sender_user_id, refund_amount_for_this_pack, order['balance_type'])

        final_balance_for_log = (await get_balance(sender_user_id)).get(order['balance_type'], new_balance_after_deduction)
        status = "success" if fail_count == 0 else ("partial_success" if success_count > 0 else "failed")
        
        transaction_documents_to_db.append({
            "sender_user_id": sender_user_id, "user_id": order['user_id'], "zone_id": order['zone_id'],
            "product_name": order['product_name'], "order_ids": order_ids_for_this_order_request,
            "date": datetime.now(ZoneInfo("Asia/Yangon")).strftime('%I:%M:%S%p %d.%m.%Y'),
            "total_cost": order['product_rate'], "refunded_amount": refund_amount_for_this_pack,
            "status": status, "initial_balance": balance_before_this_pack, "final_balance": final_balance_for_log,
            "game_type": game_type, "region": order['region'], "success_count": success_count, "fail_count": fail_count
        })

    if transaction_documents_to_db:
        try:
            await order_collection.insert_many(transaction_documents_to_db)
        except (ServerSelectionTimeoutError, OperationFailure) as e:
             logger.error(f"DB Error inserting bulk orders: {e}")
             pass
    
    await generate_report(transaction_documents_to_db, game_type, failed_orders_details_for_report, update)




import html

async def generate_report(transaction_documents_to_db, game_type, failed_orders_details_for_report, update):
    """
    Sends transaction receipts in a clean, aligned monospace format.
    ":" marks are visually aligned and consistent in Telegram <code> blocks.
    """
    nbsp = '\u00A0'  # non-breaking space
    LABEL_WIDTH = 10  # label column width

    # Game name short form
    if game_type == "magicchessgogo":
        game_display_name = "MCGG"
    elif game_type == "bigo":
        game_display_name = "BIGO"
    else:
        game_display_name = "MLBB"

    for transaction in transaction_documents_to_db:
        # Get IGN (username)
        try:
            if game_type == "bigo":
                role_info = await get_role_info(transaction['user_id'], None, product_type=game_type)
            else:
                role_info = await get_role_info(transaction['user_id'], transaction['zone_id'], product_type=game_type)
            ign = html.escape(role_info.get('username', 'N/A')) if role_info and role_info.get('username') else 'N/A'
        except Exception:
            ign = "N/A"

        sender_username = update.message.from_user.username or 'N/A'
        status = transaction['status']
        actual_amount_charged = transaction['total_cost'] - transaction['refunded_amount']
        formatted_order_ids = ' '.join(map(str, transaction['order_ids'])) if transaction['order_ids'] else "N/A"

        # Failure details
        failure_reasons_text = ""
        if transaction['fail_count'] > 0:
            related_fails = [
                f for f in failed_orders_details_for_report
                if f.get('user_id') == transaction['user_id'] and f.get('product_name') == transaction['product_name']
            ]
            if related_fails:
                failure_lines = [
                    f"- {f['reason']} (Component ID: {html.escape(str(f.get('failed_component_id', 'N/A')))})"
                    for f in related_fails
                ]
                failure_reasons_text = "\n\nFailure Reasons:\n" + "\n".join(failure_lines)

        # Main data
        user_id_data = f"{html.escape(str(transaction['user_id']))} ({html.escape(str(transaction['zone_id']))})"
        order_data = f"{html.escape(str(transaction['product_name']))} Diamonds"

        # Helper for aligned label lines
        def line(label, value):
            padding = nbsp * (LABEL_WIDTH - len(label))
            return f"{label}{padding}: {value}"

        # Pre-format money values (avoid nested f-strings)
        initial_text = f"{transaction['initial_balance']:.2f} 🪙"
        spent_text = f"{actual_amount_charged:.2f} 🪙"
        assets_text = f"{transaction['final_balance']:.2f} 🪙"

        # --- SUCCESS CASE ---
        if status == "success" and transaction['fail_count'] == 0:
            report_text = (
                "==== Transaction Report ====\n\n"
                + line("Status", "DONE ✅") + "\n"
                + line("UID", user_id_data) + "\n"
                + line("Name", html.escape(ign)) + "\n"
                + line("Order", order_data) + "\n"
                + line("SN", html.escape(formatted_order_ids)) + "\n"
                + line("Date", html.escape(transaction['date'])) + "\n"
                + f"======= {html.escape(sender_username)} ========\n"
                + line("Initial", initial_text) + "\n"
                + line("Spent", spent_text) + "\n"
                + line("Assets", assets_text) + "\n\n"
                + f"Success {transaction['success_count']} / Fail {transaction['fail_count']}\n"
            )

        # --- PARTIAL or FAILED CASE ---
        else:
            report_status_text = "PARTIAL ⚠️" if status == "partial_success" else "FAILED ❌"
            report_text = (
                "==== Transaction Report ====\n\n"
                + line("Status", report_status_text) + "\n"
                + line("UID", user_id_data) + "\n"
                + line("Name", html.escape(ign)) + "\n"
                + line("Order", order_data) + "\n"
                + line("SN", html.escape(formatted_order_ids)) + "\n"
                + line("Date", html.escape(transaction['date'])) + "\n"
                + f"======= {html.escape(sender_username)} ========\n"
                + line("Initial", initial_text) + "\n"
                + line("Spent", spent_text) + "\n"
                + line("Assets", assets_text) + "\n\n"
                + f"Success {transaction['success_count']} / Fail {transaction['fail_count']}"
                + failure_reasons_text + "\n"
            )

        # Wrap in <code> block for Telegram monospace
        final_message = f"<code>{report_text}</code>"

        # Split long messages if necessary
        for msg_chunk in split_message(final_message):
            await update.message.reply_text(msg_chunk, parse_mode='HTML')
    
async def bulk_command_ph(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for Mobile Legends PH bulk orders."""
    await bulk_command_handler(update, context, 'mobilelegends', region_override='ph')

async def bulk_command_br(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for Mobile Legends BR bulk orders."""
    await bulk_command_handler(update, context, 'mobilelegends', region_override='br')

async def bulk_command_mc_ph(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for Magic Chess PH bulk orders."""
    await bulk_command_handler(update, context, 'magicchessgogo', region_override='ph')

async def bulk_command_mc_br(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for Magic Chess BR bulk orders."""
    await bulk_command_handler(update, context, 'magicchessgogo', region_override='br')

async def bulk_command_bigo_br(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for BIGO BR bulk orders."""
    await bulk_command_handler(update, context, 'bigo', region_override='br')


def check_mlbb_user_info(game_id: str, server_id: str) -> str:
    """MLBB user info ကို miniapp.zone မှစစ်ဆေးပြီး စာသားအဖြစ်ပြန်ပေးသည်။"""
    cookies = { 'XSRF-TOKEN': 'eyJpdiI6IjZHbFZBYll1aDhDOEdZbWtPLytSanc9PSIsInZhbHVlIjoiWlVRZ1NCdnhHYXlmMldZSEdnbkxWaUpiVGp4WEdpR0JDdGltVlU2Y3JOajlIeForMjNFK1cvSC9yTitkb2I5dFREaDlqck02MUJWQWdmOEticjhna3E3alVQb2RHUXhPa2pTUDdSM0U3b3kwV1M1R0Q5ME1UVGhsK2pobTRKeGoiLCJtYWMiOiIxYWM2ODkyNDczODgzNDk4NjQ4ZjM2YjdkZTIxZWIwY2IxMDZmNjJlODdkM2M3MzQyNzA0M2M5ZWU2ZmIzY2RkIiwidGFnIjoiIn0%3D', 'alpha_cloud_session': 'eyJpdiI6ImRaZEJpYmkzbE55blZ6aXZHa0ZFUHc9PSIsInZhbHVlIjoiU1RqWGt5QjgrOWxDVUxUdGdLdDU1MEVFMVZFZ01nTllwTTB6VXBXRDR2R3F1Z3pGM2FmNnJoMUZCS0VzUGRoc3NyTmp6SjJwdVdmQWVjVG9lanFwUmRaOVg1cWQyS01tUkxreFl4M0NMTW1HNFcwNGdCKzhFaGtpWkIrSjQxcVciLCJtYWMiOiJhYWUzZjE5ZWM4MDlmYzZlMDNhN2YzNTJlZDQ0MDM4OWY2YjRlNjA5YjcwMTk2YmY3YTBkYWZmZGMxZDU1MjNjIiwidGFnIjoiIn0%3D' }
    headers = { 'authority': 'miniapp.zone', 'accept': '*/*', 'content-type': 'application/json', 'origin': 'https://miniapp.zone', 'referer': 'https://miniapp.zone/shop/mcgg', 'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36', 'x-csrf-token': 'aK6Bce26DlcHzKV6hv3WKHFSqUucQcPbS1snbfFU', 'x-requested-with': 'XMLHttpRequest' }
    json_data = {'game_id': game_id, 'server_id': server_id, 'game': 'mlbb'}
    region_to_country = {"MM": "Myanmar", "ID": "Indonesia", "PH": "Philippines", "MY": "Malaysia", "SG": "Singapore"}

    try:
        response = requests.post('https://miniapp.zone/name-check', cookies=cookies, headers=headers, json=json_data, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == 200 and 'username' in data:
            username = data.get('username', 'N/A')
            region_code = data.get('region', 'N/A')
            country = region_to_country.get(region_code, region_code)
            output_lines = [
                "<b>××× MLBB User Details ×××</b>",
                f"<b>{'Username':<10}</b> : <code>{html.escape(game_id)}</code>",
                f"<b>{'ZoneID':<10}</b> : <code>{html.escape(server_id)}</code>",
                f"<b>{'Name':<10}</b> : {html.escape(username)}",
                f"<b>{'Region':<10}</b> : {html.escape(region_code)}",
                f"<b>{'Country':<10}</b> : {html.escape(country)}", ""
            ]
            events = data.get('events', [])
            if events:
                output_lines.append("<b>-Double Diamonds on First Recharge-</b>")
                for item in events:
                    title, can_buy = item.get('title', 'N/A'), item.get('game_can_buy', False)
                    status_icon = "✅" if can_buy else "❌"
                    output_lines.append(f"<code>{title:<10}</code> :   {status_icon}")
            return "\n".join(output_lines)
        else:
            return f"<b>Error:</b> Could not retrieve user details: {html.escape(data.get('message', 'Unknown error'))}"
    except requests.exceptions.HTTPError as e:
        return f"<b>An HTTP error occurred:</b> {e}" if e.response.status_code != 419 else "<b>Error:</b> Session Expired (419)."
    except requests.exceptions.RequestException as e:
        return f"<b>A network error occurred:</b> {e}"
    except json.JSONDecodeError:
        return f"<b>Error:</b> Failed to parse server response.\n<pre>{html.escape(response.text)}</pre>"

def check_mcgg_user_info(game_id: str, server_id: str) -> str:
    """Magic Chess user info ကို miniapp.zone မှစစ်ဆေးပြီး စာသားအဖြစ်ပြန်ပေးသည်။"""
    cookies = { 'XSRF-TOKEN': 'eyJpdiI6IjZHbFZBYll1aDhDOEdZbWtPLytSanc9PSIsInZhbHVlIjoiWlVRZ1NCdnhHYXlmMldZSEdnbkxWaUpiVGp4WEdpR0JDdGltVlU2Y3JOajlIeForMjNFK1cvSC9yTitkb2I5dFREaDlqck02MUJWQWdmOEticjhna3E3alVQb2RHUXhPa2pTUDdSM0U3b3kwV1M1R0Q5ME1UVGhsK2pobTRKeGoiLCJtYWMiOiIxYWM2ODkyNDczODgzNDk4NjQ4ZjM2YjdkZTIxZWIwY2IxMDZmNjJlODdkM2M3MzQyNzA0M2M5ZWU2ZmIzY2RkIiwidGFnIjoiIn0%3D', 'alpha_cloud_session': 'eyJpdiI6ImRaZEJpYmkzbE55blZ6aXZHa0ZFUHc9PSIsInZhbHVlIjoiU1RqWGt5QjgrOWxDVUxUdGdLdDU1MEVFMVZFZ01nTllwTTB6VXBXRDR2R3F1Z3pGM2FmNnJoMUZCS0VzUGRoc3NyTmp6SjJwdVdmQWVjVG9lanFwUmRaOVg1cWQyS01tUkxreFl4M0NMTW1HNFcwNGdCKzhFaGtpWkIrSjQxcVciLCJtYWMiOiJhYWUzZjE5ZWM4MDlmYzZlMDNhN2YzNTJlZDQ0MDM4OWY2YjRlNjA5YjcwMTk2YmY3YTBkYWZmZGMxZDU1MjNjIiwidGFnIjoiIn0%3D' }
    headers = { 'authority': 'miniapp.zone', 'accept': '*/*', 'content-type': 'application/json', 'origin': 'https://miniapp.zone', 'referer': 'https://miniapp.zone/shop/mcgg', 'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36', 'x-csrf-token': 'aK6Bce26DlcHzKV6hv3WKHFSqUucQcPbS1snbfFU', 'x-requested-with': 'XMLHttpRequest' }
    json_data = {'game_id': game_id, 'server_id': server_id, 'game': 'mcgg'}
    try:
        response = requests.post('https://miniapp.zone/name-check', cookies=cookies, headers=headers, json=json_data, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == 200 and 'username' in data:
            nickname = data.get('username', 'N/A')
            return (f"<b>****Magic Chess User Details*****</b>\n"
                    f"<b>GameID</b> - <code>{html.escape(game_id)}</code>\n"
                    f"<b>ServerID</b> - <code>{html.escape(server_id)}</code>\n"
                    f"<b>Nickname</b> - {html.escape(nickname)}")
        else:
            return f"<b>Error:</b> ID သို့မဟုတ် Server မှားယွင်းနေပါသည်။\n<pre>{html.escape(json.dumps(data, indent=2))}</pre>"
    except requests.exceptions.HTTPError as e:
        return f"<b>An HTTP error occurred:</b> {e}" if e.response.status_code != 419 else "<b>Error:</b> Session Expired (419)."
    except requests.exceptions.RequestException as e:
        return f"<b>A network error occurred:</b> {e}"
    except json.JSONDecodeError:
        return f"<b>Error:</b> Failed to parse server response.\n<pre>{html.escape(response.text)}</pre>"

def check_pubg_user_info(game_id: str) -> str:
    """PUBG user info ကို miniapp.zone မှစစ်ဆေးပြီး စာသားအဖြစ်ပြန်ပေးသည်။"""
    cookies = { '__cf_bm': 'yACY4oFPOlJmkFUMb_g_097Ae03VCLtS7TrrHd2IkEk-1758359410-1.0.1.1-V53bh9sstDLINj1VHSZlZ_7u3PW3GRnIUA1.vuKVXMif..eJ2asjdf1DCQ9kBWu4Ho8P2xgERLLq5muPVUCfVzqhsm9cgrs_qKn.VEgMWVQ', '_cfuvid': 'oGJS.09yqLybqEzWZQMTJ0xIzICEac1liJCT5i2y7JE-1758359410980-0.0.1.1-604800000', 'XSRF-TOKEN': 'eyJpdiI6IlJBZ04zSWNacS9oVWZwelRXSHcwL0E9PSIsInZhbHVlIjoiZzdjbmFBNS9NbDhVeG5PckRPS09EMDBBMXVnMkllNjhCYWUxNHYvTEt4ZTFnVWZhZ0REZnY5dmlZMVdiYW9KM3BaNlo0T2ttU3BkaW0vMTNXSDd1STBjQitLLzJyV1Rva29Zcnp3L2lLWGZUTFdVZlh1NVNCc2VpYmFGRWJWQU4iLCJtYWMiOiI1MzVmMjQ2ZTgzMTA2ZGE3MGU2ODk4NTU5MWYyNmVkMDcwOTIwMzM0MjI3NjYzMGFmODNmMjVhMDMwMzY2NTBjIiwidGFnIjoiIn0%3D', 'alpha_cloud_session': 'eyJpdiI6ImlReFVOdEdCNjYyWHI4N01mSUx1U0E9PSIsInZhbHVlIjoiUWFhczYrOGswMnhyMGo2NHN5aEx6WUJ4WG8vbUlqOGhXMzhoczBjNnMyYlJ5cTJoN3lJVHgrMHgvMmFTWUdVVFBkcHl4M3FvTDBzeWhPUDNRU2taWWFwRko1NVB4ZHRiTXBrY01ydXFGRnJ3dldLWVlBcnRGR2ZwRFFkaVZ6aysiLCJtYWMiOiJkYmEzYzE4NWI0ZTc4NzdjYzdhZDM3YTAwMTcwOGUwYjQ2Y2NiODgyZThjMTM2ZmIyMDZmN2EyNTE1NTEwM2U3IiwidGFnIjoiIn0%3D' }
    headers = { 'authority': 'miniapp.zone', 'accept': '*/*', 'content-type': 'application/json', 'origin': 'https://miniapp.zone', 'referer': 'https://miniapp.zone/shop/pubg', 'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36', 'x-csrf-token': 'aK6Bce26DlcHzKV6hv3WKHFSqUucQcPbS1snbfFU', 'x-requested-with': 'XMLHttpRequest' }
    json_data = { 'game_id': game_id, 'game': 'pubg' }
    try:
        response = requests.post('https://miniapp.zone/name-check', cookies=cookies, headers=headers, json=json_data, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == 200 and 'username' in data:
            username = data.get('username', 'N/A')
            return (f"<b>****PUBG User Details****</b>\n"
                    f"<b>GameID</b>   - <code>{html.escape(game_id)}</code>\n"
                    f"<b>Nickname</b> - {html.escape(username)}")
        else:
            return f"<b>❌ Error:</b> {html.escape(data.get('message', 'User not found.'))}"
    except requests.exceptions.HTTPError as e:
        return f"<b>An HTTP error occurred:</b> {e}" if e.response.status_code != 419 else "<b>Error:</b> Session Expired (419)."
    except requests.exceptions.RequestException as e:
        return f"<b>A network error occurred:</b> {e}"
    except json.JSONDecodeError:
        return f"<b>Error:</b> Failed to parse server response.\n<pre>{html.escape(response.text)}</pre>"

def check_hok_user_info(game_id: str) -> str:
    """Honor of Kings user info ကို miniapp.zone မှစစ်ဆေးပြီး စာသားအဖြစ်ပြန်ပေးသည်။"""
    cookies = { '__cf_bm': 'yACY4oFPOlJmkFUMb_g_097Ae03VCLtS7TrrHd2IkEk-1758359410-1.0.1.1-V53bh9sstDLINj1VHSZlZ_7u3PW3GRnIUA1.vuKVXMif..eJ2asjdf1DCQ9kBWu4Ho8P2xgERLLq5muPVUCfVzqhsm9cgrs_qKn.VEgMWVQ', '_cfuvid': 'oGJS.09yqLybqEzWZQMTJ0xIzICEac1liJCT5i2y7JE-1758359410980-0.0.1.1-604800000', 'XSRF-TOKEN': 'eyJpdiI6IlJBZ04zSWNacS9oVWZwelRXSHcwL0E9PSIsInZhbHVlIjoiZzdjbmFBNS9NbDhVeG5PckRPS09EMDBBMXVnMkllNjhCYWUxNHYvTEt4ZTFnVWZhZ0REZnY5dmlZMVdiYW9KM3BaNlo0T2ttU3BkaW0vMTNXSDd1STBjQitLLzJyV1Rva29Zcnp3L2lLWGZUTFdVZlh1NVNCc2VpYmFGRWJWQU4iLCJtYWMiOiI1MzVmMjQ2ZTgzMTA2ZGE3MGU2ODk4NTU5MWYyNmVkMDcwOTIwMzM0MjI3NjYzMGFmODNmMjVhMDMwMzY2NTBjIiwidGFnIjoiIn0%3D', 'alpha_cloud_session': 'eyJpdiI6ImlReFVOdEdCNjYyWHI4N01mSUx1U0E9PSIsInZhbHVlIjoiUWFhczYrOGswMnhyMGo2NHN5aEx6WUJ4WG8vbUlqOGhXMzhoczBjNnMyYlJ5cTJoN3lJVHgrMHgvMmFTWUdVVFBkcHl4M3FvTDBzeWhPUDNRU2taWWFwRko1NVB4ZHRiTXBrY01ydXFGRnJ3dldLWVlBcnRGR2ZwRFFkaVZ6aysiLCJtYWMiOiJkYmEzYzE4NWI0ZTc4NzdjYzdhZDM3YTAwMTcwOGUwYjQ2Y2NiODgyZThjMTM2ZmIyMDZmN2EyNTE1NTEwM2U3IiwidGFnIjoiIn0%3D'}
    headers = { 'authority': 'miniapp.zone', 'accept': '*/*', 'content-type': 'application/json', 'origin': 'https://miniapp.zone', 'referer': 'https://miniapp.zone/shop/hok', 'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36', 'x-csrf-token': 'aK6Bce26DlcHzKV6hv3WKHFSqUucQcPbS1snbfFU', 'x-requested-with': 'XMLHttpRequest' }
    json_data = { 'game_id': game_id, 'game': 'hok' }
    try:
        response = requests.post('https://miniapp.zone/name-check', cookies=cookies, headers=headers, json=json_data, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == 200 and 'username' in data:
            username = data.get('username', 'N/A')
            return (f"<b>****Honor of Kings User Details****</b>\n"
                    f"<b>GameID</b>   - <code>{html.escape(game_id)}</code>\n"
                    f"<b>Nickname</b> - {html.escape(username)}")
        else:
            return f"<b>❌ Error:</b> {html.escape(data.get('message', 'User not found.'))}"
    except requests.exceptions.HTTPError as e:
        return f"<b>An HTTP error occurred:</b> {e}" if e.response.status_code != 419 else "<b>Error:</b> Session Expired (419)."
    except requests.exceptions.RequestException as e:
        return f"<b>A network error occurred:</b> {e}"
    except json.JSONDecodeError:
        return f"<b>Error:</b> Failed to parse server response.\n<pre>{html.escape(response.text)}</pre>"

def check_dtf_info(game_id: str) -> str:
    """DTF user info ကို miniapp.zone မှစစ်ဆေးပြီး စာသားအဖြစ်ပြန်ပေးသည်။"""
    cookies = { '_cfuvid': 'oGJS.09yqLybqEzWZQMTJ0xIzICEac1liJCT5i2y7JE-1758359410980-0.0.1.1-604800000', '__cf_bm': 'U2CFtuHAWtCiSVOxpILRwzYWWbneG4Bt9AMP6O1BtNk-1758360655-1.0.1.1-B2tnbrxdFb4hHL8DDgySh1zfvi0Qd2slISH8dvZCwbKyGjhSA02Z8TiPWs0UNuTwEy9Fp8FyWeccrW8er7qrIFEkQCSo7upQWoovKb5W5GA', 'XSRF-TOKEN': 'eyJpdiI6IktnMkFKWXl3NVdaRjZMQTRzb1FKN3c9PSIsInZhbHVlIjoiOVdURS9BSjA2L2c0a2VCNWhsd1hsRjdLT2FQMkVzN1QxSWZ2QXNpekxPcnZESms3dk5aVkJpSzBhdElhdjZ6ZjVXVzMyL1hqWFVFZDlBTnJWNkhDbk1LZUpIWXNsVXg4bUV5U0RoNTRyTHkzcEJxNlBoNVR3UDhReFZkSEpzL1EiLCJtYWMiOiI1MWZiYjM0NzQ1MjU4Y2NhMmEwMTM0NjA3MzFhZTEyMjMyNjIyYTEwMDkzZTQzNjk2MmYwNThkMGIzMjlkNzJmIiwidGFnIjoiIn0%3D', 'alpha_cloud_session': 'eyJpdiI6IjFPZzQySWlMcHFwVkwxTXhBRG1YSkE9PSIsInZhbHVlIjoiQ3VsM1Y0bzVBNGROM0dSU0FMWUk5ditKK0hBcmRoQlZYNERabWNPdWt5TkpldDdoYlFOazgvalllOFgxZm5GUE1ieGh0Y3dLM3RLY2p2MVI1K0p3dmZ4WUlrd0RqZDdNaENGa2FrQmY4MXBGTzJ5M2VMU3lBbFJaNXRjRFpzVGYiLCJtYWMiOiJmOTFiN2IxZmQ1ZTFjMzEzOTQ2YmU1YjVlOGU3MjY2ZWU3NTExZjMwNGVkMDBmN2E1YzBkZTdiOTg4MDJlZDQzIiwidGFnIjoiIn0%3D' }
    headers = { 'authority': 'miniapp.zone', 'accept': '*/*', 'content-type': 'application/json', 'origin': 'https://miniapp.zone', 'referer': 'https://miniapp.zone/shop/dtf', 'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36', 'x-csrf-token': 'aK6Bce26DlcHzKV6hv3WKHFSqUucQcPbS1snbfFU', 'x-requested-with': 'XMLHttpRequest' }
    json_data = { 'game_id': game_id, 'game': 'dtf' }
    try:
        response = requests.post('https://miniapp.zone/name-check', cookies=cookies, headers=headers, json=json_data, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == 200 and 'username' in data:
            username = data.get('username', 'N/A')
            return (f"<b>****DTF User Details******</b>\n"
                    f"<b>GameID</b>    - <code>{html.escape(game_id)}</code>\n"
                    f"<b>Nickname</b> -  {html.escape(username)}")
        else:
            return f"<b>Error:</b> Could not find user. Reason: {html.escape(data.get('message', 'Unknown error'))}"
    except requests.exceptions.HTTPError as e:
        return f"<b>An HTTP error occurred:</b> {e}" if e.response.status_code != 419 else "<b>Error:</b> Session Expired (419)."
    except requests.exceptions.RequestException as e:
        return f"<b>A network error occurred:</b> {e}"
    except json.JSONDecodeError:
        return f"<b>Error:</b> Failed to parse server response.\n<pre>{html.escape(response.text)}</pre>"

## Smile Coin Topup
async def send_smileone_card_request(update: Update, sec_value: str, is_add_coin: bool) -> None:
    """Helper function to send the Smile.one card request and interpret the response."""
    cookies = {
    '_did': 'web_96700016BEC9CA3', 'kwai_uuid': '89663c520542f80e46963922811132ba', '_gcl_au': '1.1.1872372272.1759890080',
    '__cf_bm': '5MfTWVwVucR2AyeNdnmw6tMc9SkuQpjISbUhQdA4zw4-1760402836-1.0.1.1-X5otVp4Ej_TDrafQhZSS4aIJ5AloCCIs1Ts6x9XS645dVsN8ORoGoX6Qq6_H_kqNDarTWaC6X.qMC43hkHYC4.ID.arjh5OkfeKPNXMQLKg',
    'cf_clearance': 'R5HzR6p4afQdwKRQo6178Y9qwX6ZAwXC_4E0nTB1KqQ-1760402903-1.2.1.1-ApObHIB8GGN KHd_eGg3.A7DWCZHwiiVDvyzjVIRJwpyLGPKh8uI_8Ns2QQ8VNM3YqEi7TADG.xEVQjIsFO4J_HqqUrwShX44m0K4sJmzjU7pOUwnA6SNeLlwAiPsQcRyLe7T0kWlxDgyfurUWuxiAz7J_wZYAZTZg87srGoCxW2MKtGObq.uI_x_0v1z2_LHI5qRL5qRL5NQ9If3Sx5jRyGp2QVbw8HntSISuT6x3M84vpU',
    '_gid': 'GA1.2.740483961.1760402904', 'website_path': '9e65e334ddae88a3dada7377dc98538b83b8ca097e874a83e2a97d2f7580a4b4a%3A2%3A%7Bi%3A0%3Bs%3A12%3A%22website_path%22%3Bi%3A1%3Bs%3A4%3A%22%2Fbr%2F%22%3B%7D',
    'PHPSESSID': 'q0ge3r0k5dv589uto4erml8iqc', '_csrf': 'beae509353aa0f1f7ca612288fafb2c8ea39ae8a3e1606cef55d7def1d98f141a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22nvfu73mq9vbpkyKyIpOwiWv8xWTXedK9%22%3B%7D',
    'anonymousId': '1c0c5745fd9e6f7c4eb6a106ef695252e0261c5476611e6ce42ad00e8eef318ca%3A2%3A%7Bi%3A0%3Bs%3A11%3A%22anonymousId%22%3Bi%3A1%3Bs%3A15%3A%22A-68ed9e81a1aab%22%3B%7D',
    '_ga': 'GA1.2.2129679872.1741005224', '_ga_YP87JRK7PM': 'GS2.1.s1760402903$o29$g1$t1760403083$j21$l0$h0',
}
    headers = { 'authority': 'www.smile.one', 'accept': 'application/json, text/javascript, */*; q=0.01', 'accept-language': 'en-US,en;q=0.9,my;q=0.8', 'content-type': 'application/x-www-form-urlencoded; charset=UTF-8', 'origin': 'https://www.smile.one', 'referer': 'https://www.smile.one/customer/recharge', 'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36', 'x-requested-with': 'XMLHttpRequest', }
    data = {'sec': sec_value}
    endpoint_url = 'https://www.smile.one/smilecard/pay/payajax' if is_add_coin else 'https://www.smile.one/smilecard/pay/checkcard'
    action_description = "adding" if is_add_coin else "checking"
    success_message = "✨ **Add Coin Successful!** ✨" if is_add_coin else "**Code valid!**"
    fail_message = "❌ **Code Invalid!**"

    await update.message.reply_text(f"{action_description.capitalize()} 🪙 `{html.escape(sec_value)}`...", parse_mode='Markdown')
    async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
        try:
            async with session.post(endpoint_url, data=data, timeout=15) as response:
                response.raise_for_status()
                response_text = await response.text()
                logger.info(f"Raw response from Smile.one for {sec_value} ({action_description}): {response_text}")
                try:
                    json_response = json.loads(response_text)
                    response_code = json_response.get("code")
                    response_msg = json_response.get("message", "No message provided.")
                    if response_code == 200:
                        if is_add_coin:
                            info_value = json_response.get("info") or json_response.get("amount") or json_response.get("value")
                            await update.message.reply_text(f"{success_message}\n💰 Added: **{html.escape(str(info_value))} coin(s)**", parse_mode='Markdown')
                        else:
                            info_value = json_response.get("info")
                            country_raw = json_response.get("country")
                            country = country_raw.split('\uff08')[0].strip() if country_raw else 'Unknown'
                            await update.message.reply_text(f"{success_message}\nYou have **{html.escape(str(info_value))} coin** in **{html.escape(country)}**.", parse_mode='Markdown')
                    else:
                        await update.message.reply_text(f"{fail_message}\nMessage: {html.escape(response_msg)}", parse_mode='Markdown')
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"Error parsing response for {sec_value} ({action_description}): {e}")
                    await update.message.reply_text(f"Error parsing server response.", parse_mode='Markdown')
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Request error for 'sec' {sec_value} ({action_description}): {e}")
            await update.message.reply_text(f"Failed to send request. Error: {html.escape(str(e))}", parse_mode='Markdown')
        finally:
            user_id = update.effective_user.id
            if user_id in user_data_state:
                del user_data_state[user_id]

async def check_smileone_card(sec_value: str):
    """Checks a Smile.one card and returns its value and country."""
    cookies = {
    '_did': 'web_96700016BEC9CA3', 'kwai_uuid': '89663c520542f80e46963922811132ba', '_gcl_au': '1.1.1872372272.1759890080',
    '__cf_bm': '5MfTWVwVucR2AyeNdnmw6tMc9SkuQpjISbUhQdA4zw4-1760402836-1.0.1.1-X5otVp4Ej_TDrafQhZSS4aIJ5AloCCIs1Ts6x9XS645dVsN8ORoGoX6Qq6_H_kqNDarTWaC6X.qMC43hkHYC4.ID.arjh5OkfeKPNXMQLKg',
    'cf_clearance': 'R5HzR6p4afQdwKRQo6178Y9qwX6ZAwXC_4E0nTB1KqQ-1760402903-1.2.1.1-ApObHIB8GGN KHd_eGg3.A7DWCZHwiiVDvyzjVIRJwpyLGPKh8uI_8Ns2QQ8VNM3YqEi7TADG.xEVQjIsFO4J_HqqUrwShX44m0K4sJmzjU7pOUwnA6SNeLlwAiPsQcRyLe7T0kWlxDgyfurUWuxiAz7J_wZYAZTZg87srGoCxW2MKtGObq.uI_x_0v1z2_LHI5qRL5qRL5NQ9If3Sx5jRyGp2QVbw8HntSISuT6x3M84vpU',
    '_gid': 'GA1.2.740483961.1760402904', 'website_path': '9e65e334ddae88a3dada7377dc98538b83b8ca097e874a83e2a97d2f7580a4b4a%3A2%3A%7Bi%3A0%3Bs%3A12%3A%22website_path%22%3Bi%3A1%3Bs%3A4%3A%22%2Fbr%2F%22%3B%7D',
    'PHPSESSID': 'q0ge3r0k5dv589uto4erml8iqc', '_csrf': 'beae509353aa0f1f7ca612288fafb2c8ea39ae8a3e1606cef55d7def1d98f141a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22nvfu73mq9vbpkyKyIpOwiWv8xWTXedK9%22%3B%7D',
    'anonymousId': '1c0c5745fd9e6f7c4eb6a106ef695252e0261c5476611e6ce42ad00e8eef318ca%3A2%3A%7Bi%3A0%3Bs%3A11%3A%22anonymousId%22%3Bi%3A1%3Bs%3A15%3A%22A-68ed9e81a1aab%22%3B%7D',
    '_ga': 'GA1.2.2129679872.1741005224', '_ga_YP87JRK7PM': 'GS2.1.s1760402903$o29$g1$t1760403083$j21$l0$h0',
}
    headers = { 'authority': 'www.smile.one', 'accept': 'application/json, text/javascript, */*; q=0.01', 'accept-language': 'en-US,en;q=0.9,my;q=0.8', 'content-type': 'application/x-www-form-urlencoded; charset=UTF-8', 'origin': 'https://www.smile.one', 'referer': 'https://www.smile.one/customer/recharge', 'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36', 'x-requested-with': 'XMLHttpRequest', }
    data = {'sec': sec_value}
    endpoint_url = 'https://www.smile.one/smilecard/pay/checkcard'
    async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
        try:
            async with session.post(endpoint_url, data=data, timeout=15) as response:
                response_text = await response.text()
                try:
                    json_response = json.loads(response_text)
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON. Server returned HTML for {sec_value}. Cookies likely expired.")
                    return {"success": False, "message": "Server returned a non-JSON response. Cookies may be expired."}

                if json_response.get("code") == 200:
                    amount = float(json_response.get("info", 0))
                    country_raw = json_response.get("country", "")
                    country = country_raw.split('\uff08')[0].strip() if country_raw else 'Unknown'
                    return {"success": True, "amount": amount, "country": country}
                else:
                    return {"success": False, "message": json_response.get("message", "Invalid code.")}
        except Exception as e:
            logger.error(f"Error checking Smile.one card {sec_value}: {e}")
            return {"success": False, "message": "API request failed."}

async def redeem_smileone_card(sec_value: str):
    """Redeems a Smile.one card."""
    cookies = {
    '_did': 'web_96700016BEC9CA3', 'kwai_uuid': '89663c520542f80e46963922811132ba', '_gcl_au': '1.1.1872372272.1759890080',
    '__cf_bm': '5MfTWVwVucR2AyeNdnmw6tMc9SkuQpjISbUhQdA4zw4-1760402836-1.0.1.1-X5otVp4Ej_TDrafQhZSS4aIJ5AloCCIs1Ts6x9XS645dVsN8ORoGoX6Qq6_H_kqNDarTWaC6X.qMC43hkHYC4.ID.arjh5OkfeKPNXMQLKg',
    'cf_clearance': 'R5HzR6p4afQdwKRQo6178Y9qwX6ZAwXC_4E0nTB1KqQ-1760402903-1.2.1.1-ApObHIB8GGN KHd_eGg3.A7DWCZHwiiVDvyzjVIRJwpyLGPKh8uI_8Ns2QQ8VNM3YqEi7TADG.xEVQjIsFO4J_HqqUrwShX44m0K4sJmzjU7pOUwnA6SNeLlwAiPsQcRyLe7T0kWlxDgyfurUWuxiAz7J_wZYAZTZg87srGoCxW2MKtGObq.uI_x_0v1z2_LHI5qRL5qRL5NQ9If3Sx5jRyGp2QVbw8HntSISuT6x3M84vpU',
    '_gid': 'GA1.2.740483961.1760402904', 'website_path': '9e65e334ddae88a3dada7377dc98538b83b8ca097e874a83e2a97d2f7580a4b4a%3A2%3A%7Bi%3A0%3Bs%3A12%3A%22website_path%22%3Bi%3A1%3Bs%3A4%3A%22%2Fbr%2F%22%3B%7D',
    'PHPSESSID': 'q0ge3r0k5dv589uto4erml8iqc', '_csrf': 'beae509353aa0f1f7ca612288fafb2c8ea39ae8a3e1606cef55d7def1d98f141a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22nvfu73mq9vbpkyKyIpOwiWv8xWTXedK9%22%3B%7D',
    'anonymousId': '1c0c5745fd9e6f7c4eb6a106ef695252e0261c5476611e6ce42ad00e8eef318ca%3A2%3A%7Bi%3A0%3Bs%3A11%3A%22anonymousId%22%3Bi%3A1%3Bs%3A15%3A%22A-68ed9e81a1aab%22%3B%7D',
    '_ga': 'GA1.2.2129679872.1741005224', '_ga_YP87JRK7PM': 'GS2.1.s1760402903$o29$g1$t1760403083$j21$l0$h0',
}
    headers = { 'authority': 'www.smile.one', 'accept': 'application/json, text/javascript, */*; q=0.01', 'accept-language': 'en-US,en;q=0.9,my;q=0.8', 'content-type': 'application/x-www-form-urlencoded; charset=UTF-8', 'origin': 'https://www.smile.one', 'referer': 'https://www.smile.one/customer/recharge', 'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36', 'x-requested-with': 'XMLHttpRequest', }
    data = {'sec': sec_value}
    endpoint_url = 'https://www.smile.one/smilecard/pay/payajax'
    async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
        try:
            async with session.post(endpoint_url, data=data, timeout=15) as response:
                response_text = await response.text()
                try:
                    json_response = json.loads(response_text)
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON. Server returned HTML for {sec_value}. Cookies likely expired.")
                    return {"success": False, "message": "Server returned a non-JSON response. Cookies may be expired."}

                if json_response.get("code") == 200:
                    return {"success": True, "message": "Redeemed successfully."}
                else:
                    return {"success": False, "message": json_response.get("message", "Failed to redeem.")}
        except Exception as e:
            logger.error(f"Error redeeming Smile.one card {sec_value}: {e}")
            return {"success": False, "message": "API request failed."}

# --- ထည့်ပေးရမည့် Admin ID List ---
ADMIN_CHAT_IDS = admins

async def add_coin_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await check_bot_status(update):
        return

    user_id = str(update.message.from_user.id)
    if not await is_registered_user(user_id, update): return

    if not context.args:
        await update.message.reply_text("<b>Usage:</b> .topup &lt;CODE&gt;", parse_mode='HTML')
        return

    sec_value = context.args[0]
    status_msg = await update.message.reply_text(
        f"🔍 Checking code <code>{html.escape(sec_value)}</code>...",
        parse_mode='HTML'
    )

    check_result = await check_smileone_card(sec_value)
    if not check_result["success"]:
        await safe_edit_message(status_msg,
                                f"❌ <b>Check Failed:</b> {html.escape(check_result['message'])}",
                                parse_mode='HTML')
        return

    coin_amount = check_result["amount"]
    country = check_result["country"]
    await safe_edit_message(status_msg,
                            f"✅ Code is valid for <b>{coin_amount} coins</b> in <b>{country}</b>.\n\nNow redeeming...",
                            parse_mode='HTML')

    redeem_result = await redeem_smileone_card(sec_value)
    if not redeem_result["success"]:
        await safe_edit_message(status_msg,
                                f"❌ <b>Redemption Failed:</b> {html.escape(redeem_result['message'])}",
                                parse_mode='HTML')
        return

    await safe_edit_message(status_msg, "✨ Redemption successful!", parse_mode='HTML')

    if coin_amount < 1000:
        fee = coin_amount * 0.002
    else:
        fee = coin_amount * 0.002
    amount_to_add = coin_amount - fee

    if 'brasil' in country.lower():
        balance_type = 'balance_br'
    elif 'philippines' in country.lower():
        balance_type = 'balance_ph'
    else:
        await update.message.reply_text(
            f"⚠️ Redemption was successful, but the country '<b>{html.escape(country)}</b>' "
            f"is not supported for automatic balance top-up. Please contact an admin.",
            parse_mode='HTML'
        )
        return

    new_balance = await update_balance(user_id, amount_to_add, balance_type)

    if new_balance is not None:
        balance_name = "BR Balance" if balance_type == "balance_br" else "PH Balance"

        await safe_edit_message(
            status_msg,
            (f"🎉 <b>Success!</b>\n\n"
             f"<b>Code Amount:</b> <code>{coin_amount:.2f}</code> 🪙\n"
             f"<b>Fee ({'0.2' if coin_amount < 1000 else '0.2'}%):</b> "
             f"<code>-{fee:.2f}</code> 🪙\n"
             f"💰<b>Amount Added:</b> <code>{amount_to_add:.2f}</code> 🪙\n\n"
             f"Your new {balance_name} is now <code>{new_balance:.2f}</code> 🪙."),
            parse_mode='HTML'
        )

        # 🔔 Admin notification
        admin_text = (
            f"🔔 Top-up Alert\n"
            f"User: @{update.effective_user.username or 'NoUsername'}\n"
            f"UserID: {user_id}\n"
            f"Added: {amount_to_add:.2f} 🪙 "
            f"(Fee {'0.2' if coin_amount < 1000 else '0.2'}%)\n"
            f"Country: {country}\n"
            f"New {balance_name}: {new_balance:.2f} 🪙"
        )
        for admin_id in ADMIN_CHAT_IDS:
            await context.bot.send_message(chat_id=admin_id, text=admin_text)
        return

    await safe_edit_message(
        status_msg,
        "⚠️ Redemption was successful, but there was an error updating your balance. Please contact an admin.",
        parse_mode='HTML'
    )

## Admin Specific Commands
# 🚨 ပြင်ဆင်ထားသော admin_command (Admin Command Helper)
async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Displays a list of commands available to admins."""
    if not is_admin(update.message.from_user.id): return
    global is_bot_paused
    status_text = "PAUSED ⏸️" if is_bot_paused else "RUNNING ▶️"
    await update.message.reply_text(
        f"<b>Hello Admin</b> {html.escape(str(update.message.from_user.username if update.message.from_user.username else 'User'))}\n"
        f"<b>Bot Status: {status_text}</b>\n\n"
        "<b>--- 1. Admin & Report Commands ---</b>\n"
        "<code>.baladmin</code>  - Check SmileOne Balances & Today's Summary\n"
        "<code>.userspend</code> - Check All User Spending (Today)\n"
        "<code>.baldate</code> &lt;DD.MM.YYYY&gt; - Check Usage for specific date\n"
        "<code>.balorder</code> &lt;user&gt; [DD.MM.YYYY] - User's Orders/Spending Report\n"
        "<code>.his</code> [DD.MM.YYYY] &lt;gameid&gt;(&lt;zoneid&gt;) - Check Order by PlayerID/Date\n"
        "<code>.checkid</code> &lt;uid&gt;(&lt;zoneid&gt;) - Check today's orders for specific PlayerID/ZoneID\n"
        "<code>.allhis</code>  - All Order History\n"
        "<code>.user</code>    - List All Users & Balances\n\n"
        "<b>--- 2. User/Wallet Management ---</b>\n"
        "<code>.angel</code> &lt;user&gt; - Register User (Grant Full Access)\n"
        "<code>.removeuser</code> &lt;user&gt; - Remove user from DB\n"
        "<code>.addbal</code> &lt;id&gt; &lt;amount&gt; &lt;balance_ph|balance_br&gt; - Add Balance\n"
        "<code>.dedbal</code> &lt;id&gt; &lt;amount&gt; &lt;balance_ph|balance_br&gt; - Deduct Balance\n\n"
        "<b>--- 3. Smile Coin Management ---</b>\n"
        "<code>.checkcoin</code> &lt;sec_value&gt; - Check Smile.one Card Balance\n"
        "<code>.addcoin</code> &lt;sec_value&gt; - Add Smile.one Coin (to linked account)\n\n"
        "<b>--- 4. Bot Control ---</b>\n"
        "<code>.pausebot</code>   - Pause all general user operations\n"
        "<code>.unpausebot</code> - Unpause bot operations",
        parse_mode='HTML'
    )

async def register_user_by_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: Registers a user by adding the 'date_joined' field."""
    if not is_admin(update.message.from_user.id): 
        await update.message.reply_text("You are not registered to use this command. Please ask an admin to register you.🤵", parse_mode='HTML'); return
    if not check_db_ready(update): return
    
    if len(context.args) != 1:
        await update.message.reply_text("*Usage*: `.angel <user_id_or_username>`", parse_mode='Markdown'); return
    
    identifier = context.args[0]
    target_user_id, display_name = await resolve_user_identifier(identifier)
    
    if not target_user_id:
        await update.message.reply_text(f"❌ *Error*: Target user ID for <b>{html.escape(identifier)}</b> not found. Ask them to .start the bot first.", parse_mode='HTML'); return
    
    try:
        existing_user = await users_collection.find_one({"user_id": target_user_id})

        if not existing_user:
            username_to_save = display_name.lstrip('@') if display_name.startswith('@') else None
            await users_collection.insert_one({"user_id": target_user_id, "username": username_to_save, "balance_ph": 0, "balance_br": 0})
            existing_user = await users_collection.find_one({"user_id": target_user_id})

        if existing_user.get('date_joined'):
            joined_date = datetime.fromtimestamp(existing_user['date_joined'], ZoneInfo("Asia/Yangon")).strftime('%d.%m.%Y')
            await update.message.reply_text(f"✅ User <b>{html.escape(display_name)}</b> is already registered since {joined_date}.", parse_mode='HTML'); return
        
        await users_collection.update_one(
            {"user_id": target_user_id},
            {"$set": {"date_joined": int(time.time())}}
        )

        await update.message.reply_text(f"🎉 User <b>{html.escape(display_name)}</b> (<code>{html.escape(target_user_id)}</code>) has been **SUCCESSFULLY REGISTERED**.", parse_mode='HTML')
        try:
            await context.bot.send_message(chat_id=int(target_user_id), text="🎉 Congratulations! An admin has registered you. You can now use the bot's full features.\nPress .help for commands.", parse_mode='HTML')
        except Exception as e:
            logger.warning(f"Could not send welcome message to new user {target_user_id}: {e}")
            
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during registration: {e}")
        await update.message.reply_text("Database connection failed during registration. Please try again later.", parse_mode='HTML')


async def remove_user_by_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Removes a user by admin. Can take Telegram ID or username."""
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return
    
    if len(context.args) != 1:
        await update.message.reply_text("*Usage*: `.removeuser <user_id_or_username>`", parse_mode='Markdown'); return
    identifier = context.args[0]
    target_user_id, display_name = await resolve_user_identifier(identifier)
    if not target_user_id:
        await update.message.reply_text(f"❌ *User Not Found*: <b>{html.escape(identifier)}</b>.", parse_mode='HTML'); return
    
    try:
        result = await users_collection.delete_one({"user_id": target_user_id})
        if result.deleted_count > 0:
            await update.message.reply_text(f"🗑️ User <b>{html.escape(display_name)}</b> has been removed.", parse_mode='HTML')
            try:
                await context.bot.send_message(chat_id=int(target_user_id), text="🚫 You have been removed from the bot by an admin. You can no longer use most commands.", parse_mode='HTML')
            except Exception as e:
                logger.warning(f"Could not send removal notification to user {target_user_id}: {e}")
        else:
            await update.message.reply_text(f"❌ User <b>{html.escape(display_name)}</b> was not found.", parse_mode='HTML')
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during user removal: {e}")
        await update.message.reply_text("Database connection failed during removal. Please try again later.", parse_mode='HTML')


async def add_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Adds balance to a user's account."""
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return
    
    if len(context.args) != 3 or not context.args[1].replace('.', '', 1).isdigit() or context.args[2] not in ['balance_ph', 'balance_br']:
        await update.message.reply_text("*Usage*: `.addbal <user> <amount> <balance_ph|balance_br>`", parse_mode='Markdown'); return
    identifier, amount, balance_type = context.args[0], float(context.args[1]), context.args[2]
    target_user_id, display_name = await resolve_user_identifier(identifier)
    if not target_user_id or not await users_collection.find_one({"user_id": target_user_id}):
        await update.message.reply_text(f"❌ User not found or not registered.", parse_mode='HTML'); return
    
    new_balance = await update_balance(target_user_id, amount, balance_type)
    
    if new_balance is not None:
        await update.message.reply_text(f"Successfully Added <code>{amount:.2f}</code>🪙to <b>{html.escape(display_name)}</b>'s {html.escape(balance_type.replace('balance_', '').upper())} balance.\nNew Balance: <code>{new_balance:.2f}</code>🪙", parse_mode='HTML')
        try:
            await context.bot.send_message(chat_id=int(target_user_id), text=f"<code>Successful Deposit : {amount:.2f} 🪙\nYour New Coins  : {new_balance:.2f} 🪙</code>", parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error sending balance notification to user {target_user_id}: {e}")
    else:
        await update.message.reply_text(f"❌ Failed to update balance for <b>{html.escape(display_name)}</b>.", parse_mode='HTML')

async def deduct_balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Deducts balance from a user's account."""
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return
    
    if len(context.args) != 3 or not context.args[1].replace('.', '', 1).isdigit() or context.args[2] not in ['balance_ph', 'balance_br']:
        await update.message.reply_text("*Usage*: `.dedbal <user> <amount> <balance_ph|balance_br>`", parse_mode='Markdown'); return
    identifier, amount, balance_type = context.args[0], float(context.args[1]), context.args[2]
    target_user_id, display_name = await resolve_user_identifier(identifier)
    if not target_user_id or not await users_collection.find_one({"user_id": target_user_id}):
        await update.message.reply_text(f"❌ User not found or not registered.", parse_mode='HTML'); return
    
    new_balance = await update_balance(target_user_id, -amount, balance_type)
    
    if new_balance is not None:
        await update.message.reply_text(f"✅ Deducted <code>{amount:.2f}</code> from <b>{html.escape(display_name)}</b>'s {html.escape(balance_type.replace('balance_', '').upper())} balance.\nNew Balance: <code>{new_balance:.2f}</code>", parse_mode='HTML')
    else:
        await update.message.reply_text(f"❌ Failed to deduct balance for <b>{html.escape(display_name)}</b> (insufficient funds or DB error).", parse_mode='HTML')


async def get_users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lists all registered users with their balances and join dates."""
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return
    
    try:
        users_list = await users_collection.find().to_list(length=None)
        if not users_list:
            await update.message.reply_text("No users found.")
            return
        await update.message.reply_text("<b>User Details:</b> 📋", parse_mode='HTML')
        message_parts = []
        for user in users_list:
            user_id_str = str(user.get('user_id', 'N/A'))
            display_name = f"@{user['username']}" if user.get('username') else user_id_str
            date_joined = datetime.fromtimestamp(user.get('date_joined', 0), ZoneInfo("Asia/Yangon")).strftime('%d.%m.%Y') if user.get('date_joined') else 'N/A (Unregistered)'
            user_entry = (f"\n---------------------------------\n"
                        f"🆔 <b>User:</b> {html.escape(display_name)} (<code>{html.escape(user_id_str)}</code>)\n"
                        f"🇵🇭 <b>PH Bal:</b> ${user.get('balance_ph', 0):.2f}\n"
                        f"🇧🇷 <b>BR Bal:</b> ${user.get('balance_br', 0):.2f}\n"
                        f"📅 <b>Joined:</b> {html.escape(date_joined)}\n")
            message_parts.append(user_entry)
        full_message = "".join(message_parts)
        for msg_part in split_message(full_message):
            await update.message.reply_text(msg_part, parse_mode='HTML')
            
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during get_users_command: {e}")
        await update.message.reply_text("Database connection failed while fetching user list. Please try again later.", parse_mode='HTML')


async def get_all_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Retrieves and displays the full order history for all users (admin only)."""
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return
    
    try:
        orders_list = await order_collection.find({}).to_list(length=None)
        if not orders_list:
            await update.message.reply_text("No orders found."); return
        
        response_summary_parts = ["==== All Order Histories ====\n\n"]
        for order in orders_list:
            sender_db_entry = await users_collection.find_one({"user_id": str(order.get('sender_user_id'))})
            sender_display_name = f"@{sender_db_entry['username']}" if sender_db_entry and sender_db_entry.get('username') else order.get('sender_user_id', 'N/A')
            order_ids = order.get('order_ids', [])
            order_ids_str = ', '.join(map(str, order_ids)) if isinstance(order_ids, list) else str(order_ids)
            response_summary_parts.append(
                (f"🆔 Sender: <b>{html.escape(sender_display_name)}</b>\n"
                f"Player ID: <code>{html.escape(str(order.get('user_id')))}</code>\n"
                f"💎 Product: {html.escape(str(order.get('product_name')))}\n"
                f"🆔 Order IDs: <code>{html.escape(order_ids_str)}</code>\n"
                f"📅 Date: {html.escape(order.get('date'))}\n"
                f"💵 Cost: ${float(order.get('total_cost', 0.0)):.2f}\n"
                f"🔄 Status: {html.escape(str(order.get('status')))}\n\n")
            )
        full_response_summary = "".join(response_summary_parts)
        for msg in split_message(full_response_summary):
            await update.message.reply_text(msg, parse_mode='HTML')
            
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during get_all_orders: {e}")
        await update.message.reply_text("Database connection failed while fetching all orders. Please try again later.", parse_mode='HTML')

async def query_point_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command to show Smile.one balances + today's summary."""
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return
    
    try:
        report_date_str = datetime.now(ZoneInfo("Asia/Yangon")).strftime('%d.%m.%Y')
        loading_message = await update.message.reply_text(f"📊 Generating report for <b>{html.escape(report_date_str)}</b>...", parse_mode="HTML")
        smile_one_balances = await get_smile_one_balances()
        all_user_balance_pipeline = [{"$group": {"_id": None, "total_ph": {"$sum": "$balance_ph"}, "total_br": {"$sum": "$balance_br"}}}]
        total_balances_result = await users_collection.aggregate(all_user_balance_pipeline).to_list(length=1)
        all_users_total_ph = total_balances_result[0].get("total_ph", 0.0) if total_balances_result else 0.0
        all_users_total_br = total_balances_result[0].get("total_br", 0.0) if total_balances_result else 0.0
        summary_pipeline = [
            {"$match": {"date": {"$regex": f".*{report_date_str}$"}}},
            {"$group": {"_id": None,
                        "total_spent_ph": {"$sum": {"$cond": [{"$eq": ["$region", "ph"]}, {"$subtract": ["$total_cost", "$refunded_amount"]}, 0]}},
                        "total_spent_br": {"$sum": {"$cond": [{"$eq": ["$region", "br"]}, {"$subtract": ["$total_cost", "$refunded_amount"]}, 0]}},
                        "total_success": {"$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}},
                        "total_partial_success": {"$sum": {"$cond": [{"$eq": ["$status", "partial_success"]}, 1, 0]}},
                        "total_fail": {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                        "unique_users": {"$addToSet": "$sender_user_id"}}},
            {"$project": {"_id": 0, "total_spent_ph": 1, "total_spent_br": 1, "total_success": 1, "total_fail": 1, "total_partial_success": 1, "users_served": {"$size": "$unique_users"}}}
        ]
        summary_result = await order_collection.aggregate(summary_pipeline).to_list(length=1)
        data = summary_result[0] if summary_result else {}
        response_message = (
            f"<b>📊 Summary for {html.escape(report_date_str)}</b>:\n\n"
            f"<b>--- Smile One Balance (Current) ---</b>\n"
            f"🇵🇭 <b>MLBB PH:</b> {html.escape(str(smile_one_balances['ph_mlbb']))}\n"
            f"🇧🇷 <b>MLBB BR:</b> {html.escape(str(smile_one_balances['br_mlbb']))}\n\n"
            f"<b>--- All User Wallet Summary ---</b>\n"
            f"🇵🇭 <b>Total User PH Balance:</b> ${all_users_total_ph:.2f} 🪙\n"
            f"🇧🇷 <b>Total User BR Balance:</b> ${all_users_total_br:.2f} 🪙\n\n"
            f"<b>--- Daily Spending Summary (Actual Cost) ---</b>\n"
            f"👥 Users Served: {data.get('users_served', 0)}\n"
            f"🇵🇭 Total Spent (PH): ${data.get('total_spent_ph', 0.0):.2f} 🪙\n"
            f"🇧🇷 Total Spent (BR): ${data.get('total_spent_br', 0.0):.2f} 🪙\n\n"
            f"<b>--- Daily Order Summary ---</b>\n"
            f"✅ Success Orders: {data.get('total_success', 0)}\n"
            f"⚠️ Partial Success Orders: {data.get('total_partial_success', 0)}\n"
            f"❌ Failed Orders: {data.get('total_fail', 0)}\n"
            f"📋 Total Orders: {sum(data.get(k, 0) for k in ['total_success', 'total_partial_success', 'total_fail'])}"
        )
        await safe_edit_message(loading_message, response_message, parse_mode='HTML')
        
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during query_point_command: {e}")
        await loading_message.edit_text("Database connection failed while generating summary. Please try again later.", parse_mode='HTML')


async def user_spend_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generates a detailed report of individual user spending for a specific date or today."""
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return

    try:
        report_date_str = None
        if context.args:
            report_date_str = context.args[0]
            try: datetime.strptime(report_date_str, '%d.%m.%Y')
            except ValueError:
                await update.message.reply_text("⚠️ Invalid date format. Use DD.MM.YYYY.", parse_mode='Markdown'); return
        else:
            report_date_str = datetime.now(ZoneInfo("Asia/Yangon")).strftime('%d.%m.%Y')
        loading_message = await update.message.reply_text(f"📊 Generating report for <b>{html.escape(report_date_str)}</b>...", parse_mode='HTML')
        pipeline = [
            {'$match': {'date': {'$regex': f'.*{re.escape(report_date_str)}$'}}},
            {'$group': {'_id': '$sender_user_id',
                        'ph_spent': {'$sum': {'$cond': [{'$eq': ['$region', 'ph']}, {'$subtract': ['$total_cost', '$refunded_amount']}, 0]}},
                        'br_spent': {'$sum': {'$cond': [{'$eq': ['$region', 'br']}, {'$subtract': ['$total_cost', '$refunded_amount']}, 0]}},
                        'total_success': {'$sum': {'$cond': [{'$eq': ['$status', 'success']}, 1, 0]}},
                        'total_partial_success': {'$sum': {'$cond': [{'$eq': ['$status', 'partial_success']}, 1, 0]}},
                        'total_fail': {'$sum': {'$cond': [{'$eq': ['$status', 'failed']}, 1, 0]}}}},
            {'$lookup': {'from': 'user', 'localField': '_id', 'foreignField': 'user_id', 'as': 'userInfo'}},
            {'$unwind': {'path': '$userInfo', 'preserveNullAndEmptyArrays': True}}
        ]
        active_users_data = {res['_id']: res for res in await order_collection.aggregate(pipeline).to_list(length=None)}
        all_users_list = await users_collection.find({}).to_list(length=None)
        report_content_parts = [f"📊 <b>User Spending Report for {html.escape(report_date_str)}</b> 📊\n\n"]
        found_active_user = False
        for user_data in all_users_list:
            user_id_str = user_data.get('user_id')
            ph_spent, br_spent, s, ps, f = 0.0, 0.0, 0, 0, 0
            if user_id_str in active_users_data:
                activity = active_users_data[user_id_str]
                ph_spent, br_spent = activity.get('ph_spent', 0.0), activity.get('br_spent', 0.0)
                s, ps, f = activity.get('total_success', 0), activity.get('total_partial_success', 0), activity.get('total_fail', 0)
            current_balance_ph, current_balance_br = user_data.get('balance_ph', 0.0), user_data.get('balance_br', 0.0)
            if (ph_spent + br_spent) > 0 or current_balance_ph > 0 or current_balance_br > 0:
                found_active_user = True
                display_name = f"@{user_data['username']}" if user_data.get('username') else f"ID: {user_id_str}"
                report_content_parts.append(
                    f"👤 <b>User:</b> {html.escape(display_name)} (<code>{html.escape(user_id_str)}</code>)\n"
                    f"  💸 Spent PH: ${ph_spent:.2f} | BR: ${br_spent:.2f} (Today's Actual)\n"
                    f"  📈 Orders: ✅ {s} / ⚠️ {ps} / ❌ {f} / 📋 {s + ps + f} Total\n"
                    f"  💰 Current Balance: PH ${current_balance_ph:.2f} | BR ${current_balance_br:.2f}\n"
                    f"---------------------------------\n"
                )
        if not found_active_user: report_content_parts.append("No spending or active balances found for any user on this date.\n")
        full_report_content = "".join(report_content_parts)
        await loading_message.delete()
        for msg_part in split_message(full_report_content):
            await update.message.reply_text(msg_part, parse_mode='HTML')
            
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during user_spend_report_command: {e}")
        await loading_message.edit_text("Database connection failed while generating report. Please try again later.", parse_mode='HTML')


async def balance_order_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin command to get a specific user's full order history, spending, 
    and current balance for a specific date or today.
    Usage: .balorder <user_id_or_username> [DD.MM.YYYY]
    """
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return
    
    try:
        if not context.args:
            await update.message.reply_text("<b>Usage:</b> .balorder &lt;user_id_or_username&gt; [DD.MM.YYYY]", parse_mode='HTML')
            return

        identifier = context.args[0]
        
        if len(context.args) > 1:
            report_date_str = context.args[1]
            try:
                datetime.strptime(report_date_str, '%d.%m.%Y')
            except ValueError:
                await update.message.reply_text("❌ Invalid date format. Use DD.MM.YYYY.", parse_mode='HTML')
                return
        else:
            report_date_str = datetime.now(ZoneInfo("Asia/Yangon")).strftime('%d.%m.%Y')

        loading_message = await update.message.reply_text(
            f"Searching orders for <b>{html.escape(identifier)}</b> on <b>{html.escape(report_date_str)}</b>...", 
            parse_mode='HTML'
        )

        target_user_id, display_name = await resolve_user_identifier(identifier)
        if not target_user_id:
            await loading_message.edit_text(f"❌ User <b>{html.escape(identifier)}</b> not found in the database.", parse_mode='HTML'); return
        
        user_doc = await users_collection.find_one({"user_id": target_user_id})
        current_ph_balance = user_doc.get('balance_ph', 0.0) if user_doc else 0.0
        current_br_balance = user_doc.get('balance_br', 0.0) if user_doc else 0.0
        
        orders_list = await order_collection.find(
            {"sender_user_id": target_user_id, "date": {'$regex': f'.*{re.escape(report_date_str)}$'}}
        ).sort("date", 1).to_list(length=None)
        
        total_s, total_ps, total_f, spent_ph, spent_br = 0, 0, 0, 0.0, 0.0
        history_lines = []
        if not orders_list:
            history_lines.append(f"\nNo orders found for {html.escape(report_date_str)}.")

        for order in orders_list:
            status = order.get('status')
            if status == 'success': total_s += 1
            elif status == 'partial_success': total_ps += 1
            elif status == 'failed': total_f += 1
            
            actual_paid = order.get('total_cost', 0.0) - order.get('refunded_amount', 0.0)
            if order.get('region') == 'ph': spent_ph += actual_paid
            elif order.get('region') == 'br': spent_br += actual_paid

            order_ids_str = ', '.join([f"<code>{html.escape(str(oid))}</code>" for oid in order.get('order_ids', [])])
            history_lines.append(
                f"---------------------------------\n"
                f"<b>Item:</b> {html.escape(str(order.get('product_name', 'N/A')))} ({html.escape(order.get('region', 'N/A').upper())})\n"
                f"<b>Game ID:</b> <code>{html.escape(str(order.get('user_id', 'N/A')))} ({html.escape(str(order.get('zone_id', 'N/A')))})</code>\n"
                f"<b>Charged:</b> ${actual_paid:.2f}\n"
                f"<b>Status:</b> {html.escape(str(status))}\n"
                f"<b>Order IDs:</b> {order_ids_str}\n"
                f"<b>Time:</b> {html.escape(order.get('date', 'N/A').split(' ')[0])}\n"
            )
        
        final_report = (
            f"📊 <b>Report for {html.escape(display_name)}</b> 📊\n"
            f"<i>Date: {html.escape(report_date_str)}</i>\n\n"
            f"<b>--- Balance & Spending ---</b>\n"
            f"💰 Current PH Bal: <code>${current_ph_balance:.2f}</code> | Spent PH: <code>${spent_ph:.2f}</code>\n"
            f"💰 Current BR Bal: <code>${current_br_balance:.2f}</code> | Spent BR: <code>${spent_br:.2f}</code>\n\n"
            f"<b>--- Order Summary ---</b>\n"
            f"✅ Success: {total_s} | ⚠️ Partial: {total_ps} | ❌ Failed: {total_f} | 📋 Total: {len(orders_list)}\n\n"
            f"<b>--- Full Order History ---</b>" + "".join(history_lines)
        )
        
        await loading_message.delete()
        for msg_part in split_message(final_report):
            await update.message.reply_text(msg_part, parse_mode='HTML')
            
    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during balance_order_command: {e}")
        await loading_message.edit_text("Database connection failed while generating report. Please try again later.", parse_mode='HTML')


# 🚨 ပြင်ဆင်ထားသော get_order_by_player_id_command function (Date argument 3 ခုလုံး လက်ခံ + Combined ID ပုံစံ လက်ခံ)
async def get_order_by_player_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin only: Retrieves order history for a specific Game ID, Zone ID, and optional Date.
    Usage: .his [DD.MM.YYYY] <gameid> <zoneid> or .his <gameid>(<zoneid>) [DD.MM.YYYY]
    """
    if not is_admin(update.message.from_user.id): return
    if not check_db_ready(update): return

    args = context.args
    
    game_id, zone_id, report_date_str = None, None, None
    
    # Argument Parsing Logic

    # Check for <date> <id>(<zone>) format (2 arguments)
    if len(args) == 2 and '(' in args[1]:
        combined_id_match = re.match(r'(\d+)\s*\((?:\s*|)(\d+)(?:\s*|)\)', args[1])
        if combined_id_match:
            report_date_str = args[0]
            game_id, zone_id = combined_id_match.groups()
        else:
            await update.message.reply_text("❌ Invalid combined ID format. Expected: <gameid>(<zoneid>)", parse_mode='HTML')
            return

    # Check for <id>(<zone>) format (1 argument - Today Search)
    elif len(args) == 1 and '(' in args[0]:
        combined_id_match = re.match(r'(\d+)\s*\((?:\s*|)(\d+)(?:\s*|)\)', args[0])
        if combined_id_match:
            game_id, zone_id = combined_id_match.groups()
            report_date_str = datetime.now(ZoneInfo("Asia/Yangon")).strftime('%d.%m.%Y')
        else:
            await update.message.reply_text("❌ Invalid combined ID format. Expected: <gameid>(<zoneid>)", parse_mode='HTML')
            return

    # Check for <date> <gameid> <zoneid> format (3 arguments)
    elif len(args) == 3:
        report_date_str = args[0]
        game_id = args[1]
        zone_id = args[2]

    # Check for <gameid> <zoneid> format (2 arguments - Today Search)
    elif len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        game_id = args[0]
        zone_id = args[1]
        report_date_str = datetime.now(ZoneInfo("Asia/Yangon")).strftime('%d.%m.%Y')

    else:
        # Fallback to enhanced_get_user_orders if no admin-specific format matched
        # This covers cases like ".his today" if admin accidentally used the admin router.
        await enhanced_get_user_orders(update, context) 
        return

    # ID validation (Numeric check)
    if not (game_id.isdigit() and zone_id.isdigit()):
        await update.message.reply_text("❌ Invalid arguments. Game ID and Zone ID must be numeric.", parse_mode='HTML')
        return
        
    # Date validation
    if report_date_str:
        try:
            datetime.strptime(report_date_str, '%d.%m.%Y')
        except ValueError:
            await update.message.reply_text("❌ Invalid date format. Use DD.MM.YYYY.", parse_mode='HTML')
            return

    loading_message = await update.message.reply_text(
        f"🔍 Searching orders for Game ID <code>{html.escape(game_id)}</code> ({html.escape(zone_id)}) on <b>{report_date_str}</b>...", 
        parse_mode='HTML'
    )

    try:
        # MongoDB query to find orders matching game_id, zone_id, and date
        orders_cursor = order_collection.find({
            "user_id": game_id,
            "zone_id": zone_id,
            "date": {"$regex": f".*{re.escape(report_date_str)}$"}
        }).sort([("date", 1)])

        orders_list = await orders_cursor.to_list(length=None)
        
        if not orders_list:
            await safe_edit_message(
                loading_message,
                f"❌ No orders found for Game ID <code>{html.escape(game_id)}</code> ({html.escape(zone_id)}) on <b>{report_date_str}</b>.",
                parse_mode='HTML'
            )
            return

        response_summary_parts = [f"==== Orders for {html.escape(game_id)} ({html.escape(zone_id)}) on {report_date_str} ====\n"]
        total_spent = 0.0

        for order in orders_list:
            sender_id = order.get('sender_user_id', 'N/A')
            sender_db = await users_collection.find_one({"user_id": str(sender_id)})
            sender_name = f"@{sender_db['username']}" if sender_db and sender_db.get('username') else sender_id
            
            order_ids = order.get('order_ids', [])
            order_ids_str = ', '.join(map(str, order_ids)) if isinstance(order_ids, list) else str(order_ids)
            
            actual_cost = float(order.get('total_cost', 0.0)) - float(order.get('refunded_amount', 0.0))
            total_spent += actual_cost

            response_summary_parts.append(
                f"👤 Sender: <b>{html.escape(sender_name)}</b> (ID: <code>{html.escape(str(sender_id))}</code>)\n"
                f"💎 Pack: {html.escape(str(order.get('product_name', 'N/A')))} ({html.escape(order.get('region', 'N/A').upper())})\n"
                f"💵 Cost: ${actual_cost:.2f} (Total Cost: ${float(order.get('total_cost', 0.0)):.2f})\n"
                f"🆔 Order ID: <code>{html.escape(order_ids_str)}</code>\n"
                f"🔄 Status: <b>{html.escape(str(order.get('status', 'N/A')).upper())}</b>\n"
                f"⌚ Time: {html.escape(order.get('date', 'N/A').split(' ')[0])}\n"
                f"---------------------------------\n"
            )
        
        response_summary_parts.append(f"💰 <b>Total Actual Spent on {report_date_str}:</b> ${total_spent:.2f} 🪙\n")

        full_response_summary = "".join(response_summary_parts)
        
        await loading_message.delete()
        for msg in split_message(full_response_summary):
            await update.message.reply_text(msg, parse_mode='HTML')

    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during get_order_by_player_id_command: {e}")
        await safe_edit_message(loading_message, "Database connection failed while fetching orders. Please try again later.", parse_mode='HTML')


async def pause_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to pause bot operations for general users."""
    if not is_admin(update.effective_user.id): return
    global is_bot_paused
    if is_bot_paused:
        await update.message.reply_text("⏸️ Bot is already paused.", parse_mode='HTML')
    else:
        is_bot_paused = True
        logger.info(f"Admin {update.effective_user.id} paused bot operations.")
        await update.message.reply_text("⏸️ Bot operations are now paused for users. Admin commands are still available.", parse_mode='HTML')

async def unpause_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to unpause bot operations for general users."""
    if not is_admin(update.effective_user.id): return
    global is_bot_paused
    if not is_bot_paused:
        await update.message.reply_text("▶️ Bot is already running.", parse_mode='HTML')
    else:
        is_bot_paused = False
        logger.info(f"Admin {update.effective_user.id} unpaused bot operations.")
        await update.message.reply_text("▶️ Bot operations are now unpaused. Users can use commands again.", parse_mode='HTML')

async def check_coin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to check a Smile.one card. Prompts for a code if not provided."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Unauthorized: This command is for admins only.")
        return

    if context.args:
        sec_value = context.args[0]
        await send_smileone_card_request(update, sec_value, is_add_coin=False)
    else:
        user_data_state[user_id] = "waiting_for_sec_check"
        await update.message.reply_text('Please send the "sec" value to check now.')

async def add_coin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin command to redeem a Smile.one card. Prompts for a code if not provided."""
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Unauthorized: This command is for admins only.")
        return

    if context.args:
        sec_value = context.args[0]
        await send_smileone_card_request(update, sec_value, is_add_coin=True)
    else:
        user_data_state[user_id] = "waiting_for_sec_add"
        await update.message.reply_text('Please send the "sec" value to add now.')

async def handle_message_for_state(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles messages when an admin is prompted for a 'sec' value."""
    user_id = update.effective_user.id
    message_text = update.message.text

    if is_admin(user_id) and user_id in user_data_state:
        current_state = user_data_state.get(user_id)
        if current_state == "waiting_for_sec_check":
            await send_smileone_card_request(update, message_text, is_add_coin=False)
        elif current_state == "waiting_for_sec_add":
            await send_smileone_card_request(update, message_text, is_add_coin=True)

async def send_daily_admin_report(context: ContextTypes.DEFAULT_TYPE): 
    """Send Daily Report with Individual User Spending and Current User Balances to Admins only."""
    if not check_db_ready(): return
    
    try:
        report_date = datetime.now(ZoneInfo("Asia/Yangon")).strftime('%d.%m.%Y')
        balances = await get_smile_one_balances()
        users_count = await users_collection.count_documents({})
        pipeline = [
            {"$match": {"date": {"$regex": f".*{report_date}$"}}},
            {"$group": {
                "_id": None,
                "ph_spent": {"$sum": {"$cond": [{"$eq": ["$region", "ph"]}, {"$subtract": ["$total_cost", "$refunded_amount"]}, 0]}},
                "br_spent": {"$sum": {"$cond": [{"$eq": ["$region", "br"]}, {"$subtract": ["$total_cost", "$refunded_amount"]}, 0]}},
                "success": {"$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}},
                "partial": {"$sum": {"$cond": [{"$eq": ["$status", "partial_success"]}, 1, 0]}},
                "fail": {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                "total": {"$sum": 1}
            }}
        ]
        stats = await order_collection.aggregate(pipeline).to_list(length=None)
        summary = stats[0] if stats else {}

        report_msg = (
            f"📊 Daily Usage Report - {report_date} 📊\n\n"
            f"--- Smile One Balances (Current) ---\n"
            f"🇵🇭 MLBB PH: {balances.get('ph_mlbb')}\n"
            f"🇧🇷 MLBB BR: {balances.get('br_mlbb')}\n\n"
            f"--- Overall Summary for {report_date} ---\n"
            f"👥 Users Served: {users_count}\n"
            f"🇵🇭 Total Spent (PH): ${summary.get('ph_spent', 0):.2f} | "
            f"🇧🇷 Total Spent (BR): ${summary.get('br_spent', 0):.2f}\n\n"
            f"--- Order Summary ---\n"
            f"✅ {summary.get('success', 0)} / ⚠️ {summary.get('partial', 0)} "
            f"/ ❌ {summary.get('fail', 0)} / 📋 {summary.get('total', 0)} Total\n"
        )

        user_pipeline = [
            {"$match": {"date": {"$regex": f".*{report_date}$"}}},
            {"$group": {
                "_id": "$sender_user_id",
                "ph_spent": {"$sum": {"$cond": [{"$eq": ["$region", "ph"]},
                                                {"$subtract": ["$total_cost", "$refunded_amount"]}, 0]}},
                "br_spent": {"$sum": {"$cond": [{"$eq": ["$region", "br"]},
                                                {"$subtract": ["$total_cost", "$refunded_amount"]}, 0]}},
                "success": {"$sum": {"$cond": [{"$eq": ["$status", "success"]}, 1, 0]}},
                "partial": {"$sum": {"$cond": [{"$eq": ["$status", "partial_success"]}, 1, 0]}},
                "fail": {"$sum": {"$cond": [{"$eq": ["$status", "failed"]}, 1, 0]}},
                "total": {"$sum": 1}
            }}
        ]
        user_stats = await order_collection.aggregate(user_pipeline).to_list(length=None)

        if user_stats:
            report_msg += "\n--- Individual User Spending & Current Balances ---\n"
            for stat in user_stats:
                user = await users_collection.find_one({"user_id": stat["_id"]})
                username = f"@{user.get('username')}" if user and user.get("username") else stat["_id"]
                balance_ph = user.get("balance_ph", 0) if user else 0
                balance_br = user.get("balance_br", 0) if user else 0

                report_msg += (
                    f"👤 {username}\n"
                    f"   Spent → 🇵🇭 ${stat['ph_spent']:.2f} | 🇧🇷 ${stat['br_spent']:.2f}\n"
                    f"   Current → 🇵🇭 ${balance_ph:.2f} | 🇧🇷 ${balance_br:.2f}\n"
                    f"   Orders → ✅{stat['success']} / ⚠️{stat['partial']} / ❌{stat['fail']}\n"
                )

        for admin_id in admins:
            try:
                await context.bot.send_message(chat_id=admin_id, text=report_msg) 
            except Exception as e:
                logger.error(f"Failed to send daily report to admin {admin_id}: {e}")
                
    except (ServerSelectionTimeoutError, OperationFailure) as e:
         logger.error(f"DB Error during daily admin report generation: {e}")
         for admin_id in admins:
            try:
                await context.bot.send_message(chat_id=admin_id, text=f"🛑 Database connection failed during daily report generation. Error: {e}", parse_mode='HTML')
            except Exception:
                pass


async def check_orders(update: Update, order_collection):
    """
    Admin Only: Check orders for a given UID + ZoneID on the current day,
    including sender details.
    """
    if not check_db_ready(update): return
    
    # 🚨 Combined ID parsing from message text
    text_after_command = update.message.text.replace(".checkid", "").strip()
    
    # Updated regex to handle both: "<uid> <zoneid>" AND "<uid>(<zoneid>)"
    match = re.match(r"(\d+)\s*\((?:\s*|)(\d+)(?:\s*|)\)|\s*(\d+)\s+(\d+)", text_after_command)
    
    uid, zoneid = None, None
    if match:
        if match.group(1) and match.group(2):
            uid, zoneid = match.group(1), match.group(2)
        elif match.group(3) and match.group(4):
            uid, zoneid = match.group(3), match.group(4)
    
    if not uid or not zoneid:
         await update.message.reply_text(
            "Usage: `.checkid <uid> <zoneid>` or `.checkid <uid>(<zoneid>)`",
            parse_mode="Markdown"
        )
         return

    now = datetime.now(ZoneInfo("Asia/Yangon"))
    
    if now.time() < dt_time(6, 0, 0):
        target_date = (now - timedelta(days=1)).date()
    else:
        target_date = now.date()

    today_str = target_date.strftime("%d.%m.%Y")
    loading_message = await update.message.reply_text(
        f"🔍 Searching orders for Game ID <code>{html.escape(uid)}</code> ({html.escape(zoneid)}) on <b>{today_str}</b>...", 
        parse_mode='HTML'
    )

    try:
        orders_cursor = order_collection.find({
            "user_id": uid,
            "zone_id": zoneid,
            "date": {"$regex": f".*{re.escape(today_str)}$"}
        }).sort([("date", 1)])

        orders = await orders_cursor.to_list(length=None)

        if not orders:
            await safe_edit_message(
                loading_message,
                f"❌ No orders found today for UID: {uid} ({zoneid}) on <b>{today_str}</b>.", 
                parse_mode="HTML"
            )
            return

        response_summary_parts = [f"==== Orders for UID {uid} ({zoneid}) on {today_str} ====\n"]
        total_amount = 0.0
        
        for o in orders:
            sender_id = o.get('sender_user_id', 'N/A')
            sender_user_db = await users_collection.find_one({"user_id": str(sender_id)})
            sender_display_name = f"@{sender_user_db['username']}" if sender_user_db and sender_user_db.get('username') else f"ID: {sender_id}"
            
            actual_cost = float(o.get("total_cost", 0)) - float(o.get("refunded_amount", 0)) 
            total_amount += actual_cost

            response_summary_parts.append(
                f"---------------------------------\n"
                f"👤 **Sender:** <b>{html.escape(sender_display_name)}</b>\n"
                f"💎 **Pack:** {html.escape(o.get('product_name', 'N/A'))} | **Cost:** ${actual_cost:.2f} 🪙\n"
                f"🔄 **Status:** <b>{html.escape(o.get('status', 'N/A')).upper()}</b>\n"
                f"🆔 **Order ID:** <code>{html.escape(', '.join(map(str, o.get('order_ids', ['N/A']))))}</code>\n"
                f"⌚ **Time:** {html.escape(o.get('date', 'N/A').split(' ')[0])}"
            )
        
        response_summary_parts.append(f"\n<b>Total Actual Spent Today: {total_amount:.2f} 🪙</b>")
        
        await loading_message.delete()
        for msg in split_message("".join(response_summary_parts)):
            await update.message.reply_text(msg, parse_mode="HTML")

    except (ServerSelectionTimeoutError, OperationFailure) as e:
        logger.error(f"DB Error during check_orders: {e}")
        await safe_edit_message(loading_message, "Database connection failed while checking orders. Please try again later.", parse_mode='HTML')


async def checkid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin only - wrapper for check_orders"""
    if await check_bot_status(update): 
        return
    
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text(
            "❌ You are not authorized to use this command.", parse_mode="HTML"
        )
        return

    await check_orders(update, order_collection)
    
# --- DOT ONLY COMMAND ROUTER ---
async def dot_command_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text or not update.message.text.startswith('.'):
        return
    
    if is_admin(update.effective_user.id) and update.effective_user.id in user_data_state:
        return

    text = update.message.text.strip()
    parts = text.split()
    if not parts or parts[0] == '.': return
    command = parts[0][1:].lower()
    context.args = parts[1:]
    
    user_id_str = str(update.message.from_user.id)
    is_current_user_admin = is_admin(update.message.from_user.id)
    
    # 🚨 Admin commands
    if is_current_user_admin:
        if command == "admin":
            await admin_command(update, context)
            return
        elif command == "angel": 
            await register_user_by_admin_command(update, context)
            return
        elif command == "removeuser":
            await remove_user_by_admin_command(update, context)
            return
        elif command == "addbal":
            await add_balance_command(update, context)
            return
        elif command == "dedbal":
            await deduct_balance_command(update, context)
            return
        elif command == "user":
            await get_users_command(update, context)
            return
        elif command == "allhis":
            await get_all_orders(update, context)
            return
        elif command == "baladmin":
            await query_point_command(update, context)
            return
        elif command == "baldate":
            await user_spend_report_command(update, context)
            return
        elif command == "userspend":
            await user_spend_report_command(update, context)
            return
        elif command == "balorder":
            await balance_order_command(update, context)
            return
        elif command == "his": 
            # Admin: arguments 2 ခု (gameid, zoneid) သို့မဟုတ် 3 ခု (date, gameid, zoneid) ပါမပါစစ်
            # 2 arguments: <id> <zone> OR <date> <id>(<zone>)
            # 3 arguments: <date> <id> <zone>
            if len(context.args) == 2 or len(context.args) == 3 or (len(context.args) == 1 and ('(' in context.args[0] or context.args[0].lower() in ['today', 'week', 'month'])):
                # If it matches any format (including user-level time filters), pass to enhanced handler
                await enhanced_get_user_orders(update, context)
                return
        elif command == "checkcoin":
            await check_coin_command(update, context)
            return
        elif command == "addcoin":
            await add_coin_command(update, context)
            return
        elif command == "pausebot":
            await pause_bot_command(update, context)
            return
        elif command == "checkid":
            # checkid သည် argument 1 ခု (<id>(<zone>)) သို့မဟုတ် 2 ခု (<id> <zone>) ကို လက်ခံသည်
            if len(context.args) == 2 or (len(context.args) == 1 and '(' in context.args[0]):
                await checkid_command(update, context)
                return
        elif command == "unpausebot":
            await unpause_bot_command(update, context)
            return

    # Public commands (register မစစ်)
    if command == "start":
        await start_command(update, context)
        return
    elif command == "getid":
        await getid_command(update, context)
        return

    # Registered user commands
    if not await is_registered_user(user_id_str, update):
        return

    # 🚨 User commands
    if command == "help":
        await help_command(update, context)
    elif command == "bal":
        await balance_command(update, context)
    elif command == "his":
        # User အတွက် သို့မဟုတ် argument မပါသော Admin အတွက်
        await enhanced_get_user_orders(update, context)
    elif command == "use":
        await use_command(update, context)
    elif command == "pricebr":
        await pricebr_command(update, context)
    elif command == "priceph":
        await priceph_command(update, context)
    elif command == "mcpricebr":
        await mcpricebr_command(update, context)
    elif command == "mcpriceph":
        await mcpriceph_command(update, context)
    elif command == "bigopricebr":
        await bigopricebr_command(update, context)
    elif command == "mlp" and context.args:
        await bulk_command_ph(update, context)
    elif command == "ml" and context.args:
        await bulk_command_br(update, context)
    elif command == "mcggp" and context.args:
        await bulk_command_mc_ph(update, context)
    elif command == "mcggb" and context.args:
        await bulk_command_mc_br(update, context)
    elif command == "bigo" and context.args:
        await bulk_command_handler(update, context, 'bigo', region_override='br')  
    elif command == "topup":
        await add_coin_balance_command(update, context)
    elif command == "role":
        await bulk_command_router_role(update, context)
    elif command == "mcgg":
        await bulk_command_router_mcgg(update, context)
    elif command == "pubg":
        await bulk_command_router_pubg(update, context)
    elif command == "hok":
        await bulk_command_router_hok(update, context)
    elif command == "dtf":
        await bulk_command_router_dtf(update, context)
    else:
        pass


async def bulk_command_router_role(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args_text = update.message.text.split(" ", 1)[-1].strip()
    match = re.match(r"(\d+)\s*\(?(\d+)\)?", args_text)
    if not match:
        await update.message.reply_text("❌ Usage: `.role <player_id> <zone_id>`", parse_mode="Markdown"); return
    player_id, zone_id = match.groups()
    result = check_mlbb_user_info(player_id, zone_id)
    await update.message.reply_text(result, parse_mode="HTML")

async def bulk_command_router_mcgg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 2:
        await update.message.reply_text("❌ Usage: `.mcgg <player_id> <zone_id>`", parse_mode="Markdown"); return
    result = check_mcgg_user_info(args[0], args[1])
    await update.message.reply_text(result, parse_mode="HTML")

async def bulk_command_router_pubg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("❌ Usage: `.pubg <game_id>`", parse_mode="Markdown"); return
    result = check_pubg_user_info(args[0])
    await update.message.reply_text(result, parse_mode="HTML")

async def bulk_command_router_hok(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("❌ Usage: `.hok <game_id>`", parse_mode="Markdown"); return
    result = check_hok_user_info(args[0])
    await update.message.reply_text(result, parse_mode="HTML")

async def bulk_command_router_dtf(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if not args:
        await update.message.reply_text("❌ Usage: `.dtf <game_id>`", parse_mode="Markdown"); return
    result = check_dtf_info(args[0])
    await update.message.reply_text(result, parse_mode="HTML")


def main() -> None:
    """Start the bot."""
    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_for_state), group=0)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, dot_command_router), group=1)

    job_queue = application.job_queue
    
    job_queue.run_daily(
        send_daily_admin_report,
        time=dt_time(0, 0, 0, tzinfo=ZoneInfo("Asia/Yangon")),
        name="Daily Admin Report"
    )

    logger.info("Bot started and polling for updates...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
