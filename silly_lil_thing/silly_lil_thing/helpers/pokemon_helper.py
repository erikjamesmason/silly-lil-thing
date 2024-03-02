import asyncio
import aiohttp

async def fetch_pokemon(session, pokemon_name):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name}"
    async with session.get(url, ssl=False) as response:
        return await response.json()