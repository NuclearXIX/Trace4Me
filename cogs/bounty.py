from discord.ext import commands

class Bounty(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

# Required setup function
async def setup(bot):
    await bot.add_cog(Bounty(bot))
