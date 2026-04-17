import asyncio
import disnake
from disnake.ext import tasks, commands
from disnake.ext.commands import is_owner, NotOwner
import db_helpers
from helpers import *
from monitor import UniversalMonitor
import os
import os.path
from conf import *
import traceback


intents = disnake.Intents.default()
intents.members = True
intents.guilds = True
intents.message_content = True
bot = commands.InteractionBot(intents=intents, test_guilds=approved_guilds)
monitor = UniversalMonitor(bot, bot_name, webhook_url)


async def get_roblox_id(user):
    try:
        async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://users.roblox.com/v1/usernames/users",
                    json={"usernames": [user], "excludeBannedUsers": False},
                    timeout=15
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            users = data.get("data", [])
                            if users:
                                return users[0]["id"]  # returns first match
                            else:
                                print(f"No user found for: {user}")
                                return None
                        else:
                            print(f"Failed: {response.status}\nget_roblox_id")
                            return None

    except Exception as e:
        await monitor.report_warn(f"Error fetching Discord ID for {user}: {e}", "get_roblox_id")
        return None

async def get_member_id(user, search_guild_id):
    '''Takes a roblox username and finds the discord id via bloxlink'''

    roblox_id = await get_roblox_id(user)
    if not roblox_id:
        return None

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
            f'https://api.blox.link/v4/public/guilds/{search_guild_id}/roblox-to-discord/{roblox_id}',
            headers={"Authorization": bloxlink_api},
            timeout=10
                ) as response:
                if response.status != 200:
                    await monitor.report_warn(f"Bloxlink returned {response.status} for {user}", "get_member_id")
                    return None

                discord_data = await response.json()

        try:
            return discord_data["discordIDs"][0]
        except KeyError:
            await monitor.report_warn(
                f"Key Error on {user}. Roblox ID {roblox_id}\nDiscord data: {discord_data}",
                "get_member_id"
            )
            return None

    except Exception as e:
        await monitor.report_warn(f"Error fetching Discord ID for {user}: {e}", "get_member_id")
        return None


def instructor_tracking(inter: disnake.Interaction):
    with open("instructor_tracking.json", "r") as f:
        instruct = json.load(f)

    try:
        instructor = instruct[inter.user.display_name]
        instructor += 1

    except KeyError:
        data = {inter.user.display_name: 1}

        with open("instructor_tracking.json", "w") as f:
            json.dump(data, f, indent=2)

async def get_cadet(sheet_id, guild, user, sheet ):
    try:
        return await guild.fetch_member(int(sheet_id))

    except (disnake.NotFound, disnake.HTTPException, ValueError):
        temp_id = await get_member_id(user, guild.id)
        if temp_id:
            try:
                return await guild.fetch_member(temp_id)
            except (disnake.NotFound, disnake.HTTPException) as e:
                await monitor.report_warn(f"Cannot fetch member {user}: {e}",
                                          f"get_cadet_{sheet}")
                return None
        else:
            await monitor.report_warn(
                f"No Discord ID found for {user}",
                f"get_cadet_{sheet}"
            )
            return None

class PostReviewView(disnake.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @disnake.ui.button(label=":white_check_mark: Approve", style=disnake.ButtonStyle.success, custom_id="post_approve")
    async def approve(self, button: disnake.ui.Button, interaction: disnake.Interaction):

        review_data = await db_helpers.get_pending_review(bot.pool, interaction.message.id)
        if not review_data:
            await interaction.response.send_message(
                "Review data not found.", ephemeral=True)
            return

        user_id = review_data[0]["userid"]

        increment_instructor_count(
            interaction.user.id,
            interaction.user.display_name,
            "post"
        )

        for item in self.children:
            item.disabled = True

        embed = interaction.message.embeds[0]
        embed.color = disnake.Color.green()
        embed.title = ":white_check_mark: Approved"
        embed.add_field(
            name="Decision",
            value=f"Approved by {interaction.user.mention}",
            inline=False
        )

        await interaction.message.edit(embed=embed, view=self)

        member = await interaction.guild.fetch_member(user_id)
        role = interaction.guild.get_role(post_p1_id)
        await member.add_roles(role)

        exam_results = interaction.guild.get_channel(exam_results_channel)
        await interaction.response.send_message("Processing...", ephemeral=True)
        await exam_results.send(f"Congratulations {member.mention} you have passed POST P1!")
        await interaction.edit_original_message(content="Complete", delete_after=5)

        await db_helpers.delete_pending_review(bot.pool, interaction.message.id)

    @disnake.ui.button(label=":x: Deny", style=disnake.ButtonStyle.danger, custom_id="post_deny")
    async def deny(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        review_data = await db_helpers.get_pending_review(bot.pool, interaction.message.id)
        if not review_data:
            await interaction.response.send_message(
                "Review data not found.", ephemeral=True)
            return

        user_id = review_data[0]["userid"]

        increment_instructor_count(
            interaction.user.id,
            interaction.user.display_name,
            "post"
        )

        for item in self.children:
            item.disabled = True

        embed = interaction.message.embeds[0]
        embed.color = disnake.Color.red()
        embed.title = ":x: Denied"
        embed.add_field(
            name="Decision",
            value=f"Denied by {interaction.user.mention}",
            inline=False
        )

        await interaction.message.edit(embed=embed, view=self)

        user = interaction.client.get_user(user_id)
        if user:
            await user.send(":x: You did not pass POST P1.")

        await interaction.response.send_message(":x: Exam Denied.", ephemeral=True, delete_after=10)

        await db_helpers.delete_pending_review(bot.pool, interaction.message.id)

class SceneReviewView(disnake.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @disnake.ui.button(label=":white_check_mark: Approve", style=disnake.ButtonStyle.success, custom_id="scene_approve")
    async def approve(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        review_data = await db_helpers.get_pending_review(bot.pool, interaction.message.id)
        if not review_data:
            await interaction.response.send_message("Review data not found.", ephemeral=True)
            return

        user_id = review_data[0]["userid"]

        increment_instructor_count(
            interaction.user.id,
            interaction.user.display_name,
            "scene"
        )

        for item in self.children:
            item.disabled = True

        embed = interaction.message.embeds[0]
        embed.color = disnake.Color.green()
        embed.title = ":white_check_mark: Approved"
        embed.add_field(
            name="Decision",
            value=f"Approved by {interaction.user.mention}",
            inline=False
        )

        await interaction.message.edit(embed=embed, view=self)

        member = await interaction.guild.fetch_member(user_id)
        if not member.get_role(car_bl) and not member.get_role(sc_bl):
            role = interaction.guild.get_role(scene_p1_id)
            await member.add_roles(role)

            exam_results = interaction.guild.get_channel(exam_results_channel)
            await interaction.response.send_message("Processing...", ephemeral=True)
            await exam_results.send(f"Congratulations {member.mention} you have passed Scene Command P1!")
            await interaction.edit_original_message(content="Complete", delete_after=5)

        await db_helpers.delete_pending_review(bot.pool, interaction.message.id)

    @disnake.ui.button(label=":x: Deny", style=disnake.ButtonStyle.danger, custom_id="scene_deny")
    async def deny(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        review_data = await db_helpers.get_pending_review(bot.pool, interaction.message.id)
        if not review_data:
            await interaction.response.send_message("Review data not found.", ephemeral=True)
            return

        user_id = review_data[0]["userid"]

        increment_instructor_count(
            interaction.user.id,
            interaction.user.display_name,
            "scene"
        )

        for item in self.children:
            item.disabled = True

        embed = interaction.message.embeds[0]
        embed.color = disnake.Color.red()
        embed.title = ":x: Denied"
        embed.add_field(
            name="Decision",
            value=f"Denied by {interaction.user.mention}",
            inline=False
        )

        await interaction.message.edit(embed=embed, view=self)

        user = interaction.client.get_user(user_id)
        if user:
            await user.send(":x: You did not pass Scene Command P1.")

        await interaction.response.send_message(":x: Exam Denied.", ephemeral=True, delete_after=10)
        await db_helpers.delete_pending_review(bot.pool, interaction.message.id)


class AviationReviewView(disnake.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @disnake.ui.button(label=":white_check_mark: Approve", style=disnake.ButtonStyle.success, custom_id="aviation_approve")
    async def approve(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        review_data = await db_helpers.get_pending_review(bot.pool, interaction.message.id)
        if not review_data:
            await interaction.response.send_message("Review data not found.", ephemeral=True)
            return

        user_id = review_data[0]["userid"]

        increment_instructor_count(
            interaction.user.id,
            interaction.user.display_name,
            "aviation"
        )

        for item in self.children:
            item.disabled = True

        embed = interaction.message.embeds[0]
        embed.color = disnake.Color.green()
        embed.title = ":white_check_mark: Approved"
        embed.add_field(
            name="Decision",
            value=f"Approved by {interaction.user.mention}",
            inline=False
        )

        await interaction.message.edit(embed=embed, view=self)

        member = await interaction.guild.fetch_member(user_id)
        if not member.get_role(car_bl) and not member.get_role(heli_bl) and not member.get_role(plane_bl):
            role = interaction.guild.get_role(aviation_id)
            await member.add_roles(role)

            exam_results = interaction.guild.get_channel(exam_results_channel)
            await interaction.response.send_message("Processing...", ephemeral=True)
            await exam_results.send(f"Congratulations {member.mention} you have passed Aviation P1!")
            await interaction.edit_original_message(content="Complete", delete_after=5)

        await db_helpers.delete_pending_review(bot.pool, interaction.message.id)

    @disnake.ui.button(label=":x: Deny", style=disnake.ButtonStyle.danger, custom_id="aviation_deny")
    async def deny(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        review_data = await db_helpers.get_pending_review(bot.pool, interaction.message.id)
        if not review_data:
            await interaction.response.send_message("Review data not found.", ephemeral=True)
            return

        user_id = review_data[0]["userid"]

        increment_instructor_count(
            interaction.user.id,
            interaction.user.display_name,
            "aviation"
        )

        for item in self.children:
            item.disabled = True

        embed = interaction.message.embeds[0]
        embed.color = disnake.Color.red()
        embed.title = ":x: Denied"
        embed.add_field(
            name="Decision",
            value=f"Denied by {interaction.user.mention}",
            inline=False
        )

        await interaction.message.edit(embed=embed, view=self)

        user = interaction.client.get_user(user_id)
        if user:
            await user.send(":x: You did not pass Aviation P1.")

        await interaction.response.send_message(":x: Exam Denied.", ephemeral=True, delete_after=10)
        await db_helpers.delete_pending_review(bot.pool, interaction.message.id)

class CertReportView(disnake.ui.View):
    def __init__(self, pages: list[str], title: str):
        super().__init__(timeout=120)
        self.pages = pages
        self.index = 0
        self.title = title

    def make_embed(self):
        embed = disnake.Embed(
            title=self.title,
            description=self.pages[self.index],
            color=disnake.Color.gold()
        )
        embed.set_footer(text=f"Page {self.index + 1} / {len(self.pages)}")
        return embed

    @disnake.ui.button(label="Prev", style=disnake.ButtonStyle.secondary)
    async def prev(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        if self.index > 0:
            self.index -= 1
            await interaction.response.edit_message(embed=self.make_embed(), view=self)
        else:
            await interaction.response.defer()

    @disnake.ui.button(label="Next", style=disnake.ButtonStyle.secondary)
    async def next(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        if self.index < len(self.pages) - 1:
            self.index += 1
            await interaction.response.edit_message(embed=self.make_embed(), view=self)
        else:
            await interaction.response.defer()

@bot.slash_command(name="instructor_report", description="[STAFF] View instructor approval statistics")
async def instructor_report(inter: disnake.ApplicationCommandInteraction):
    await inter.response.defer()

    if not is_role(rma_employee, inter.user):
        return await inter.edit_original_response(
            ":no_entry: You have insufficient permissions to use this command."
        )

    tracking = get_instructor_stats()

    if not tracking:
        await inter.edit_original_response("No instructor data recorded yet.")
        return

    sorted_instructors = sorted(
        tracking.items(),
        key=lambda x: x[1]["total"],
        reverse=True
    )

    embed = disnake.Embed(
        title="Instructor Exam Statistics",
        description="Total Exams Reviewed per instructor",
        color=disnake.Color.blue()
    )

    for instructor_id, data in sorted_instructors:
        instructor_name = data["name"]
        total = data["total"]
        post = data["post"]
        scene = data["scene"]
        aviation = data["aviation"]

        field_value = (
            f"**Total:** {total}\n"
            f"POST: {post} | Scene: {scene} | Aviation: {aviation}"
        )

        embed.add_field(
            name=f"{instructor_name}",
            value=field_value,
            inline=False
        )

    embed.set_footer(text=f"Total Instructors: {len(tracking)}")
    await inter.edit_original_response(embed=embed)


@bot.slash_command(name="my_reviews", description="[STAFF] View your approval statistics")
async def my_reviews(inter: disnake.ApplicationCommandInteraction):

    await inter.response.defer(ephemeral=True)

    if not is_role(rma_employee, inter.user):
        return await inter.edit_original_response(
            ":no_entry: You have insufficient permissions to use this command."
        )
    stats = get_instructor_stats(inter.user.id)
    if not stats:
        await inter.edit_original_response(
            "You haven't approved any exams yet."
        )
        return

    embed = disnake.Embed(
        title=f"Your Approval Statistics",
        description=f"Instructor: {inter.user.display_name}",
        color=disnake.Color.blue()
    )
    embed.add_field(name="Total Approvals", value=str(stats["total"]), inline=False)
    embed.add_field(name="POST P1", value=str(stats["post"]), inline=True)
    embed.add_field(name="Scene P1", value=str(stats["scene"]), inline=True)
    embed.add_field(name="Aviation P1", value=str(stats["aviation"]), inline=True)

    await inter.edit_original_response(embed=embed)


@bot.slash_command(name="reset_instructor_tracking", description="[Division Lead] Reset all instructor tracking data")
async def reset_tracking(inter: disnake.ApplicationCommandInteraction):

    await inter.response.defer(ephemeral=True)

    if not is_role(dl_role, inter.user):
        return await inter.edit_original_response(
            ":no_entry: You have insufficient permissions to use this command."
        )

    await inter.response.defer(ephemeral=True)

    view = ConfirmResetView()
    await inter.edit_original_response(
        ":warning: Are you sure you want to reset ALL instructor tracking data? This cannot be undone!",
        view=view
    )

@bot.slash_command(name="examcheck", description="Checks a Users exam results")
async def examcheck(inter: disnake.ApplicationCommandInteraction,
                    user : disnake.Member = commands.Param(description="User to view exam"),
                    exam = commands.Param(description="Exam to view results",
                                          choices=[
                                              "POST P1",
                                              "Scene Command P1",
                                              "Aviation P1"
                                          ])):

    await inter.response.defer(ephemeral=True)

    user_name = user.display_name
    if exam == "POST P1":
        data = read_sheet(post_sheet_name, post_sheet_id)
    elif exam == "Scene Command P1":
        data = read_sheet(scene_sheet_name, scene_sheet_id)
    elif exam == "Aviation P1":
        data = read_sheet(aviation_sheet_name, aviation_sheet_id)

    headers = data[13]

    for row in data[14::]:
        records = dict(zip(headers, row))
        if records['Username'] == user_name:

            user = records.get("What is your ROBLOX username?")
            email = records.get("Email Address")
            user_score = records.get("Score")
            sheet_disc_id = records.get("What is your Discord User ID?")

            embed = disnake.Embed(
                title = f"Exam stats for {user}"
            )
            embed.add_field(name="What is your Roblox username", value=user)
            embed.add_field(name="Email Address", value=email)
            embed.add_field(name="Score", value=user_score)
            embed.add_field(name="What is your Discord User ID", value=sheet_disc_id)
            embed.add_field(name="Exam Type", value=exam)

            await inter.edit_original_response(embed=embed)


class ConfirmResetView(disnake.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @disnake.ui.button(label=":white_check_mark: Confirm Reset", style=disnake.ButtonStyle.danger)
    async def confirm(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        reset_instructor_tracking()

        for item in self.children:
            item.disabled = True

        await interaction.response.edit_message(
            content=":white_check_mark: Instructor tracking data has been reset.",
            view=self
        )

    @disnake.ui.button(label=":x: Cancel", style=disnake.ButtonStyle.secondary)
    async def cancel(self, button: disnake.ui.Button, interaction: disnake.Interaction):
        for item in self.children:
            item.disabled = True

        await interaction.response.edit_message(
            content=":x: Reset cancelled.",
            view=self
        )

@bot.slash_command(name="certification_report", description="[STAFF] Runs a report to check certification statuses")
async def cert_report(inter: disnake.ApplicationCommandInteraction):

    await inter.response.defer()

    if not is_role(rma_employee, inter.user):
        return await inter.edit_original_response(
            ":no_entry:  You have insufficient permissions to use this command."
        )

    sheet_data = read_sheet("Certification List", car_db_sheet)
    headers = sheet_data[13]

    guild = bot.get_guild(guild_id)
    #guild = bot.get_guild(1470528441349701780)

    if not guild:
        await monitor.report_error(Exception("Guild not found"), context="cert_report")
        return

    reported_certs = list()

    display_names = {member.display_name for member in guild.members}

    for row in sheet_data[14::]:
        records = dict(zip(headers, row))

        member_name = records.get("Username")
        post_cert_db = records.get("POST")
        instructor = records.get("INSTRUCTOR")
        heli = records.get("HELI PILOT")
        scene = records.get("SCENE CMD")
        plane = records.get("PLANE PILOT")

        if not member_name:
            continue

        if member_name not in display_names:
            reported_certs.append(f"**{member_name}** - Not found in RMA Server.\n")

        try:
            for member in guild.members:

                if member.display_name == member_name:
                    certs = []

                    roles = member.roles
                    heli_check = cert_validator(heli, heli_cert, roles)
                    post_check = cert_validator(post_cert_db, post_cert, roles)
                    plane_check = cert_validator(plane, plane_cert, roles)
                    instructor_check = cert_validator(instructor, instructor_role, roles)
                    scene_check = cert_validator(scene, scene_role, roles)

                    if not heli_check[0]:
                        certs.append(f"Certification: Heli Pilot\n{heli_check[1]}")
                    if not post_check[0]:
                        certs.append(f"Certification: POST\n{post_check[1]}")
                    if not plane_check[0]:
                        certs.append(f"Certification: Plane Pilot\n{plane_check[1]}")
                    if not instructor_check[0]:
                        certs.append(f"Certification: Instructor\n{instructor_check[1]}")
                    if not scene_check[0]:
                        certs.append(f"Certification: Scene Command\n{scene_check[1]}")

                    if certs:
                        certs_review = ", ".join(certs)
                        reported_certs.append(f"**{member_name}** - {certs_review}\n")
                    #reported_certs[member_name] = certs

        except Exception as e:
            continue

    if reported_certs:

        page_size = 10

        pages = [
            "\n".join(reported_certs[i:i + page_size])
            for i in range(0, len(reported_certs), page_size)
        ]
        title = "RMA User Certification Report"

        view  = CertReportView(pages, title)

        await inter.edit_original_response(embed=view.make_embed(), view=view)

    else:
        await inter.edit_original_response("None Found, not good. something busted")

@tasks.loop(seconds=poll_time)
async def poll_sheet_post_p1():
    dm_logs = bot.get_channel(1483829387202658526)
    post_logging = bot.get_channel(1483284646606409885)

    guild = bot.get_guild(guild_id)

    try:
        data = await db_helpers.post_insert(bot.pool)
        if data:
            for db_hash in data:
                rows_by_hash = await db_helpers.search_for_hash(bot.pool, db_hash, "postp1exams")

                user = rows_by_hash[0]["robloxusername"]
                user_score = rows_by_hash[0]['score']
                long_form = rows_by_hash[0]['longform']
                disc_id = rows_by_hash[0]['discordid']
                stats_link = rows_by_hash[0]['statslink']
                final_score = int(user_score.split("/")[0].strip())

                if disc_id:
                    member = await get_cadet(disc_id, guild, user, "post_p1")
                else:
                    await monitor.report_warn(f"No sheet_disc_id for {user}", "poll_sheet_post_p1")
                    member = None

                if final_score >= post_p1_score:
                    if member:
                        try:
                            if member.get_role(car_bl) or member.get_role(post_bl):
                                try:
                                    await member.send(
                                        "Your exam cannot be graded until your POST Blacklist is removed. See support for more details.")
                                except disnake.Forbidden:
                                    await monitor.report_warn(f"Cannot DM blacklisted user {member.id}",
                                                              "poll_sheet_post_p1")

                        except Exception as e:
                            await monitor.report_error(e, context="POST BL Check")
                            continue

                    embed = disnake.Embed(
                        title=f"POST P1 Exam - {user}",
                        color=disnake.Color.blurple()
                    )

                    if member:
                        embed.add_field(name="User", value=member.mention, inline=True)
                    else:
                        embed.add_field(name="User", value=f":warning: No Discord ID found for {user} - Manual review required",
                                        inline=True)

                    embed.add_field(name="Score", value=user_score, inline=True)
                    #embed.add_field(name="Long Form Response", value=long_form, inline=True)
                    add_field_safe(embed, name="Long Form Response", value=long_form, inline=True)
                    embed.add_field(name="User Statistics", value=f"[View Stats Image]({stats_link})", inline=False)
                    embed.set_footer(text="RMA Manager - Developed by bat_nation0224")

                    message = await post_logging.send(embed=embed, view=PostReviewView())
                    await db_helpers.exampending_insert(bot.pool,
                                                        exam_type="post",
                                                        user_id=disc_id,
                                                        score=user_score,
                                                        longform=long_form,
                                                        stats=stats_link,
                                                        msg_id=message.id)

                elif final_score < 50:
                    if not member:
                        await monitor.report_warn(f"Cannot notify {user} of failed exam - no Discord member found",
                                                  "poll_sheet_post_p1")
                        await dm_logs.send(
                            f"User {user} did not pass POST P1.\nUser had a score of {user_score}\nUser was not notified, unable to notify user.")
                        continue
                    try:
                        await member.send(f"You did not pass POST P1. Your score was {user_score}")
                        await dm_logs.send(
                            f"User {member.mention} did not pass POST P1.\nUser had a score of {user_score}")
                    except disnake.Forbidden:
                        await dm_logs.send(
                            f"User {member.mention} did not pass POST P1 (DM failed).\nUser had a score of {user_score}")

    except Exception as e:
        await monitor.report_error(e, context=f"poll_sheet_post_p1")

@tasks.loop(seconds=poll_time)
async def poll_sheet_scene_p1():
    dm_logs = bot.get_channel(1483829387202658526)
    scene_logging = bot.get_channel(1483284709072044102)
    guild = bot.get_guild(guild_id)

    try:
        data = await db_helpers.scene_insert(bot.pool)
        if data:
            for db_hash in data:
                rows_by_hash = await db_helpers.search_for_hash(bot.pool, db_hash, "scenep1exams")

                user = rows_by_hash[0]["robloxusername"]
                user_score = rows_by_hash[0]['score']
                long_form = rows_by_hash[0]['longform']
                disc_id = rows_by_hash[0]['discordid']
                stats_link = rows_by_hash[0]['statslink']
                final_score = int(user_score.split("/")[0].strip())

                if disc_id:
                    member = await get_cadet(disc_id, guild, user, "scene")
                else:
                    await monitor.report_warn(f"No sheet_disc_id for {user}", "poll_sheet_scene_p1")
                    member = None

                if final_score >= 26:
                    if member:
                        try:
                            if member.get_role(car_bl) or member.get_role(sc_bl):
                                try:
                                    await member.send(
                                        "Your exam cannot be graded until your Scene Blacklist is removed. See support for details.")
                                except disnake.Forbidden:
                                    await monitor.report_warn(f"Cannot DM blacklisted user {member.id}",
                                                              "poll_sheet_scene_p1")
                                continue
                        except Exception as e:
                            await monitor.report_error(e, context="Scene BL Check")
                            continue

                    embed = disnake.Embed(
                        title=f"Scene P1 Exam - {user}",
                        color=disnake.Color.blurple()
                    )

                    if member:
                        embed.add_field(name="User", value=member.mention, inline=True)
                    else:
                        embed.add_field(name="User", value=f":warning: No Discord ID found for {user} - Manual review required",
                                        inline=True)

                    embed.add_field(name="Score", value=user_score, inline=True)
                    # embed.add_field(name="Long Form Response", value=long_form, inline=True)
                    add_field_safe(embed, name="Long Form Response", value=long_form, inline=True)
                    embed.add_field(name="User Statistics", value=f"[View Stats Image]({stats_link})", inline=False)
                    embed.set_footer(text="RMA Manager - Developed by bat_nation0224")

                    message = await scene_logging.send(embed=embed, view=SceneReviewView())
                    await db_helpers.exampending_insert(bot.pool,
                                                        exam_type="scene",
                                                        user_id=disc_id,
                                                        score=user_score,
                                                        longform=long_form,
                                                        stats=stats_link,
                                                        msg_id=message.id)

                elif final_score < 26:
                    if not member:
                        await monitor.report_warn(f"Cannot notify {user} of failed exam - no Discord member found",
                                                  "poll_sheet_scene_p1")
                        await dm_logs.send(
                            f"User {user} did not pass Scene Command P1.\nUser had a score of {user_score}\nUser was not notified, unable to notify user.")
                        continue

                    try:
                        await member.send(f"You did not pass Scene Command P1. Your score was {user_score}")
                        await dm_logs.send(
                            f"User {member.mention} did not pass Scene Command P1.\nUser had a score of {user_score}")
                    except disnake.Forbidden:
                        await dm_logs.send(
                            f"User {member.mention} did not pass Scene Command P1 (DM failed).\nUser had a score of {user_score}")

    except Exception as e:
        await monitor.report_error(e, context=f"poll_sheet_scene_p1")


@tasks.loop(seconds=poll_time)
async def poll_sheet_aviation():
    guild = bot.get_guild(guild_id)
    aviation_logging = bot.get_channel(1483284745034141736)
    dm_logs = bot.get_channel(1483829387202658526)


    try:
        data = await db_helpers.aviation_insert(bot.pool)
        if data:
            for db_hash in data:
                rows_by_hash = await db_helpers.search_for_hash(bot.pool, db_hash, "scenep1exams")

                user = rows_by_hash[0]["robloxusername"]
                user_score = rows_by_hash[0]['score']
                long_form = rows_by_hash[0]['longform']
                disc_id = rows_by_hash[0]['discordid']
                stats_link = rows_by_hash[0]['statslink']
                final_score = int(user_score.split("/")[0].strip())

                if disc_id:
                    member = await get_cadet(disc_id, guild, user, "scene")
                else:
                    await monitor.report_warn(f"No sheet_disc_id for {user}", "poll_sheet_scene_p1")
                    member = None

                if final_score >= 16:
                    if member:
                        try:
                            if member.get_role(car_bl) or member.get_role(heli_bl) or member.get_role(plane_bl):
                                try:
                                    await member.send(
                                        "Your exam cannot be graded until your Aviation blacklists are removed. See support for details.")
                                except disnake.Forbidden:
                                    await monitor.report_warn(f"Cannot DM blacklisted user {member.id}",
                                                              "poll_sheet_aviation")
                                continue

                        except Exception as e:
                            await monitor.report_error(e, context="Aviation BL Check")
                            continue

                    embed = disnake.Embed(
                        title=f"Aviation P1 Exam - {user}",
                        color=disnake.Color.blurple()
                    )

                    if member:
                        embed.add_field(name="User", value=member.mention, inline=True)
                    else:
                        embed.add_field(name="User", value=f":warning: No Discord ID found for {user} - Manual review required",
                                        inline=True)

                    embed.add_field(name="Score", value=user_score, inline=True)
                    # embed.add_field(name="Long Form Response", value=long_form, inline=True)
                    add_field_safe(embed, name="Long Form Response", value=long_form, inline=True)
                    embed.add_field(name="User Statistics", value=f"[View Stats Image]({stats_link})", inline=False)
                    embed.set_footer(text="RMA Manager - Developed by bat_nation0224")

                    view_user_id = disc_id if disc_id else 0
                    message = await aviation_logging.send(embed=embed, view=AviationReviewView())
                    await db_helpers.exampending_insert(bot.pool,
                                                        exam_type="aviation",
                                                        user_id=disc_id,
                                                        score=user_score,
                                                        longform=long_form,
                                                        stats=stats_link,
                                                        msg_id=message.id)

                elif final_score < 16:
                    if not member:
                        await monitor.report_warn(f"Cannot notify {user} of failed exam - no Discord member found",
                                                  "poll_sheet_aviation")
                        await dm_logs.send(
                            f"User {user} did not pass Aviation P1.\nUser had a score of {user_score}\nUser was not notified, unable to notify user.")
                        continue

                    try:
                        await member.send(f"You did not pass Aviation P1. Your score was {user_score}")
                        await dm_logs.send(
                            f"User {member.mention} did not pass Aviation P1.\nUser had a score of {user_score}")
                    except disnake.Forbidden:
                        await dm_logs.send(
                            f"User {member.mention} did not pass Aviation P1 (DM failed).\nUser had a score of {user_score}")

    except Exception as e:
        await monitor.report_error(e, context=f"poll_sheet_aviation")

@bot.slash_command(name="guild_check", description="Joined Guild details")
@is_owner()
async def guild_check(interaction: disnake.ApplicationCommandInteraction):
    joined_guilds = bot.guilds
    for guild in joined_guilds:
        await monitor.guild_report(guild)

@bot.slash_command(name="remove")
@is_owner()
async def remove(interaction: disnake.ApplicationCommandInteraction, guild_id):
    await interaction.response.defer(ephemeral=True)

    guild = bot.get_guild(int(guild_id))

    if guild is None:
        await interaction.followup.send("Error in guild id")

    guild_name = guild.name
    await guild.leave()
    await interaction.followup.send(f"Left {guild_name}", ephemeral=True)

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")

    bot.add_view(PostReviewView())
    bot.add_view(SceneReviewView())
    bot.add_view(AviationReviewView())

    if not poll_sheet_post_p1.is_running():
        poll_sheet_post_p1.start()
    if not poll_sheet_scene_p1.is_running():
        poll_sheet_scene_p1.start()
    if not poll_sheet_aviation.is_running():
        poll_sheet_aviation.start()

    if os.path.exists(flag_path):
        await monitor.report_restart()

    with open(flag_path, "w") as f:
        f.write("running")
    await monitor.report_online()
    bot.loop.create_task(monitor.heartbeat())

@bot.event
async def on_guild_join(guild: disnake.Guild):
    new_guild = guild.id
    guild_owner = guild.owner_id

    if new_guild not in approved_guilds:

        inviter = None

        try:
            async for entry in guild.audit_logs(action=disnake.AuditLogAction.bot_add, limit=5):
                if entry.target.id == bot.user.id:
                    inviter = entry.user
                    break
        except disnake.Forbidden:
            await monitor.leave_report(guild, "[on_guild_join] No audit log access")

        try:
            await guild.get_member(guild_owner).send(f"BOT NOT APPROVED FOR USE IN {guild.name}. BOT WILL BE LEAVING NOW!\n"
                                                     f"BOT INVITED BY {inviter}.\n"
                                                     f"FOR AUTHORIZATION CONTACT THE DEVELOPER.")
        except disnake.Forbidden:
            pass

        await monitor.leave_report(guild, inviter)
        await guild.leave()


@bot.event
async def on_button_click(inter: disnake.MessageInteraction):
    """Track button clicks"""
    monitor.track_request()
    await monitor.check_rate_limit()

@bot.event
async def on_message_command(inter: disnake.MessageCommandInteraction):
    """Track message commands"""
    monitor.command_count += 1
    monitor.track_request()
    await monitor.check_rate_limit()

@bot.event
async def on_error(event, *args, **kwargs):
    await monitor.report_error(Exception(traceback.format_exc()))

@bot.event
async def on_slash_command_error(inter: disnake.ApplicationCommandInteraction, error):
    if isinstance(error, NotOwner):
        ran_by = inter.user.display_name
        await inter.send(":x: You don't have permissions to use this command.")
        if inter.guild.name:
            await monitor.report_warn(f"User: {ran_by} tried to run this command in {inter.guild.name}",
            context=f"/{inter.application_command.name}")
        else:
            await monitor.report_warn(f"User: {ran_by} tried to run this command.",
            context=f"/{inter.application_command.name}")
    else:
        await monitor.report_error(error, context=f"/{inter.application_command.name}")


async def run():
    bot.pool = await db_helpers.create_pool()
    bot.run(BOT_TOKEN)

if __name__ == "__main__":
    asyncio.run(run())