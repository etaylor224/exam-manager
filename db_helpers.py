import hashlib
import asyncpg
import asyncio
from conf import db, db_user, db_pass, server, post_sheet_name, post_sheet_id, scene_sheet_name, scene_sheet_id, aviation_sheet_name, aviation_sheet_id
import helpers

'''
DATA FLOW

READ google sheet -> done
check for new exams -> done
new exam found -> insert into database -> done
take new exam and send message to discord ->
push that information up to pending exams -> 

if exam passes -> mark it, role user, find all entries for user ID delete from DB except the passing one
'''






mock_data = ("POST P1", "495812537296617473", "55/64", "This is a test and a bunch of mock data so it dont matter", "https://drive.google.com/open?id=1t_AHvOuYnWw-5Ix3vzim8GMZkb3Nk-tl",
             "1493837468242284554")

mock_db_data = [("4/15/2026 13:39:40",
                "54 / 64",
                "Issatovic",
                "I want to obtain the POST certification in order to get Scene Command, which could greatly help me in myy IRS RP and allow me to assist people. I could also consider going into PIT or FWS. Then I would like to gain more skills and authority because if I join PIT, people will understand that I am not bad at what I do.",
                "https://drive.google.com/open?id=1uF6tl4XOJM5oTnX15DGj7ElSpBQGAkVP",
                "477e3bf477659718067671c810440cde32bef4521eac74f1bbbe906352859a47",
                "1146082090971045909")]

def hash_compute(data):

    data_str = " | ".join(data)
    row_hash = hashlib.sha256(data_str.encode("utf-8")).hexdigest()
    return row_hash


async def create_pool():
    pool = await asyncpg.create_pool(
        user = db_user,
        password = db_pass,
        database = db,
        host=server,
        min_size=5,
        max_size=20
    )
    return pool

async def search_for_hash(pool: asyncpg.Pool, row_hash, table):
    con = await pool.acquire(timeout=30)
    query = f"""
    select * from {table}
    where row_hash = $1
    """
    try:
        record = await con.fetch(query, row_hash)

    except Exception as e:
        print(e)
    finally:
        await pool.release(con)

    return record

async def get_pending_review(pool, msg_id):
    con = await pool.acquire(timeout=30)
    query = f"""
    select * from examspending
    where msg_id = $1
    """
    try:
        record = await con.fetch(query, msg_id)
    except Exception as e:
        print(e)
    finally:
        await pool.release(con)

    return record

async def delete_pending_review(pool, msg_id):
    con = await pool.acquire(timeout=30)
    query = """
    delete from examspending
    where msg_id = $1
    """
    try:
        await con.execute(query, msg_id)
    except Exception as e:
        print(e)
    finally:
        await pool.reease(con)



async def exam_table_insert(pool: asyncpg.Pool, table, data, row_hash):
    con = await pool.acquire(timeout=30)
    query = f"""
    insert into {table}(
    discordid,
    timestamp,
    score,
    robloxusername,
    longform,
    statslink,
    row_hash)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    """
    try:
        await con.execute(query,
                          data[0],
                          data[1],
                          data[2],
                          data[3],
                          data[4],
                          data[5],
                          row_hash)
    except Exception as e:
        print(e)
    finally:
        await pool.release(con)



async def exampending_insert(pool: asyncpg.Pool,
                             exam_type,
                             user_id,
                             score,
                             longform,
                             stats,
                             msg_id):
    con = await pool.acquire(timeout=30)
    query = """
    insert into examspending(
           exam_type,
           userid,
           score,
           longform,
           stats,
           msg_id)
        VALUES ($1,$2,$3,$4,$5,$6)
    """

    try:
        await con.execute(query,
                          exam_type,
                          user_id,
                          score,
                          longform,
                          stats,
                          msg_id)

    except Exception as e:
        print(e)
    finally:
        await pool.release(con)

async def delete_pending_exam(pool : asyncpg.Pool, msg_id):
    con = await pool.acquire(timeout=30)
    query = """
    DELETE from examspending 
    WHERE msg_id = $1
    """
    try:
        await con.execute(query, msg_id)
    except Exception as e:
        print(e)
    finally:
        await pool.release(con)

async def get_pending_exam(pool: asyncpg.Pool, msg_id):
    con = await pool.acquire(timeout=30)
    query = """
    SELECT * FROM examspending
    WHERE msg_id = $1    
    """
    try:
       data = await con.fetch(query, msg_id)

    except Exception as e:
        print(e)
    finally:
        await pool.release(con)

    return data

async def diff_check(pool, row_hash, table):
    db_record = await search_for_hash(pool, row_hash, table)
    try:
        if db_record[0]['row_hash'] == row_hash:
            return True
        else:
            return False
    except IndexError:
        return False
    except Exception as e:
        print(e)
    return False

async def search_for_user(pool: asyncpg.Pool, discord_id, table):
    query = f"""
    SELECT 
    """
    pass

async def post_insert(pool):
    sheet = helpers.read_sheet(post_sheet_name, post_sheet_id)
    headers = sheet[0]
    hashes_to_send = list()
    num_rows_inserted = 0

    for row in sheet[1:]:
        records = dict(zip(headers, row))
        user = records.get("What is your ROBLOX username?")
        timestamp = records.get("Timestamp")
        user_score = records.get("Score")
        long_form = records.get('Why are you interested in obtaining a POST certification? (grammar required)')
        sheet_disc_id = records.get("What is your Discord User ID?")
        stats_link = records.get("Please upload an image of your game statistics")

        if not stats_link:
            stats_link = ""

        row_data = (sheet_disc_id, timestamp, user_score, user, long_form, stats_link)
        row_hash = hash_compute(row_data)

        diff = await diff_check(pool, row_hash, "postp1exams")
        if diff:
            continue
        else:
            hashes_to_send.append(row_hash)
            await exam_table_insert(pool=pool,table="postp1exams", data=row_data, row_hash=row_hash)
            num_rows_inserted += 1

    print(f"Inserted {num_rows_inserted} rows in POST Table")
    return hashes_to_send

async def scene_insert(pool):
    sheet = helpers.read_sheet(scene_sheet_name, scene_sheet_id)
    headers = sheet[0]
    hashes_to_send = list()
    num_rows_inserted = 0

    for row in sheet[1:]:
        records = dict(zip(headers, row))
        user = records.get("What is your ROBLOX username?")
        timestamp = records.get("Timestamp")
        user_score = records.get("Score")
        long_form = records.get(
            'Why are you interested in obtaining a Scene Command certification? (grammar required)')
        sheet_disc_id = records.get("What is your Discord User ID?")
        stats_link = records.get("Please upload an image of your game statistics")

        if not stats_link:
            stats_link = ""

        row_data = (sheet_disc_id, timestamp, user_score, user, long_form, stats_link)
        row_hash = hash_compute(row_data)

        diff = await diff_check(pool, row_hash, "scenep1exam")
        if diff:
            continue
        else:
            hashes_to_send.append(row_hash)
            await exam_table_insert(pool=pool,table="scenep1exam", data=row_data, row_hash=row_hash)
            num_rows_inserted += 1

    print(f"Inserted {num_rows_inserted} rows in Scene Table")
    return hashes_to_send

async def aviation_insert(pool):
    sheet = helpers.read_sheet(aviation_sheet_name, aviation_sheet_id)
    headers = sheet[0]
    hashes_to_send = list()
    num_rows_inserted = 0

    for row in sheet[1:]:
        records = dict(zip(headers, row))
        user = records.get("What is your ROBLOX username?")
        timestamp = records.get("Timestamp")
        user_score = records.get("Score")
        long_form = records.get(
            'Why are you interested in obtaining a Helicopter and/or Plane Pilot certification? (grammar required)')
        sheet_disc_id = records.get("What is your Discord User ID?")
        stats_link = records.get("Please upload an image of your game statistics")

        if not stats_link:
            stats_link = ""

        row_data = (sheet_disc_id, timestamp, user_score, user, long_form, stats_link)
        row_hash = hash_compute(row_data)

        diff = await diff_check(pool, row_hash, "aviationp1exam")
        if diff:
            continue
        else:
            hashes_to_send.append(row_hash)
            await exam_table_insert(pool=pool,table="aviationp1exam", data=row_data, row_hash=row_hash)
            num_rows_inserted += 1

    print(f"Inserted {num_rows_inserted} rows in Aviation Table")
    return hashes_to_send

async def run():

    import time
    start_time = time.time()

    pool = await create_pool()
    await post_insert(pool)
    await scene_insert(pool)
    await aviation_insert(pool)

    print("--- %s seconds ---" % (time.time() - start_time))

    #await exampending_insert(pool, mock_data)
    #await get_pending_exam(pool, mock_data[5])
asyncio.run(run())