import pytest
import asyncio
import sqlite3
from os import path
import common.db_sqlite as db_sqlite


async def native_populate_db(testdbpath):
    # Convenience helper that creates a testing database, populates it with example data, and returns the connection
    sampledata = [("example.com/statuses/1234567890", "This is a sample status.", None, 1700000000, "en", False, False, False),
                  ("example.com/statuses/1234567891", "This is a sample reply.", None, 1700000001, "en", False, True, False),
                  ("example.com/statuses/1234567892", "This is a sample media status.", None, 1700000002, "en", False, False, True),
                  ("example.com/statuses/1234567893", "This is a sample bot status.", None, 1700000003, "en", True, False, False),
                  ("example.com/statuses/1234567894", "This is an example status.", None, 1700000004, "en", False, False, False),
                  ("example.com/statuses/1234567895", "This is an example status with a keyword.", None, 1700000005, "en", False, False, False)]
    db_sqlite.initdb(testdbpath)
    await db_sqlite.batchwrite(sampledata, testdbpath)

    con = sqlite3.connect(testdbpath)
    return con



def test_initdb(tmpdir):
    testdbpath = tmpdir.join("test.sqlite")
    db_sqlite.initdb(testdbpath)

    assert path.isfile(testdbpath)

    con = sqlite3.connect(testdbpath)
    cur = con.cursor()
    res = cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    tables = [row[0] for row in res]
    assert "statuses" in tables
    assert "fts_status" in tables

    con.close()


@pytest.mark.asyncio
@pytest.mark.dependency(name="sqlite_write")
async def test_write_batch_statuses(tmpdir):
    testdbpath = tmpdir.join("test.sqlite")
    con = await native_populate_db(testdbpath)

    cur = con.cursor()
    res = cur.execute("SELECT url, text, subject, created, language, bot, reply, attachments FROM statuses;").fetchone()

    assert res[0] == "example.com/statuses/1234567890"
    assert res[1] == "This is a sample status."
    assert res[2] is None
    assert res[3] == 1700000000
    assert res[4] == "en"
    assert res[5] == 0
    assert res[6] == 0
    assert res[7] == 0

    con.close()


@pytest.mark.asyncio
@pytest.mark.dependency(depends=["sqlite_write"])
async def test_search(tmpdir):
    testdbpath = tmpdir.join("test.sqlite")
    con = await native_populate_db(testdbpath)

    # Normal search
    results = await db_sqlite.search(testdbpath, ["example"], [], [])
    assert len(results) == 2

    # AND search
    results = await db_sqlite.search(testdbpath, ["keyword", "example"], [], [])
    assert len(results) == 1

    # NOT search
    results = await db_sqlite.search(testdbpath, ["example"], [], ["keyword"])
    assert len(results) == 1