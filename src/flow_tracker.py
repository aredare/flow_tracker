#!/usr/bin/env python3
import os
import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import pandas_market_calendars as mcal
import ivolatility as ivol
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, DateTime, Float, Integer, select, and_, exists, func
)

# ─── CONFIG ─────────────────────────────────────────────────────────────────
# MySQL connection
MYSQL_URI = os.getenv("MYSQL_URI")
engine = create_engine(MYSQL_URI)
metadata = MetaData()

# --- Configure logging at the start of your main script ---
logging.basicConfig(
    level=logging.INFO,  # Set the minimum level of message to display
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# ivol login
ivol.setLoginParams(apiKey=os.getenv("API_KEY"))
ivol.setLoginParams(username=os.getenv("IVOL_USERNAME"), password=os.getenv("IVOL_PASSWORD"))
getOptionIV = ivol.setMethod("/equities/intraday/single-equity-option-rawiv")

# NYSE calendar for trading‐day counting
nyse = mcal.get_calendar("NYSE")

# your tables
option_iv_data = Table(
    "option_iv_data", metadata,
    Column("timestamp",            DateTime, nullable=False),
    Column("stockId",              String(50)),
    Column("stockSymbol",          String(20)),
    Column("optionExpirationDate", DateTime),
    Column("optionStrike",         Float),
    Column("optionType",           String(10)),
    Column("optionStyle",          String(20)),
    Column("optionSymbol",         String(50)),
    Column("optionBidPrice",       Float),
    Column("optionAskPrice",       Float),
    Column("optionBidDateTime",    DateTime),
    Column("optionAskDateTime",    DateTime),
    Column("optionBidSize",        Integer),
    Column("optionAskSize",        Integer),
    Column("optionBidExchange",    String(20)),
    Column("optionAskExchange",    String(20)),
    Column("optionVolume",         Integer),
    Column("optionIv",             Float),
    Column("underlyingPrice",      Float),
    Column("optionDelta",          Float),
    Column("optionGamma",          Float),
    Column("optionTheta",          Float),
    Column("optionVega",           Float),
    Column("optionRho",            Float),
    Column("optionPreIv",          Float),
    Column("optionImpliedYield",   Float),
    Column("calcTimestamp",        DateTime)
)
metadata.create_all(engine)  # idempotent

# JSON‐backed tracking store
# TODO: Probably just move this to a the database
TRACK_FILE = "tracked_options.json"
TRADE_FLOW_DATA = "test_data.csv"


def load_tracked():
    if not os.path.exists(TRACK_FILE):
        return []
    with open(TRACK_FILE) as f:
        data = json.load(f)
    # parse dates
    for ev in data:
        ev["event_ts"] = datetime.fromisoformat(ev["event_ts"])
        ev["expiration"] = datetime.fromisoformat(ev["expiration"]).date()
        ev["strike"] = float(ev["strike"])
    return data


def save_tracked(tracked):
    out = []
    for ev in tracked:
        out.append({
            "symbol":     ev["symbol"],
            "expiration": ev["expiration"].isoformat(),
            "strike":     ev["strike"],
            "optType":    ev["optType"],
            "event_ts":   ev["event_ts"].isoformat(),
        })
    with open(TRACK_FILE, "w") as f:
        json.dump(out, f, indent=2)


def trading_days_between(d0, d1):
    sched = nyse.schedule(start_date=d0, end_date=d1)
    return sched.index.normalize().unique()


def data_already_exists(tracked_instrument, trade_date, db_engine):
    """
    Checks if a specific option trade record already exists in the database.

    Args:
        tracked_instrument (dict): The instrument dictionary
        trade_date (datetime.datetime): The timestamp of the trade.
        db_engine: An active SQLAlchemy database engine.

    Returns:
        bool: True if the record exists, False otherwise.
    """
    # We compare the DATE part of the timestamp column in the DB
    # with the date part of the incoming trade_date.
    trade_date_only = trade_date

    # Build an EXISTS query. This is very efficient.
    # The database stops searching as soon as it finds one match.
    stmt = select(
        exists().where(
            and_(
                option_iv_data.c.stockSymbol == tracked_instrument['symbol'],
                option_iv_data.c.optionExpirationDate == tracked_instrument['expiration'],
                option_iv_data.c.optionStrike == tracked_instrument['strike'],
                option_iv_data.c.optionType == tracked_instrument['optType'],
                # Compare the date part of the timestamp column to our trade date
                func.date(option_iv_data.c.timestamp) == trade_date_only
            )
        )
    )

    with db_engine.connect() as conn:
        # Execute the query and get the boolean result
        result = conn.execute(stmt).scalar()
    return result


def transform_iv_response(df: pd.DataFrame, db_engine):
    """
    df: pandas Dataframe from iVolatility's single-equity-option-rawiv through their Python library
    engine:   SQLAlchemy engine for your MySQL database

    Persists the DataFrame and persists it to the `option_iv_data` table.
    """
    # 1) load JSON
    if df.empty:
        return df

    # 3) parse timestamps
    ts_cols = [
        "timestamp",
        "optionExpirationDate",
        "optionBidDateTime",
        "optionAskDateTime",
        "calcTimestamp"
    ]
    for c in ts_cols:
        if c in df:
            df[c] = pd.to_datetime(df[c])

    # 4) coerce numeric columns
    num_cols = [
        "stockId", "optionStrike", "optionBidPrice", "optionAskPrice",
        "optionBidSize", "optionAskSize", "optionVolume", "optionIv",
        "underlyingPrice", "optionDelta", "optionGamma", "optionTheta",
        "optionVega", "optionRho", "optionPreIv", "optionImpliedYield"
    ]
    for c in num_cols:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # 5) Persist to MySQL
    #    Assumes your `option_iv_data` table matches all DataFrame columns
    df.to_sql(
        name="option_iv_data",
        con=db_engine,
        if_exists="append",
        index=False,
        dtype={
            # optional: you can specify column types here if needed
        }
    )


def main():
    # 1) read the full trade‐flow CSV
    df = pd.read_csv(TRADE_FLOW_DATA).drop(['k = kiantrades', 'r = JonETrades'], axis=1)

    # combine and parse into a single datetime column
    df['trade_dt'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], format='%m/%d/%Y %I:%M:%S %p')
    df['time'] = pd.to_datetime(df['time'], format='%I:%M:%S %p').dt.strftime('%H:%M')
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values("trade_dt", inplace=True)

    # load tracked options state
    tracked = load_tracked()

    # loop each row in trade-flow
    for _, row in df.iterrows():
        current_date = row.trade_dt.date()
        logger.info(f"current_date {current_date}")
        # 3) prune by 10 trading‑day window
        new_tracked = []
        for ev in tracked:
            window = trading_days_between(ev["event_ts"].date(),
                                          ev["event_ts"].date() + timedelta(days=15))[:11]

            if pd.to_datetime(current_date) in window:
                new_tracked.append(ev)
            else:
                logger.info(f"checking tracked symbol is out of 10-day trading window"
                            f": {ev['symbol'], ev['expiration'], ev['optType'], ev['strike']}")

        tracked = new_tracked

        # 4) prune expired
        logger.info("checking tracked symbol aren't expired")
        tracked = [ev for ev in tracked if current_date <= ev["expiration"]]

        # 5) for each still‑tracked ev, fetch if missing
        for ev in tracked:
            missing = data_already_exists(ev, current_date, engine)
            if missing:
                logger.warning(f"tracked symbol is already in database: {ev['symbol'], ev['expiration'], ev['optType'], ev['strike']}")

            else:
                try:
                    # 6) fetch from iVol
                    logger.info(f"Fetching tracked symbol intraday data from ivol: {ev['symbol'], ev['expiration'], ev['optType'], ev['strike']}")
                    raw = getOptionIV(
                        symbol=ev["symbol"],
                        date=current_date.isoformat(),
                        expDate=ev["expiration"].isoformat(),
                        strike=str(ev["strike"]),
                        optType=ev["optType"],
                        minuteType="MINUTE_1"
                    )
                    # 6b) persist
                    logger.info(f"persisting symbol to database")
                    transform_iv_response(raw, engine)

                except Exception as e:
                    # Log that an error occurred, including which symbol failed.
                    # We also log the type of the exception and the exception message itself.
                    logger.error(
                        f"Failed to process symbol {row.symbol} due to an error. " f"Error Type: {type(e).__name__}. " f"Error Message: {e}",
                        exc_info=True)

        # 7) build this trade’s option symbol
        already_tracked = any(ev["symbol"] == row.symbol and
                              ev["expiration"] == pd.to_datetime(row.expiration).date() and
                              ev["strike"] == float(row.strike) and
                              ev["optType"] == row.c_p
                              for ev in tracked)
        if already_tracked:
            continue

        # 8) add new one to tracking
        tracked.append({
            "symbol":     row.symbol,
            "expiration": pd.to_datetime(row.expiration).date(),
            "strike":     float(row.strike),
            "optType":    row.c_p,
            "event_ts":   row.trade_dt
        })
        logger.info(f"Added symbol to tracked symbols {row.symbol, row.expiration, row.c_p, row.strike}")

        instrument = dict(row)
        instrument['optType'] = instrument['c_p']
        instrument['expiration'] = pd.to_datetime(instrument['expiration'])
        missing = data_already_exists(instrument, current_date, engine)

        if missing:
            logger.warning(
                f"tracked symbol is already in database: {instrument['symbol'], instrument['expiration'], instrument['optType'], instrument['strike']}")

        else:
            # 6) fetch from iVol
            logger.info(
                f"Fetching symbol intraday data from ivol: {instrument['symbol'], instrument['expiration'], instrument['optType'], instrument['strike']}")
            try:
                raw = getOptionIV(
                    symbol=row.symbol,
                    date=current_date.isoformat(),
                    expDate=pd.to_datetime(row.expiration).date().isoformat(),
                    strike=str(row.strike),
                    optType=row.c_p,
                    minuteType="MINUTE_1"
                )

                logger.info(f"persisting symbol to database")
                transform_iv_response(raw, engine)

            except Exception as e:
                # Log that an error occurred, including which symbol failed.
                # We also log the type of the exception and the exception message itself.
                logger.error(f"Failed to process symbol {row.symbol} due to an error. " f"Error Type: {type(e).__name__}. " f"Error Message: {e}", exc_info=True)

    # persist updated tracking
    logger.info("Saving updated tracked_options")
    save_tracked(tracked)


if __name__ == "__main__":
    main()
