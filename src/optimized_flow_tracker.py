#!/usr/bin/env python3
import os
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict

import pandas as pd
import pandas_market_calendars as mcal
import ivolatility as ivol
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    String, DateTime, Float, Integer, select, and_, exists, func
)

# ─── CONFIG ─────────────────────────────────────────────────────────────────
MYSQL_URI = os.getenv("MYSQL_URI")
engine = create_engine(MYSQL_URI)
metadata = MetaData()
start_date = datetime(2025, 5, 30)
skip_dates = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
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

# Database table definition
option_iv_data = Table(
    "option_iv_data", metadata,
    Column("timestamp", DateTime, nullable=False),
    Column("stockId", String(50)),
    Column("stockSymbol", String(20)),
    Column("optionExpirationDate", DateTime),
    Column("optionStrike", Float),
    Column("optionType", String(10)),
    Column("optionStyle", String(20)),
    Column("optionSymbol", String(50)),
    Column("optionBidPrice", Float),
    Column("optionAskPrice", Float),
    Column("optionBidDateTime", DateTime),
    Column("optionAskDateTime", DateTime),
    Column("optionBidSize", Integer),
    Column("optionAskSize", Integer),
    Column("optionBidExchange", String(20)),
    Column("optionAskExchange", String(20)),
    Column("optionVolume", Integer),
    Column("optionIv", Float),
    Column("underlyingPrice", Float),
    Column("optionDelta", Float),
    Column("optionGamma", Float),
    Column("optionTheta", Float),
    Column("optionVega", Float),
    Column("optionRho", Float),
    Column("optionPreIv", Float),
    Column("optionImpliedYield", Float),
    Column("calcTimestamp", DateTime)
)
metadata.create_all(engine)

# Tracking files
TRACK_FILE = "tracked_options.json"
TRADE_FLOW_DATA = "option_flow_data.csv"


class OptimizedTracker:
    def __init__(self):
        # Use sets for O(1) lookups instead of lists
        self.tracked_options = set()
        self.option_metadata = {}  # Store full option data

        # Cache for performance
        self.trading_days_cache = {}
        self.db_exists_cache = {}  # Cache database existence checks by date

    def load_tracked(self):
        """Load tracked options from JSON file"""
        if not os.path.exists(TRACK_FILE):
            return

        with open(TRACK_FILE) as f:
            data = json.load(f)

        for item in data:
            key = self._make_option_key(
                item["symbol"],
                datetime.fromisoformat(item["expiration"]).date(),
                float(item["strike"]),
                item["optType"]
            )
            self.tracked_options.add(key)
            self.option_metadata[key] = {
                "symbol": item["symbol"],
                "expiration": datetime.fromisoformat(item["expiration"]).date(),
                "strike": float(item["strike"]),
                "optType": item["optType"],
                "event_ts": datetime.fromisoformat(item["event_ts"])
            }

    def save_tracked(self):
        """Save tracked options to JSON file"""
        data = []
        for key in self.tracked_options:
            meta = self.option_metadata[key]
            data.append({
                "symbol": meta["symbol"],
                "expiration": meta["expiration"].isoformat(),
                "strike": meta["strike"],
                "optType": meta["optType"],
                "event_ts": meta["event_ts"].isoformat()
            })

        with open(TRACK_FILE, "w") as f:
            json.dump(data, f, indent=2)

    def _make_option_key(self, symbol, expiration, strike, opt_type):
        """Create a unique key for an option"""
        return f"{symbol}|{expiration}|{strike}|{opt_type}"

    def get_trading_days(self, start_date, end_date):
        """Get trading days with caching"""
        cache_key = (start_date, end_date)
        if cache_key not in self.trading_days_cache:
            sched = nyse.schedule(start_date=start_date, end_date=end_date)
            self.trading_days_cache[cache_key] = sched.index.normalize().unique()
        return self.trading_days_cache[cache_key]

    def bulk_check_db_exists(self, date, db_engine):
        """Check database existence for all tracked options on a specific date"""
        if date in self.db_exists_cache:
            return self.db_exists_cache[date]

        if not self.tracked_options:
            self.db_exists_cache[date] = set()
            return set()

        # Build conditions for all tracked options
        conditions = []
        for key in self.tracked_options:
            meta = self.option_metadata[key]
            condition = and_(
                option_iv_data.c.stockSymbol == meta['symbol'],
                option_iv_data.c.optionExpirationDate == meta['expiration'],
                option_iv_data.c.optionStrike == meta['strike'],
                option_iv_data.c.optionType == meta['optType']
            )
            conditions.append(condition)

        # Query for existing data on this date
        from sqlalchemy import or_
        stmt = select(
            option_iv_data.c.stockSymbol,
            option_iv_data.c.optionExpirationDate,
            option_iv_data.c.optionStrike,
            option_iv_data.c.optionType
        ).where(
            and_(
                func.date(option_iv_data.c.timestamp) == date,
                or_(*conditions)
            )
        )

        existing_options = set()
        with db_engine.connect() as conn:
            result = conn.execute(stmt)
            for row in result:
                key = self._make_option_key(
                    row.stockSymbol,
                    row.optionExpirationDate.date(),
                    row.optionStrike,
                    row.optionType
                )
                existing_options.add(key)

        self.db_exists_cache[date] = existing_options
        logger.debug(f"Found {len(existing_options)} existing options in DB for {date}")
        return existing_options

    def prune_tracked_options(self, current_date):
        """Remove expired and out-of-window options"""
        to_remove = set()

        for key in self.tracked_options:
            meta = self.option_metadata[key]

            # Check if expired
            if current_date > meta["expiration"]:
                to_remove.add(key)
                continue

            # Check if out of 10-day trading window
            window_end = meta["event_ts"].date() + timedelta(days=15)
            trading_days = self.get_trading_days(meta["event_ts"].date(), window_end)[:11]

            if pd.to_datetime(current_date) not in trading_days:
                to_remove.add(key)
                logger.info(
                    f"Removing out-of-window option: {meta['symbol']} {meta['expiration']} {meta['optType']} {meta['strike']}")

        # Remove expired/out-of-window options
        for key in to_remove:
            self.tracked_options.remove(key)
            del self.option_metadata[key]

    def add_option(self, symbol, expiration, strike, opt_type, event_ts):
        """Add new option to tracking"""
        key = self._make_option_key(symbol, expiration, strike, opt_type)

        if key not in self.tracked_options:
            self.tracked_options.add(key)
            self.option_metadata[key] = {
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "optType": opt_type,
                "event_ts": event_ts
            }
            logger.info(f"Added new option to tracking: {symbol} {expiration} {opt_type} {strike}")
            return True
        return False

    def get_options_to_fetch(self, date):
        """Get list of options that need data fetching for a specific date"""
        existing_in_db = self.bulk_check_db_exists(date, engine)

        options_to_fetch = []
        skipped_count = 0

        for key in self.tracked_options:
            meta = self.option_metadata[key]

            # Skip SPXW symbols
            if meta['symbol'] == 'SPXW':
                continue

            # Only fetch if NOT already in database
            if key not in existing_in_db:
                options_to_fetch.append(meta)
            else:
                skipped_count += 1

        logger.info(f"Date {date}: {len(options_to_fetch)} options to fetch, {skipped_count} already in DB")
        return options_to_fetch

    def check_single_option_exists(self, symbol, expiration, strike, opt_type, date):
        """Check if a single option already exists in database for a specific date"""
        key = self._make_option_key(symbol, expiration, strike, opt_type)

        # Check cache first
        # if date in self.db_exists_cache:
        #     return key in self.db_exists_cache[date]

        # If not in cache, do direct database check
        stmt = select(
            exists().where(
                and_(
                    option_iv_data.c.stockSymbol == symbol,
                    option_iv_data.c.optionExpirationDate == expiration,
                    option_iv_data.c.optionStrike == strike,
                    option_iv_data.c.optionType == opt_type,
                    func.date(option_iv_data.c.timestamp) == date
                )
            )
        )

        with engine.connect() as conn:
            result = conn.execute(stmt).scalar()

        # Update cache
        if date not in self.db_exists_cache:
            self.db_exists_cache[date] = set()
        if result:
            self.db_exists_cache[date].add(key)

        return result


def transform_iv_response(df: pd.DataFrame, db_engine):
    """Transform and persist iVolatility response data"""
    if df.empty:
        return df

    # Parse timestamps
    ts_cols = [
        "timestamp", "optionExpirationDate", "optionBidDateTime",
        "optionAskDateTime", "calcTimestamp"
    ]
    for c in ts_cols:
        if c in df:
            df[c] = pd.to_datetime(df[c])

    # Coerce numeric columns
    num_cols = [
        "stockId", "optionStrike", "optionBidPrice", "optionAskPrice",
        "optionBidSize", "optionAskSize", "optionVolume", "optionIv",
        "underlyingPrice", "optionDelta", "optionGamma", "optionTheta",
        "optionVega", "optionRho", "optionPreIv", "optionImpliedYield"
    ]
    for c in num_cols:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    logger.info(
        f"persisting new option")

    # Persist to MySQL
    df.to_sql(
        name="option_iv_data",
        con=db_engine,
        if_exists="append",
        index=False
    )


def main():
    tracker = OptimizedTracker()
    tracker.load_tracked()

    # Load and prepare trade flow data
    df = pd.read_csv(TRADE_FLOW_DATA).drop(['k = kiantrades', 'r = JonETrades'], axis=1)
    df['trade_dt'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], format='%m/%d/%Y %I:%M:%S %p')
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values("trade_dt", inplace=True)

    # Group by date for batch processing
    date_groups = df.groupby(df['trade_dt'].dt.date)

    for current_date, day_trades in date_groups:
        if skip_dates and current_date < start_date.date():
            continue

        logger.info(f"Processing date: {current_date}")

        # Prune tracked options once per date
        tracker.prune_tracked_options(current_date)

        # Get options that need fetching for this date
        options_to_fetch = tracker.get_options_to_fetch(current_date)

        # Fetch data for tracked options
        for option_meta in options_to_fetch:
            try:
                logger.info(
                    f"Fetching tracked option: {option_meta['symbol']} {option_meta['expiration']} {option_meta['optType']} {option_meta['strike']}")
                raw = getOptionIV(
                    symbol=option_meta["symbol"],
                    date=current_date.isoformat(),
                    expDate=option_meta["expiration"].isoformat(),
                    strike=str(option_meta["strike"]),
                    optType=option_meta["optType"],
                    minuteType="MINUTE_1"
                )
                transform_iv_response(raw, engine)

            except Exception as e:
                logger.error(f"Failed to fetch {option_meta['symbol']}: {e}", exc_info=True)

        # Process new trades for this date
        new_options_added = set()
        for _, row in day_trades.iterrows():
            expiration_date = pd.to_datetime(row.expiration).date()

            # Add new option to tracking if not already tracked
            option_key = tracker._make_option_key(row.symbol, expiration_date, float(row.strike), row.c_p)

            if tracker.add_option(row.symbol, expiration_date, float(row.strike), row.c_p, row.trade_dt):
                new_options_added.add(option_key)

        # Fetch data for newly added options (check each one individually)
        for option_key in new_options_added:
            if option_key in tracker.option_metadata:
                option_meta = tracker.option_metadata[option_key]
                if option_meta['symbol'] == 'SPXW':
                    continue

                # Check if this specific new option already exists in DB
                if tracker.check_single_option_exists(
                        option_meta['symbol'],
                        option_meta['expiration'],
                        option_meta['strike'],
                        option_meta['optType'],
                        current_date
                ):
                    logger.info(
                        f"New option already exists in DB: {option_meta['symbol']} {option_meta['expiration']} {option_meta['optType']} {option_meta['strike']}")
                    continue

                try:
                    logger.info(
                        f"Fetching new option: {option_meta['symbol']} {option_meta['expiration']} {option_meta['optType']} {option_meta['strike']}")
                    raw = getOptionIV(
                        symbol=option_meta["symbol"],
                        date=current_date.isoformat(),
                        expDate=option_meta["expiration"].isoformat(),
                        strike=str(option_meta["strike"]),
                        optType=option_meta["optType"],
                        minuteType="MINUTE_1"
                    )
                    transform_iv_response(raw, engine)

                except Exception as e:
                    logger.error(f"Failed to fetch new option {option_meta['symbol']}: {e}", exc_info=True)

        # Clear cache for this date to free memory
        if current_date in tracker.db_exists_cache:
            del tracker.db_exists_cache[current_date]

    # Save updated tracking state
    tracker.save_tracked()
    logger.info("Processing complete")


if __name__ == "__main__":
    main()