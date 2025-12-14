import sqlite3
import logging
import sys
from pathlib import Path
from typing import List, Tuple, Set

class DatabaseRepository:
    def __init__(self, db_file: Path, timeframes: List[str], logger: logging.Logger):
        self.db_file = db_file
        self.timeframes = timeframes
        self.logger = logger
        self.conn = self._setup_database()

    def _setup_database(self) -> sqlite3.Connection:
        """データベース接続をセットアップし、タイムフレームごとにテーブルを作成する"""
        try:
            self.db_file.parent.mkdir(exist_ok=True)
            conn = sqlite3.connect(self.db_file, timeout=10)
            cursor = conn.cursor()
            self.logger.info(f"データベースに接続: {self.db_file}")

            for tf in self.timeframes:
                tf_clean = tf.strip()
                if not tf_clean: continue
                table_name = self.get_table_name(tf_clean)
                self.logger.info(f"テーブル '{table_name}' の準備中...")
                cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    symbol TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    PRIMARY KEY (symbol, timestamp)
                )
                """)
            conn.commit()
            self.logger.info("全テーブルの準備完了。")
            return conn
        except sqlite3.Error as e:
            self.logger.error(f"データベースのセットアップに失敗: {e}")
            sys.exit(1)

    def get_table_name(self, timeframe: str) -> str:
        return f"ohlcv_{timeframe}"

    def upsert_ohlcv_data(self, timeframe: str, records: List[Tuple]):
        if not records:
            return

        table_name = self.get_table_name(timeframe)
        self.logger.info(f"[{timeframe}] {len(records)} 件のレコードをテーブル '{table_name}' にUPSERTします...")
        cursor = self.conn.cursor()
        try:
            upsert_sql = f"""
            INSERT INTO {table_name} (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, timestamp) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume
            """
            cursor.executemany(upsert_sql, records)
            self.conn.commit()
            self.logger.info(f"[{timeframe}] UPSERTが完了しました。")
        except sqlite3.Error as e:
            self.logger.error(f"[{timeframe}] DB保存中にエラー: {e}")
            self.conn.rollback()

    def cleanup_old_ohlcv_data(self, timeframe: str, symbols: Set[str], history_limit: int):
        if not symbols:
            return

        table_name = self.get_table_name(timeframe)
        self.logger.info(f"[{timeframe}] テーブル '{table_name}' の古いデータをクリーンアップします...")
        cursor = self.conn.cursor()
        try:
            delete_sql = f"""
            DELETE FROM {table_name} WHERE rowid IN (
                SELECT rowid FROM {table_name}
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT -1 OFFSET {history_limit}
            )
            """
            for symbol in symbols:
                cursor.execute(delete_sql, (symbol,))
            self.conn.commit()
            self.logger.info(f"[{timeframe}] クリーンアップが完了しました。")
        except sqlite3.Error as e:
            self.logger.error(f"[{timeframe}] DBクリーンアップ中にエラー: {e}")
            self.conn.rollback()

    def close(self):
        if self.conn:
            self.conn.close()
            self.logger.info("データベース接続をクローズしました。")
