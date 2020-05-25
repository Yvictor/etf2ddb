import typing
import requests
import pandas as pd
import dolphindb


def crawler() -> requests.models.Response:
    url = "https://mis.twse.com.tw/stock/data/all_etf.txt"
    res = requests.get(url)
    return res


def processor(res: requests.models.Response) -> pd.DataFrame:
    data = res.json()
    if res.status_code == 200:
        df = pd.DataFrame(
            [msgarr for l in data.get("a1", []) for msgarr in l.get("msgArray", [])]
        )
        df.columns = [
            "Code",
            "Name",
            "TotalOutstandingUnit",
            "NetChangesOutstandingUnit",
            "Price",
            "EstimatedNetAssetValue",
            "PercentageOfEstimatedPremiumDiscount",
            "NetAssetValuePerUnitOfThePreviousBusinessDay",
            "Date",
            "Time",
            "k",
        ]
        to_int_cols = ["TotalOutstandingUnit", "NetChangesOutstandingUnit", "k"]
        to_float_cols = [
            "Price",
            "EstimatedNetAssetValue",
            "PercentageOfEstimatedPremiumDiscount",
            "NetAssetValuePerUnitOfThePreviousBusinessDay",
        ]

        def to_int(s):
            return int(float(str(s).replace(",", "") if s else 0))

        def to_float(s):
            return float(str(s).replace(",", "") if s and s != "-" else 0)

        for col in to_int_cols:
            df[col] = df[col].map(to_int)

        for col in to_float_cols:
            df[col] = df[col].map(to_float)
        df["DateTime"] = pd.to_datetime(
            df["Date"] + df["Time"], format="%Y%m%d%H:%M:%S"
        )
        df = df.drop(columns=["Date", "Time"])
    else:
        df = pd.DataFrame()
    return df


def db_init(df: pd.DataFrame, db: dolphindb.session):
    if not df.empty:
        dtype2ddbtype = {"int64": "LONG", "float64": "DOUBLE", "object": "SYMBOL"}
        cols_type = [("Date", "DATE"), ("Time", "TIME")] + [
            (k, dtype2ddbtype[v.name])
            for k, v in df.dtypes.items()
            if v.name in dtype2ddbtype
        ]
        schema = (",\n").join([f"array({col[1]}, 0) as {col[0]}" for col in cols_type])

        dfs = "dfs://dailyETF"
        table_name = "dailyETF"
        EXIST_DB = f"existsDatabase('{dfs}')"
        if not db.run(EXIST_DB):
            INIT_DB = f"dbETF = database('{dfs}', VALUE, 2013.01.01..2013.12.31)"
            LOAD_DB = INIT_DB
        else:
            LOAD_DB = f"dbETF = database('{dfs}')"
        EXIST_TABLE = f"existsTable('{dfs}', `{table_name})"
        if not db.run(EXIST_TABLE):
            db.run(LOAD_DB)

            CREATE_TABLE = f"""
            {table_name} = keyedTable(`Date`Time`Code, 
                {schema}
            )
            dbETF.createPartitionedTable({table_name}, `{table_name}, `Date)
            """
            db.run(CREATE_TABLE)


def db_update(df: pd.DataFrame, db: dolphindb.session):
    table_name = "dailyETF"
    LOAD_TABLE = f"""
    {table_name} = loadTable("dfs://dailyETF", `{table_name})
    """
    cols = (", ").join(df.columns[:-1])
    db.run(LOAD_TABLE)
    if not df.empty:
        db.upload({"df": df})
        db.run(
            f"append!({table_name}, select date(DateTime) as Date, time(DateTime) as Time, {cols} from df)"
        )
