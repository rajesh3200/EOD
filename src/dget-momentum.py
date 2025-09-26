import os
from pathlib import Path
from datetime import datetime
from pandas import read_csv
from defs.Config import Config
from argparse import ArgumentParser

def lookup(sym):
    fpath = DIR / "eod2_data" / "daily" / f"{sym}.csv"

    if not fpath.exists():
        exit(f"{sym}: File not found.")

    df = read_csv(fpath, index_col="Date", parse_dates=True)  
    
    df["AVG_TRD_QTY"] = (
        df["QTY_PER_TRADE"].rolling(config.DGET_AVG_DAYS).mean().round(2)
    )

    df["AVG_DLV_QTY"] = (
        df["DLV_QTY"].rolling(config.DGET_AVG_DAYS).mean().round(2)
    )

    df["AVG_VOL"] = df["Volume"].rolling(config.DGET_AVG_DAYS).mean().round(2)

    df["DQ"] = (df["DLV_QTY"] / df["AVG_DLV_QTY"]).round(2)
    df["TQ"] = (df["QTY_PER_TRADE"] / df["AVG_TRD_QTY"]).round(2)
    df["VOL"] = (df["Volume"] / df["AVG_VOL"]).round(2)

    df["IM"] = True
    df["IM"] = df["IM"].where(
        (df["QTY_PER_TRADE"] > df["AVG_TRD_QTY"])
        & (df["DLV_QTY"] > df["AVG_DLV_QTY"]),
        "",
    )

    df["IM"] = df["IM"].apply(lambda v: "$$" if v else "-")
    df = df[-config.DGET_DAYS :][["DQ", "TQ", "IM", "VOL","Close"]]
    return df


config = Config()
parser = ArgumentParser(prog="dget-momentum.py")
parser.add_argument("-p","--period",type=int,metavar="str",help="Investment period in days")

args = parser.parse_args()

DIR = Path(__file__).parent
filename = os.path.join("data",datetime.now().strftime('%d-%m-%Y.csv'))
fpath = DIR / f"{filename}"
dframe = read_csv(fpath)

# Filter the DataFrame to show only rows where the 'DQ' column is greater than 1.5
df_filtered = dframe[dframe['DQ'] > 1.5]

if args.period:
    investmentperiod = args.period
else:   
    investmentperiod = 6

print(f"SCRIPT      Count      7days IM $$      Range     Mean")
print("---------------------------------------------------------------")
# foreach row in the filtered DataFrame, print the index first, then all the rows of that index
for index, row in df_filtered.iterrows():
    df_sym = lookup(row['SCRIP'])
    df_last7 = df_sym.tail(7)
    #count the df_sym where the 'IM' column is '$$'
    count = df_sym[df_sym['IM'] == '$$'].shape[0]
    last7count = df_last7[df_last7['IM'] == '$$'].shape[0]
    
    #average Close price of last 7 days
    mean_close = df_last7['Close'].mean()

    #high Close price of last 7 days
    high_close = df_last7['Close'].max()
    #low Close price of last 7 days
    low_close = df_last7['Close'].min()
    #percentage difference between high and low close price
    perc_diff = ((high_close - low_close) / low_close * 100) if low_close != 0 else 0
    

    if count >= investmentperiod:
        print(row['SCRIP'].ljust(12),
            str(count).ljust(12),     
            str(last7count).ljust(12),       
            f"{perc_diff:.2f}%,     {mean_close:.2f}")
